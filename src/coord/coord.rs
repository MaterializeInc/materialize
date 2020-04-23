// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordination of installed views, available timestamps, and compacted timestamps.
//!
//! The command coordinator maintains a view of the installed views, and for each tracks
//! the frontier of available times (`upper`) and the frontier of compacted times (`since`).
//! The upper frontier describes times that may not return immediately, as any timestamps in
//! advance of the frontier are still open. The since frontier constrains those times for
//! which the maintained view will be correct, as any timestamps in advance of the frontier
//! must accumulate to the same value as would an un-compacted trace.

use std::collections::{BTreeMap, HashMap};
use std::iter;
use std::os::unix::ffi::OsStringExt;
use std::path::Path;
use std::thread;
use std::time::Duration;

use failure::{bail, ResultExt};
use futures::executor::block_on;
use futures::future::{self, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{self, StreamExt, TryStreamExt};
use timely::progress::frontier::{Antichain, AntichainRef, MutableAntichain};
use timely::progress::ChangeBatch;

use catalog::names::{DatabaseSpecifier, FullName};
use catalog::{Catalog, CatalogItem, PlanContext, SinkConnectorState};
use dataflow::logging::materialized::MaterializedEvent;
use dataflow::{SequencedCommand, WorkerFeedback, WorkerFeedbackWithMeta};
use dataflow_types::logging::LoggingConfig;
use dataflow_types::{
    AvroOcfSinkConnector, DataflowDesc, IndexDesc, KafkaSinkConnector, PeekResponse, PeekWhen,
    SinkConnector, TailSinkConnector, Timestamp, Update,
};
use expr::transform::Optimizer;
use expr::{
    GlobalId, Id, IdHumanizer, NullaryFunc, PartitionId, RelationExpr, RowSetFinishing, ScalarExpr,
    SourceInstanceId,
};
use ore::collections::CollectionExt;
use ore::thread::JoinHandleExt;
use repr::{ColumnName, Datum, RelationDesc, RelationType, Row};
use sql::{ExplainOptions, MutationKind, ObjectType, Params, Plan, PreparedStatement, Session};
use sql_parser::ast::ExplainStage;

use crate::persistence::SqlSerializer;
use crate::timestamp::{TimestampConfig, TimestampMessage, Timestamper};
use crate::util::ClientTransmitter;
use crate::{sink_connector, Command, ExecuteResponse, Response, StartupMessage};

pub enum Message {
    Command(Command),
    Worker(WorkerFeedbackWithMeta),
    AdvanceSourceTimestamp {
        id: SourceInstanceId,
        partition_count: i32,
        pid: PartitionId,
        timestamp: u64,
        offset: i64,
    },
    StatementReady {
        conn_id: u32,
        session: Session,
        tx: ClientTransmitter<ExecuteResponse>,
        result: Result<sql::Statement, failure::Error>,
        params: Params,
    },
    SinkConnectorReady {
        session: Session,
        tx: ClientTransmitter<ExecuteResponse>,
        id: GlobalId,
        result: Result<SinkConnector, failure::Error>,
    },
    Shutdown,
}

pub struct Config<'a, C>
where
    C: comm::Connection,
{
    pub switchboard: comm::Switchboard<C>,
    pub num_timely_workers: usize,
    pub symbiosis_url: Option<&'a str>,
    pub logging: Option<&'a LoggingConfig>,
    pub data_directory: Option<&'a Path>,
    pub executor: &'a tokio::runtime::Handle,
    pub timestamp: TimestampConfig,
    pub logical_compaction_window: Option<Duration>,
}

/// Glues the external world to the Timely workers.
pub struct Coordinator<C>
where
    C: comm::Connection,
{
    switchboard: comm::Switchboard<C>,
    broadcast_tx: comm::broadcast::Sender<SequencedCommand>,
    num_timely_workers: usize,
    optimizer: Optimizer,
    catalog: Catalog,
    symbiosis: Option<symbiosis::Postgres>,
    /// Maps (global Id of view) -> (existing indexes)
    views: HashMap<GlobalId, ViewState>,
    /// Maps (global Id of arrangement) -> (frontier information)
    indexes: HashMap<GlobalId, IndexState>,
    since_updates: Vec<(GlobalId, Vec<Timestamp>)>,
    /// For each connection running a TAIL command, the name of the dataflow
    /// that is servicing the TAIL. A connection can only run one TAIL at a
    /// time.
    active_tails: HashMap<u32, GlobalId>,
    timestamp_config: TimestampConfig,
    /// Delta from leading edge of an arrangement from which we allow compaction.
    logical_compaction_window_ms: Option<Timestamp>,
    /// Instance count: number of times sources have been instantiated in views. This is used
    /// to associate each new instance of a source with a unique instance id (iid)
    local_input_time: Timestamp,
    log: bool,
    executor: tokio::runtime::Handle,
    feedback_rx: Option<comm::mpsc::Receiver<WorkerFeedbackWithMeta>>,
}

impl<C> Coordinator<C>
where
    C: comm::Connection,
{
    pub fn new(config: Config<C>) -> Result<Self, failure::Error> {
        config.executor.enter(|| {
            let mut broadcast_tx = config.switchboard.broadcast_tx(dataflow::BroadcastToken);

            let symbiosis = if let Some(symbiosis_url) = config.symbiosis_url {
                Some(block_on(symbiosis::Postgres::open_and_erase(
                    symbiosis_url,
                ))?)
            } else {
                None
            };

            let optimizer = Optimizer::default();
            let catalog = open_catalog(config.data_directory, config.logging, optimizer)?;
            let logical_compaction_window_ms = config.logical_compaction_window.map(|d| {
                let millis = d.as_millis();
                if millis > Timestamp::max_value() as u128 {
                    Timestamp::max_value()
                } else if millis < Timestamp::min_value() as u128 {
                    Timestamp::min_value()
                } else {
                    millis as Timestamp
                }
            });
            let logging = config.logging;
            let (tx, rx) = config.switchboard.mpsc_limited(config.num_timely_workers);
            broadcast(&mut broadcast_tx, SequencedCommand::EnableFeedback(tx));
            let mut coord = Self {
                switchboard: config.switchboard,
                broadcast_tx,
                num_timely_workers: config.num_timely_workers,
                optimizer: Default::default(),
                catalog,
                symbiosis,
                views: HashMap::new(),
                indexes: HashMap::new(),
                since_updates: Vec::new(),
                active_tails: HashMap::new(),
                local_input_time: 1,
                log: config.logging.is_some(),
                executor: config.executor.clone(),
                timestamp_config: config.timestamp,
                logical_compaction_window_ms,
                feedback_rx: Some(rx),
            };

            let catalog_entries: Vec<_> = coord
                .catalog
                .iter()
                .map(|entry| (entry.id(), entry.name().clone(), entry.item().clone()))
                .collect();
            for (id, name, item) in catalog_entries {
                match item {
                    //currently catalog item rebuild assumes that sinks and
                    //indexes are always built individually and does not store information
                    //about how it was built. If we start building multiple sinks and/or indexes
                    //using a single dataflow, we have to make sure the rebuild process re-runs
                    //the same multiple-build dataflow.
                    CatalogItem::Source(_) => {
                        coord.views.insert(id, ViewState::new(false, vec![]));
                    }
                    CatalogItem::View(view) => {
                        coord.insert_view(id, &view);
                    }
                    CatalogItem::Sink(sink) => {
                        let builder = match sink.connector {
                            SinkConnectorState::Pending(builder) => builder,
                            SinkConnectorState::Ready(_) => {
                                panic!("sink already initialized during catalog boot")
                            }
                        };
                        let connector = block_on(sink_connector::build(builder, id))
                            .with_context(|e| format!("recreating sink {}: {}", name, e))?;
                        coord.handle_sink_connector_ready(id, connector);
                    }
                    CatalogItem::Index(index) => match id {
                        GlobalId::User(_) => {
                            coord.create_index_dataflow(name.to_string(), id, index)
                        }
                        GlobalId::System(_) => {
                            // TODO(benesch): a smarter way to determine whether this system index
                            // is on a logging source or a logging view. Probably logging sources
                            // should not be catalog views.
                            if logging
                                .unwrap()
                                .active_views()
                                .iter()
                                .any(|v| v.index_id == id)
                            {
                                coord.create_index_dataflow(name.to_string(), id, index)
                            } else {
                                coord.insert_index(id, &index, Some(1_000))
                            }
                        }
                    },
                }
            }

            // Announce primary and foreign key relationships.
            if let Some(logging_config) = logging {
                for log in logging_config.active_logs().iter() {
                    coord.report_catalog_update(
                        log.id(),
                        coord
                            .catalog
                            .humanize_id(expr::Id::Global(log.id()))
                            .unwrap(),
                        true,
                    );
                    for (index, key) in log.schema().typ().keys.iter().enumerate() {
                        broadcast(
                            &mut coord.broadcast_tx,
                            SequencedCommand::AppendLog(MaterializedEvent::PrimaryKey(
                                log.id(),
                                key.clone(),
                                index,
                            )),
                        );
                    }
                    for (index, (parent, pairs)) in log.foreign_keys().into_iter().enumerate() {
                        broadcast(
                            &mut coord.broadcast_tx,
                            SequencedCommand::AppendLog(MaterializedEvent::ForeignKey(
                                log.id(),
                                parent,
                                pairs,
                                index,
                            )),
                        );
                    }
                }
            }
            Ok(coord)
        })
    }

    pub fn serve(&mut self, cmd_rx: futures::channel::mpsc::UnboundedReceiver<Command>) {
        self.executor.clone().enter(|| self.serve_core(cmd_rx))
    }

    fn serve_core(&mut self, cmd_rx: futures::channel::mpsc::UnboundedReceiver<Command>) {
        let (internal_cmd_tx, internal_cmd_stream) = futures::channel::mpsc::unbounded();

        let cmd_stream = cmd_rx
            .map(Message::Command)
            .chain(stream::once(future::ready(Message::Shutdown)));

        let feedback_stream = self.feedback_rx.take().unwrap().map(|r| match r {
            Ok(m) => Message::Worker(m),
            Err(e) => panic!("coordinator feedback receiver failed: {}", e),
        });

        let (ts_tx, ts_rx) = std::sync::mpsc::channel();
        let mut timestamper = Timestamper::new(
            &self.timestamp_config,
            self.catalog.storage_handle(),
            internal_cmd_tx.clone(),
            ts_rx,
        );
        let executor = self.executor.clone();
        let _timestamper_thread =
            thread::spawn(move || executor.enter(|| timestamper.update())).join_on_drop();

        let mut messages = ore::future::select_all_biased(vec![
            // Order matters here. We want to drain internal commands
            // (`internal_cmd_stream` and `feedback_stream`) before processing
            // external commands (`cmd_stream`).
            internal_cmd_stream.boxed(),
            feedback_stream.boxed(),
            cmd_stream.boxed(),
        ]);

        while let Some(msg) = block_on(messages.next()) {
            match msg {
                Message::Command(Command::Startup { session, tx }) => {
                    let mut messages = vec![];
                    if self.catalog.database_resolver(session.database()).is_err() {
                        messages.push(StartupMessage::UnknownSessionDatabase);
                    }
                    ClientTransmitter::new(tx).send(Ok(messages), session)
                }
                Message::Command(Command::Execute {
                    portal_name,
                    session,
                    conn_id,
                    tx,
                }) => {
                    let result = session
                        .get_portal(&portal_name)
                        .ok_or_else(|| failure::format_err!("portal does not exist {:?}", portal_name))
                        .and_then(|portal| {
                            session
                                .get_prepared_statement(&portal.statement_name)
                                .ok_or_else(|| failure::format_err!(
                                    "statement for portal does not exist portal={:?} statement={:?}",
                                    portal_name,
                                    portal.statement_name
                                ))
                                .map(|ps| (portal, ps))
                        });
                    let (portal, prepared) = match result {
                        Ok((portal, prepared)) => (portal, prepared),
                        Err(e) => {
                            let _ = tx.send(Response {
                                result: Err(e),
                                session,
                            });
                            return;
                        }
                    };
                    match prepared.sql() {
                        Some(stmt) => {
                            let mut internal_cmd_tx = internal_cmd_tx.clone();
                            let stmt = stmt.clone();
                            let params = portal.parameters.clone();
                            tokio::spawn(async move {
                                let result = sql::purify(stmt).await;
                                internal_cmd_tx
                                    .send(Message::StatementReady {
                                        session,
                                        tx: ClientTransmitter::new(tx),
                                        result,
                                        conn_id,
                                        params,
                                    })
                                    .await
                                    .expect("sending to internal_cmd_tx cannot fail");
                            });
                        }
                        None => {
                            let _ = tx.send(Response {
                                result: Ok(ExecuteResponse::EmptyQuery),
                                session,
                            });
                        }
                    }
                }

                Message::StatementReady {
                    conn_id,
                    session,
                    tx,
                    result,
                    params,
                } => match result.and_then(|stmt| self.handle_statement(&session, stmt, &params)) {
                    Ok((pcx, plan)) => {
                        self.sequence_plan(&internal_cmd_tx, tx, session, pcx, plan, conn_id)
                    }
                    Err(e) => tx.send(Err(e), session),
                },

                Message::SinkConnectorReady {
                    session,
                    tx,
                    id,
                    result,
                } => match result {
                    Ok(connector) => {
                        // NOTE: we must not fail from here on out. We have a
                        // connector, which means there is external state (like
                        // a Kafka topic) that's been created on our behalf. If
                        // we fail now, we'll leak that external state.
                        if self.catalog.try_get_by_id(id).is_some() {
                            self.handle_sink_connector_ready(id, connector);
                        } else {
                            // Another session dropped the sink while we were
                            // creating the connector. Report to the client that
                            // we created the sink, because from their
                            // perspective we did, as there is state (e.g. a
                            // Kafka topic) they need to clean up.
                        }
                        tx.send(Ok(ExecuteResponse::CreatedSink { existed: false }), session);
                    }
                    Err(e) => {
                        self.catalog
                            .transact(vec![catalog::Op::DropItem(id)])
                            .expect("corrupt catalog");
                        tx.send(Err(e), session);
                    }
                },

                Message::Command(Command::Parse {
                    name,
                    sql,
                    mut session,
                    tx,
                }) => {
                    let result = self.handle_parse(&mut session, name, sql);
                    let _ = tx.send(Response { result, session });
                }

                Message::Command(Command::CancelRequest { conn_id }) => {
                    self.sequence_cancel(conn_id);
                }

                Message::Command(Command::DumpCatalog { tx }) => {
                    let _ = tx.send(self.catalog.dump());
                }

                Message::Worker(WorkerFeedbackWithMeta {
                    worker_id: _,
                    message: WorkerFeedback::FrontierUppers(updates),
                }) => {
                    for (name, changes) in updates {
                        self.update_upper(&name, changes);
                    }
                    self.maintenance();
                }

                Message::Worker(WorkerFeedbackWithMeta {
                    worker_id: _,
                    message: WorkerFeedback::DroppedSource(source_id),
                }) => {
                    // Notify timestamping thread that source has been dropped
                    ts_tx
                        .send(TimestampMessage::DropInstance(source_id))
                        .expect("Failed to send Drop Instance notice to timestamper");
                }
                Message::Worker(WorkerFeedbackWithMeta {
                    worker_id: _,
                    message: WorkerFeedback::CreateSource(source_id, _sc),
                }) => {
                    if let Some(entry) = self.catalog.try_get_by_id(source_id.sid) {
                        if let CatalogItem::Source(s) = entry.item() {
                            ts_tx
                                .send(TimestampMessage::Add(source_id, s.connector.clone()))
                                .expect("Failed to send CREATE Instance notice to timestamper");
                        } else {
                            panic!("A non-source is re-using the same source ID");
                        }
                    } else {
                        // Someone already dropped the source
                    }
                }

                Message::AdvanceSourceTimestamp {
                    id,
                    partition_count,
                    pid,
                    timestamp,
                    offset,
                } => {
                    broadcast(
                        &mut self.broadcast_tx,
                        SequencedCommand::AdvanceSourceTimestamp {
                            id,
                            partition_count,
                            pid,
                            timestamp,
                            offset,
                        },
                    );
                }

                Message::Shutdown => {
                    ts_tx.send(TimestampMessage::Shutdown).unwrap();
                    self.shutdown();
                    break;
                }
            }
        }

        // Cleanly drain any pending messages from the worker before shutting
        // down.
        drop(internal_cmd_tx);
        while let Some(_) = block_on(messages.next()) {}
    }

    /// Instruct the dataflow layer to cancel any ongoing, interactive work for
    /// the named `conn_id`. This means canceling the active PEEK or TAIL, if
    /// one exists.
    ///
    /// NOTE(benesch): this function makes the assumption that a connection can
    /// only have one active query at a time. This is true today, but will not
    /// be true once we have full support for portals.
    pub fn sequence_cancel(&mut self, conn_id: u32) {
        if let Some(name) = self.active_tails.remove(&conn_id) {
            // A TAIL is known to be active, so drop the dataflow that is
            // servicing it. No need to try to cancel PEEKs in this case,
            // because if a TAIL is active, a PEEK cannot be.
            self.drop_sinks(vec![name]);
        } else {
            // No TAIL is known to be active, so drop the PEEK that may be
            // active on this connection. This is a no-op if no PEEKs are
            // active.
            broadcast(
                &mut self.broadcast_tx,
                SequencedCommand::CancelPeek { conn_id },
            );
        }
    }

    fn sequence_plan(
        &mut self,
        internal_cmd_tx: &futures::channel::mpsc::UnboundedSender<Message>,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        pcx: PlanContext,
        plan: Plan,
        conn_id: u32,
    ) {
        match plan {
            Plan::CreateDatabase {
                name,
                if_not_exists,
            } => tx.send(self.sequence_create_database(name, if_not_exists), session),

            Plan::CreateSchema {
                database_name,
                schema_name,
                if_not_exists,
            } => tx.send(
                self.sequence_create_schema(database_name, schema_name, if_not_exists),
                session,
            ),

            Plan::CreateTable {
                name,
                desc,
                if_not_exists,
            } => tx.send(
                self.sequence_create_table(pcx, name, desc, if_not_exists),
                session,
            ),

            Plan::CreateSource {
                name,
                source,
                if_not_exists,
                materialized,
            } => tx.send(
                self.sequence_create_source(pcx, name, source, if_not_exists, materialized),
                session,
            ),

            Plan::CreateSink {
                name,
                sink,
                if_not_exists,
            } => self.sequence_create_sink(
                pcx,
                internal_cmd_tx.clone(),
                tx,
                session,
                name,
                sink,
                if_not_exists,
            ),

            Plan::CreateView {
                name,
                view,
                replace,
                materialize,
                if_not_exists,
            } => tx.send(
                self.sequence_create_view(pcx, name, view, replace, materialize, if_not_exists),
                session,
            ),

            Plan::CreateIndex {
                name,
                index,
                if_not_exists,
            } => tx.send(
                self.sequence_create_index(pcx, name, index, if_not_exists),
                session,
            ),

            Plan::DropDatabase { name } => tx.send(self.sequence_drop_database(name), session),

            Plan::DropSchema {
                database_name,
                schema_name,
            } => tx.send(
                self.sequence_drop_schema(database_name, schema_name),
                session,
            ),

            Plan::DropItems { items, ty } => tx.send(self.sequence_drop_items(items, ty), session),

            Plan::EmptyQuery => tx.send(Ok(ExecuteResponse::EmptyQuery), session),

            Plan::ShowAllVariables => tx.send(self.sequence_show_all_variables(&session), session),

            Plan::ShowVariable(name) => {
                tx.send(self.sequence_show_variable(&session, name), session)
            }

            Plan::SetVariable { name, value } => tx.send(
                self.sequence_set_variable(&mut session, name, value),
                session,
            ),

            Plan::StartTransaction => {
                session.start_transaction();
                tx.send(Ok(ExecuteResponse::StartedTransaction), session)
            }

            Plan::CommitTransaction => {
                session.end_transaction();
                tx.send(Ok(ExecuteResponse::CommittedTransaction), session)
            }

            Plan::AbortTransaction => {
                session.end_transaction();
                tx.send(Ok(ExecuteResponse::AbortedTransaction), session)
            }

            Plan::Peek {
                source,
                when,
                finishing,
                materialize,
            } => tx.send(
                self.sequence_peek(conn_id, source, when, finishing, materialize),
                session,
            ),

            Plan::Tail(id) => tx.send(self.sequence_tail(conn_id, id), session),

            Plan::SendRows(rows) => tx.send(Ok(send_immediate_rows(rows)), session),

            Plan::ExplainPlan {
                sql,
                raw_plan,
                decorrelated_plan,
                row_set_finishing,
                stage,
                options,
            } => tx.send(
                self.sequence_explain_plan(
                    sql,
                    raw_plan,
                    decorrelated_plan,
                    row_set_finishing,
                    stage,
                    options,
                ),
                session,
            ),

            Plan::SendDiffs {
                id,
                updates,
                affected_rows,
                kind,
            } => tx.send(
                self.sequence_send_diffs(id, updates, affected_rows, kind),
                session,
            ),

            Plan::ShowViews {
                ids,
                full,
                show_queryable,
                limit_materialized,
            } => tx.send(
                self.sequence_show_views(ids, full, show_queryable, limit_materialized),
                session,
            ),
        }
    }

    fn sequence_create_database(
        &mut self,
        name: String,
        if_not_exists: bool,
    ) -> Result<ExecuteResponse, failure::Error> {
        let ops = vec![
            catalog::Op::CreateDatabase { name: name.clone() },
            catalog::Op::CreateSchema {
                database_name: DatabaseSpecifier::Name(name),
                schema_name: "public".into(),
            },
        ];
        match self.catalog_transact(ops) {
            Ok(_) => Ok(ExecuteResponse::CreatedDatabase { existed: false }),
            Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedDatabase { existed: true }),
            Err(err) => Err(err),
        }
    }

    fn sequence_create_schema(
        &mut self,
        database_name: DatabaseSpecifier,
        schema_name: String,
        if_not_exists: bool,
    ) -> Result<ExecuteResponse, failure::Error> {
        let op = catalog::Op::CreateSchema {
            database_name,
            schema_name,
        };
        match self.catalog_transact(vec![op]) {
            Ok(_) => Ok(ExecuteResponse::CreatedSchema { existed: false }),
            Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedSchema { existed: true }),
            Err(err) => Err(err),
        }
    }

    fn sequence_create_table(
        &mut self,
        pcx: PlanContext,
        name: FullName,
        desc: RelationDesc,
        if_not_exists: bool,
    ) -> Result<ExecuteResponse, failure::Error> {
        let source_id = self.catalog.allocate_id()?;
        let source = catalog::Source {
            create_sql: "TODO".to_string(),
            plan_cx: pcx,
            connector: dataflow_types::SourceConnector::Local,
            desc,
        };
        let index_id = self.catalog.allocate_id()?;
        let mut index_name = name.clone();
        index_name.item += "_primary_idx";
        let index =
            auto_generate_src_idx(index_name.item.clone(), name.clone(), &source, source_id);
        match self.catalog_transact(vec![
            catalog::Op::CreateItem {
                id: source_id,
                name: name.clone(),
                item: CatalogItem::Source(source.clone()),
            },
            catalog::Op::CreateItem {
                id: index_id,
                name: index_name,
                item: CatalogItem::Index(index.clone()),
            },
        ]) {
            Ok(_) => {
                self.views.insert(source_id, ViewState::new(false, vec![]));
                broadcast(
                    &mut self.broadcast_tx,
                    SequencedCommand::CreateLocalInput {
                        name: name.to_string(),
                        index_id,
                        index: IndexDesc {
                            on_id: index.on,
                            keys: index.keys.clone(),
                        },
                        on_type: source.desc.typ().clone(),
                        advance_to: self.local_input_time,
                    },
                );
                self.insert_index(index_id, &index, self.logical_compaction_window_ms);
                Ok(ExecuteResponse::CreatedTable { existed: false })
            }
            Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedTable { existed: true }),
            Err(err) => Err(err),
        }
    }

    fn sequence_create_source(
        &mut self,
        pcx: PlanContext,
        name: FullName,
        source: sql::Source,
        if_not_exists: bool,
        materialized: bool,
    ) -> Result<ExecuteResponse, failure::Error> {
        let source = catalog::Source {
            create_sql: source.create_sql,
            plan_cx: pcx,
            connector: source.connector,
            desc: source.desc,
        };
        let source_id = self.catalog.allocate_id()?;
        let mut ops = vec![catalog::Op::CreateItem {
            id: source_id,
            name: name.clone(),
            item: CatalogItem::Source(source.clone()),
        }];
        let (index_id, index) = if materialized {
            let mut index_name = name.clone();
            index_name.item += "_primary_idx";
            let index =
                auto_generate_src_idx(index_name.item.clone(), name.clone(), &source, source_id);
            let index_id = self.catalog.allocate_id()?;
            ops.push(catalog::Op::CreateItem {
                id: index_id,
                name: index_name,
                item: CatalogItem::Index(index.clone()),
            });
            (Some(index_id), Some(index))
        } else {
            (None, None)
        };
        match self.catalog_transact(ops) {
            Ok(()) => {
                self.views.insert(source_id, ViewState::new(false, vec![]));
                if materialized {
                    let mut dataflow = DataflowDesc::new(name.to_string());
                    self.import_source_or_view(&source_id, &source_id, &mut dataflow);
                    self.build_arrangement(
                        &index_id.unwrap(),
                        index.unwrap(),
                        source.desc.typ().clone(),
                        dataflow,
                    );
                }
                Ok(ExecuteResponse::CreatedSource { existed: false })
            }
            Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedSource { existed: true }),
            Err(err) => Err(err),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn sequence_create_sink(
        &mut self,
        pcx: PlanContext,
        mut internal_cmd_tx: futures::channel::mpsc::UnboundedSender<Message>,
        tx: ClientTransmitter<ExecuteResponse>,
        session: Session,
        name: FullName,
        sink: sql::Sink,
        if_not_exists: bool,
    ) {
        // First try to allocate an ID. If that fails, we're done.
        let id = match self.catalog.allocate_id() {
            Ok(id) => id,
            Err(e) => {
                tx.send(Err(e.into()), session);
                return;
            }
        };

        // Then try to create a placeholder catalog item with an unknown
        // connector. If that fails, we're done, though if the client specified
        // `if_not_exists` we'll tell the client we succeeded.
        //
        // This placeholder catalog item reserves the name while we create
        // the sink connector, which could take an arbitrarily long time.
        let op = catalog::Op::CreateItem {
            id,
            name,
            item: CatalogItem::Sink(catalog::Sink {
                create_sql: sink.create_sql,
                plan_cx: pcx,
                from: sink.from,
                connector: catalog::SinkConnectorState::Pending(sink.connector_builder.clone()),
            }),
        };
        match self.catalog_transact(vec![op]) {
            Ok(()) => (),
            Err(_) if if_not_exists => {
                tx.send(Ok(ExecuteResponse::CreatedSink { existed: true }), session);
                return;
            }
            Err(e) => {
                tx.send(Err(e), session);
                return;
            }
        }

        // Now we're ready to create the sink connector. Arrange to notify the
        // main coordinator thread when the future completes.
        let connector_builder = sink.connector_builder;
        tokio::spawn(async move {
            internal_cmd_tx
                .send(Message::SinkConnectorReady {
                    session,
                    tx,
                    id,
                    result: sink_connector::build(connector_builder, id).await,
                })
                .await
                .expect("sending to internal_cmd_tx cannot fail");
        });
    }

    fn sequence_create_view(
        &mut self,
        pcx: PlanContext,
        name: FullName,
        view: sql::View,
        replace: Option<GlobalId>,
        materialize: bool,
        if_not_exists: bool,
    ) -> Result<ExecuteResponse, failure::Error> {
        let mut ops = vec![];
        if let Some(id) = replace {
            ops.extend(self.catalog.drop_items_ops(&[id]));
        }
        let view_id = self.catalog.allocate_id()?;
        let view = catalog::View {
            create_sql: view.create_sql,
            plan_cx: pcx,
            optimized_expr: self.optimizer.optimize(view.expr, self.catalog.indexes())?,
            desc: view.desc,
        };
        ops.push(catalog::Op::CreateItem {
            id: view_id,
            name: name.clone(),
            item: CatalogItem::View(view.clone()),
        });
        let (index_id, index) = if materialize {
            let mut index_name = name.clone();
            index_name.item += "_primary_idx";
            let index =
                auto_generate_view_idx(index_name.item.clone(), name.clone(), &view, view_id);
            let index_id = self.catalog.allocate_id()?;
            ops.push(catalog::Op::CreateItem {
                id: index_id,
                name: index_name,
                item: CatalogItem::Index(index.clone()),
            });
            (Some(index_id), Some(index))
        } else {
            (None, None)
        };
        match self.catalog_transact(ops) {
            Ok(()) => {
                self.insert_view(view_id, &view);
                if materialize {
                    let mut dataflow = DataflowDesc::new(name.to_string());
                    self.build_view_collection(&view_id, &view, &mut dataflow);
                    self.build_arrangement(
                        &index_id.unwrap(),
                        index.unwrap(),
                        view.desc.typ().clone(),
                        dataflow,
                    );
                }
                Ok(ExecuteResponse::CreatedView { existed: false })
            }
            Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedView { existed: true }),
            Err(err) => Err(err),
        }
    }

    fn sequence_create_index(
        &mut self,
        pcx: PlanContext,
        name: FullName,
        index: sql::Index,
        if_not_exists: bool,
    ) -> Result<ExecuteResponse, failure::Error> {
        let index = catalog::Index {
            create_sql: index.create_sql,
            plan_cx: pcx,
            keys: index.keys,
            on: index.on,
        };
        let id = self.catalog.allocate_id()?;
        let op = catalog::Op::CreateItem {
            id,
            name: name.clone(),
            item: CatalogItem::Index(index.clone()),
        };
        match self.catalog_transact(vec![op]) {
            Ok(()) => {
                self.create_index_dataflow(name.to_string(), id, index);
                Ok(ExecuteResponse::CreatedIndex { existed: false })
            }
            Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedIndex { existed: true }),
            Err(err) => Err(err),
        }
    }

    fn sequence_drop_database(&mut self, name: String) -> Result<ExecuteResponse, failure::Error> {
        let ops = self.catalog.drop_database_ops(name);
        self.catalog_transact(ops)?;
        Ok(ExecuteResponse::DroppedDatabase)
    }

    fn sequence_drop_schema(
        &mut self,
        database_name: DatabaseSpecifier,
        schema_name: String,
    ) -> Result<ExecuteResponse, failure::Error> {
        let ops = self.catalog.drop_schema_ops(database_name, schema_name);
        self.catalog_transact(ops)?;
        Ok(ExecuteResponse::DroppedSchema)
    }

    fn sequence_drop_items(
        &mut self,
        items: Vec<GlobalId>,
        ty: ObjectType,
    ) -> Result<ExecuteResponse, failure::Error> {
        let ops = self.catalog.drop_items_ops(&items);
        self.catalog_transact(ops)?;
        Ok(match ty {
            ObjectType::Schema => unreachable!(),
            ObjectType::Source => ExecuteResponse::DroppedSource,
            ObjectType::View => ExecuteResponse::DroppedView,
            ObjectType::Table => ExecuteResponse::DroppedTable,
            ObjectType::Sink => ExecuteResponse::DroppedSink,
            ObjectType::Index => ExecuteResponse::DroppedIndex,
        })
    }

    fn sequence_show_all_variables(
        &mut self,
        session: &Session,
    ) -> Result<ExecuteResponse, failure::Error> {
        Ok(send_immediate_rows(
            session
                .vars()
                .iter()
                .map(|v| {
                    Row::pack(&[
                        Datum::String(v.name()),
                        Datum::String(&v.value()),
                        Datum::String(v.description()),
                    ])
                })
                .collect(),
        ))
    }

    fn sequence_show_variable(
        &self,
        session: &Session,
        name: String,
    ) -> Result<ExecuteResponse, failure::Error> {
        let variable = session.get(&name)?;
        let row = Row::pack(&[Datum::String(&variable.value())]);
        Ok(send_immediate_rows(vec![row]))
    }

    fn sequence_set_variable(
        &self,
        session: &mut Session,
        name: String,
        value: String,
    ) -> Result<ExecuteResponse, failure::Error> {
        session.set(&name, &value)?;
        Ok(ExecuteResponse::SetVariable { name })
    }

    fn sequence_peek(
        &mut self,
        conn_id: u32,
        mut source: RelationExpr,
        when: PeekWhen,
        finishing: RowSetFinishing,
        materialize: bool,
    ) -> Result<ExecuteResponse, failure::Error> {
        let timestamp = self.determine_timestamp(&source, when)?;

        // See if the query is introspecting its own logical timestamp, and
        // install the determined timestamp if so.
        source.visit_scalars_mut(&mut |e| {
            if let ScalarExpr::CallNullary(f @ NullaryFunc::MzLogicalTimestamp) = e {
                *e = ScalarExpr::literal_ok(Datum::from(timestamp as i128), f.output_type());
            }
        });

        // TODO (wangandi): Is there anything that optimizes to a
        // constant expression that originally contains a global get? Is
        // there anything not containing a global get that cannot be
        // optimized to a constant expression?
        let mut source = self.optimizer.optimize(source, self.catalog.indexes())?;

        // If this optimizes to a constant expression, we can immediately return the result.
        if let RelationExpr::Constant { rows, typ: _ } = source.as_ref() {
            let mut results = Vec::new();
            for &(ref row, count) in rows {
                assert!(
                    count >= 0,
                    "Negative multiplicity in constant result: {}",
                    count
                );
                for _ in 0..count {
                    results.push(row.clone());
                }
            }
            finishing.finish(&mut results);
            Ok(send_immediate_rows(results))
        } else {
            // Peeks describe a source of data and a timestamp at which to view its contents.
            //
            // We need to determine both an appropriate timestamp from the description, and
            // also to ensure that there is a view in place to query, if the source of data
            // for the peek is not a base relation.

            // Choose a timestamp for all workers to use in the peek.
            // We minimize over all participating views, to ensure that the query will not
            // need to block on the arrival of further input data.
            let (rows_tx, rows_rx) = self.switchboard.mpsc_limited(self.num_timely_workers);

            let (project, filter) = Self::plan_peek(source.as_mut());

            let (fast_path, index_id) = if let RelationExpr::Get {
                id: Id::Global(id),
                typ: _,
            } = source.as_ref()
            {
                if let Some(Some((index_id, _))) = self.views.get(&id).map(|v| &v.default_idx) {
                    (true, *index_id)
                } else if materialize {
                    (false, self.catalog.allocate_id()?)
                } else {
                    bail!(
                        "{} is not materialized",
                        self.catalog.humanize_id(expr::Id::Global(*id)).unwrap()
                    )
                }
            } else {
                (false, self.catalog.allocate_id()?)
            };

            let index = if !fast_path {
                // Slow path. We need to perform some computation, so build
                // a new transient dataflow that will be dropped after the
                // peek completes.
                let typ = source.as_ref().typ();
                let ncols = typ.column_types.len();
                // Cheat a little bit here to get a relation description. A
                // relation description is just a relation type with column
                // names, but we don't know the column names for `source`
                // here. Nothing in the dataflow layer cares about column
                // names, so just set them all to `None`. The column names
                // will ultimately be correctly transmitted to the client
                // because they are safely stashed in the connection's
                // session.
                let desc = RelationDesc::new(
                    typ.clone(),
                    iter::repeat::<Option<ColumnName>>(None).take(ncols),
                );
                let view_id = self.catalog.allocate_id()?;
                let view_name = FullName {
                    database: DatabaseSpecifier::Ambient,
                    schema: "temp".into(),
                    item: format!("temp-view-{}", view_id),
                };
                let index_name = format!("temp-index-on-{}", view_id);
                let mut dataflow = DataflowDesc::new(view_name.to_string());
                dataflow.as_of(Some(vec![timestamp.clone()]));
                let view = catalog::View {
                    create_sql: "<none>".into(),
                    plan_cx: PlanContext::default(),
                    optimized_expr: source,
                    desc,
                };
                self.build_view_collection(&view_id, &view, &mut dataflow);
                let index = auto_generate_view_idx(index_name, view_name, &view, view_id);
                self.build_arrangement(&index_id, index.clone(), typ, dataflow);
                Some(index)
            } else {
                None
            };

            broadcast(
                &mut self.broadcast_tx,
                SequencedCommand::Peek {
                    id: index_id,
                    conn_id,
                    tx: rows_tx,
                    timestamp,
                    finishing: finishing.clone(),
                    project,
                    filter,
                },
            );

            if !fast_path {
                self.drop_indexes(vec![(index_id, &index.unwrap())]);
            }

            let rows_rx = rows_rx
                .try_fold(PeekResponse::Rows(vec![]), |memo, resp| {
                    match (memo, resp) {
                        (PeekResponse::Rows(mut memo), PeekResponse::Rows(rows)) => {
                            memo.extend(rows);
                            future::ok(PeekResponse::Rows(memo))
                        }
                        (PeekResponse::Error(e), _) | (_, PeekResponse::Error(e)) => {
                            future::ok(PeekResponse::Error(e))
                        }
                        (PeekResponse::Canceled, _) | (_, PeekResponse::Canceled) => {
                            future::ok(PeekResponse::Canceled)
                        }
                    }
                })
                .map_ok(move |mut resp| {
                    if let PeekResponse::Rows(rows) = &mut resp {
                        finishing.finish(rows)
                    }
                    resp
                })
                .err_into();

            Ok(ExecuteResponse::SendingRows(Box::pin(rows_rx)))
        }
    }

    fn sequence_tail(
        &mut self,
        conn_id: u32,
        source_id: GlobalId,
    ) -> Result<ExecuteResponse, failure::Error> {
        // Determine the frontier of updates to tail *from*.
        // Updates greater or equal to this frontier will be produced.
        let since = if let Some(Some((index_id, _))) = self
            .views
            .get(&source_id)
            .map(|view_state| &view_state.default_idx)
        {
            self.upper_of(index_id)
                .expect("name missing at coordinator")
                .get(0)
                .copied()
                .unwrap_or(Timestamp::max_value())
        } else {
            0 as Timestamp
        };

        let sink_name = format!(
            "tail-source-{}",
            self.catalog
                .humanize_id(Id::Global(source_id))
                .expect("Source id is known to exist in catalog")
        );
        let sink_id = self.catalog.allocate_id()?;
        self.active_tails.insert(conn_id, sink_id);
        let (tx, rx) = self.switchboard.mpsc_limited(self.num_timely_workers);
        self.create_sink_dataflow(
            sink_name,
            sink_id,
            source_id,
            SinkConnector::Tail(TailSinkConnector { tx, since }),
        );
        Ok(ExecuteResponse::Tailing { rx })
    }

    fn sequence_explain_plan(
        &mut self,
        sql: String,
        raw_plan: sql::RelationExpr,
        decorrelated_plan: Result<expr::RelationExpr, failure::Error>,
        row_set_finishing: Option<RowSetFinishing>,
        stage: ExplainStage,
        options: ExplainOptions,
    ) -> Result<ExecuteResponse, failure::Error> {
        let explanation_string = match stage {
            ExplainStage::Sql => sql,
            ExplainStage::RawPlan => {
                let mut explanation = raw_plan.explain(&self.catalog);
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                if options.typed {
                    // TODO(jamii) does this fail?
                    explanation.explain_types(&BTreeMap::new());
                }
                explanation.to_string()
            }
            ExplainStage::DecorrelatedPlan => {
                let plan = decorrelated_plan?;
                let mut explanation = plan.explain(&self.catalog);
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                if options.typed {
                    explanation.explain_types();
                }
                explanation.to_string()
            }
            ExplainStage::OptimizedPlan => {
                let optimized_plan = self
                    .optimizer
                    .optimize(decorrelated_plan?, self.catalog.indexes())?
                    .into_inner();
                let mut explanation = optimized_plan.explain(&self.catalog);
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                if options.typed {
                    explanation.explain_types();
                }
                explanation.to_string()
            }
        };
        let rows = vec![Row::pack(&[Datum::from(&*explanation_string)])];
        Ok(send_immediate_rows(rows))
    }

    fn sequence_send_diffs(
        &mut self,
        id: GlobalId,
        updates: Vec<(Row, isize)>,
        affected_rows: usize,
        kind: MutationKind,
    ) -> Result<ExecuteResponse, failure::Error> {
        let updates = updates
            .into_iter()
            .map(|(row, diff)| Update {
                row,
                diff,
                timestamp: self.local_input_time,
            })
            .collect();

        self.local_input_time += 1;

        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::Insert {
                id,
                updates,
                advance_to: self.local_input_time,
            },
        );

        Ok(match kind {
            MutationKind::Delete => ExecuteResponse::Deleted(affected_rows),
            MutationKind::Insert => ExecuteResponse::Inserted(affected_rows),
            MutationKind::Update => ExecuteResponse::Updated(affected_rows),
        })
    }

    fn sequence_show_views(
        &mut self,
        ids: Vec<(String, GlobalId)>,
        full: bool,
        show_queryable: bool,
        limit_materialized: bool,
    ) -> Result<ExecuteResponse, failure::Error> {
        let view_information = ids
            .into_iter()
            .filter_map(|(name, id)| {
                let class = match id {
                    GlobalId::System(_) => "SYSTEM",
                    GlobalId::User(_) => "USER",
                };
                if let Some(view_state) = self.views.get(&id) {
                    if !limit_materialized || view_state.default_idx.is_some() {
                        Some((
                            name,
                            class,
                            view_state.queryable,
                            view_state.default_idx.is_some(),
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let mut rows = view_information
            .into_iter()
            .map(|(name, class, queryable, materialized)| {
                let mut datums = vec![Datum::from(name.as_str())];
                if full {
                    datums.push(Datum::from(class));
                    if show_queryable {
                        datums.push(Datum::from(queryable));
                    }
                    if !limit_materialized {
                        datums.push(Datum::from(materialized));
                    }
                }
                Row::pack(&datums)
            })
            .collect::<Vec<_>>();
        rows.sort_unstable_by(move |a, b| a.unpack_first().cmp(&b.unpack_first()));
        Ok(send_immediate_rows(rows))
    }

    fn catalog_transact(&mut self, ops: Vec<catalog::Op>) -> Result<(), failure::Error> {
        let mut sources_to_drop = vec![];
        let mut views_to_drop = vec![];
        let mut sinks_to_drop = vec![];
        let mut indexes_to_drop = vec![];

        let statuses = self.catalog.transact(ops)?;
        for status in &statuses {
            match status {
                catalog::OpStatus::CreatedItem(id) => {
                    let name = self.catalog.humanize_id(expr::Id::Global(*id)).unwrap();
                    self.report_catalog_update(*id, name, true);
                }
                catalog::OpStatus::DroppedItem(entry) => {
                    self.report_catalog_update(entry.id(), entry.name().to_string(), false);
                    match entry.item() {
                        CatalogItem::Source(_) => {
                            sources_to_drop.push(entry.id());
                            views_to_drop.push(entry.id());
                        }
                        CatalogItem::View(_) => views_to_drop.push(entry.id()),
                        CatalogItem::Sink(catalog::Sink {
                            connector: SinkConnectorState::Ready(connector),
                            ..
                        }) => {
                            sinks_to_drop.push(entry.id());
                            match connector {
                                SinkConnector::Kafka(KafkaSinkConnector { topic, .. }) => {
                                    broadcast(
                                        &mut self.broadcast_tx,
                                        SequencedCommand::AppendLog(MaterializedEvent::KafkaSink {
                                            id: entry.id(),
                                            topic: topic.clone(),
                                            insert: false,
                                        }),
                                    );
                                }
                                SinkConnector::AvroOcf(AvroOcfSinkConnector { path }) => {
                                    broadcast(
                                        &mut self.broadcast_tx,
                                        SequencedCommand::AppendLog(
                                            MaterializedEvent::AvroOcfSink {
                                                id: entry.id(),
                                                path: path.clone().into_os_string().into_vec(),
                                                insert: false,
                                            },
                                        ),
                                    );
                                }
                                _ => (),
                            }
                        }
                        CatalogItem::Sink(catalog::Sink {
                            connector: SinkConnectorState::Pending(_),
                            ..
                        }) => {
                            // If the sink connector state is pending, the sink
                            // dataflow was never created, so nothing to drop.
                        }
                        CatalogItem::Index(idx) => indexes_to_drop.push((entry.id(), idx)),
                    }
                }
                _ => (),
            }
        }

        if !sources_to_drop.is_empty() {
            broadcast(
                &mut self.broadcast_tx,
                SequencedCommand::DropSources(sources_to_drop),
            );
        }
        if !views_to_drop.is_empty() {
            for id in views_to_drop {
                self.views.remove(&id);
            }
        }
        if !sinks_to_drop.is_empty() {
            broadcast(
                &mut self.broadcast_tx,
                SequencedCommand::DropSinks(sinks_to_drop),
            );
        }
        if !indexes_to_drop.is_empty() {
            self.drop_indexes(indexes_to_drop);
        }

        Ok(())
    }

    fn import_source_or_view(
        &self,
        orig_id: &GlobalId,
        id: &GlobalId,
        dataflow: &mut DataflowDesc,
    ) {
        if dataflow.objects_to_build.iter().any(|bd| &bd.id == id)
            || dataflow.source_imports.iter().any(|(i, _)| &i.sid == id)
        {
            return;
        }
        if let Some((index_id, keys)) = &self.views[id].default_idx {
            let index_desc = IndexDesc {
                on_id: *id,
                keys: keys.to_vec(),
            };
            match self.catalog.get_by_id(id).item() {
                CatalogItem::View(view) => {
                    dataflow.add_index_import(*index_id, index_desc, view.desc.typ().clone(), *id);
                }
                CatalogItem::Source(source) => {
                    dataflow.add_index_import(
                        *index_id,
                        index_desc,
                        source.desc.typ().clone(),
                        *id,
                    );
                }
                _ => unreachable!(),
            }
        } else {
            match self.catalog.get_by_id(id).item() {
                CatalogItem::Source(source) => {
                    // A source is being imported as part of a new view. We have to notify the timestamping
                    // thread that a source instance is being created for this view
                    let instance_id = SourceInstanceId {
                        sid: *id,
                        vid: *orig_id,
                    };
                    dataflow.add_source_import(
                        instance_id,
                        source.connector.clone(),
                        source.desc.clone(),
                    );
                }
                CatalogItem::View(view) => {
                    self.build_view_collection(id, &view, dataflow);
                }
                _ => unreachable!(),
            }
        }
    }

    fn build_view_collection(
        &self,
        view_id: &GlobalId,
        view: &catalog::View,
        dataflow: &mut DataflowDesc,
    ) {
        // TODO: We only need to import Get arguments for which we cannot find arrangements.
        view.optimized_expr.as_ref().visit(&mut |e| {
            if let RelationExpr::Get {
                id: Id::Global(id),
                typ: _,
            } = e
            {
                self.import_source_or_view(view_id, &id, dataflow);
                dataflow.add_dependency(*view_id, *id)
            }
        });
        // Collect sources, views, and indexes used.
        view.optimized_expr.as_ref().visit(&mut |e| {
            if let RelationExpr::ArrangeBy { input, keys } = e {
                if let RelationExpr::Get {
                    id: Id::Global(on_id),
                    typ,
                } = &**input
                {
                    for key_set in keys {
                        let index_desc = IndexDesc {
                            on_id: *on_id,
                            keys: key_set.to_vec(),
                        };
                        // If the arrangement exists, import it. It may not exist, in which
                        // case we should import the source to be sure that we have access
                        // to the collection to arrange it ourselves.
                        if let Some(view) = self.views.get(on_id) {
                            if let Some(ids) = view.primary_idxes.get(key_set) {
                                dataflow.add_index_import(
                                    *ids.first().unwrap(),
                                    index_desc,
                                    typ.clone(),
                                    *view_id,
                                );
                            }
                        }
                    }
                }
            }
        });
        dataflow.add_view_to_build(
            *view_id,
            view.optimized_expr.clone(),
            view.desc.typ().clone(),
        );
    }

    fn build_arrangement(
        &mut self,
        id: &GlobalId,
        index: catalog::Index,
        on_type: RelationType,
        mut dataflow: DataflowDesc,
    ) {
        self.import_source_or_view(id, &index.on, &mut dataflow);
        dataflow.add_index_to_build(*id, index.on.clone(), on_type.clone(), index.keys.clone());
        dataflow.add_index_export(*id, index.on, on_type, index.keys.clone());
        // TODO: should we still support creating multiple dataflows with a single command,
        // Or should it all be compacted into a single DataflowDesc with multiple exports?
        dataflow.optimize();
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::CreateDataflows(vec![dataflow]),
        );
        self.insert_index(*id, &index, self.logical_compaction_window_ms);
    }

    fn create_index_dataflow(&mut self, name: String, id: GlobalId, index: catalog::Index) {
        let dataflow = DataflowDesc::new(name);
        let on_type = self
            .catalog
            .get_by_id(&index.on)
            .desc()
            .unwrap()
            .typ()
            .clone();
        self.build_arrangement(&id, index, on_type, dataflow);
    }

    fn handle_sink_connector_ready(&mut self, id: GlobalId, connector: SinkConnector) {
        // Update catalog entry with sink connector.
        let entry = self.catalog.get_by_id(&id);
        let name = entry.name().clone();
        let mut sink = match entry.item() {
            CatalogItem::Sink(sink) => sink.clone(),
            _ => unreachable!(),
        };
        sink.connector = catalog::SinkConnectorState::Ready(connector.clone());
        let ops = vec![
            catalog::Op::DropItem(id),
            catalog::Op::CreateItem {
                id,
                name: name.clone(),
                item: CatalogItem::Sink(sink.clone()),
            },
        ];
        self.catalog
            .transact(ops)
            .expect("replacing a sink cannot fail");

        // Create the sink dataflow.
        self.create_sink_dataflow(name.to_string(), id, sink.from, connector)
    }

    fn create_sink_dataflow(
        &mut self,
        name: String,
        id: GlobalId,
        from: GlobalId,
        connector: SinkConnector,
    ) {
        #[allow(clippy::single_match)]
        match &connector {
            SinkConnector::Kafka(KafkaSinkConnector { topic, .. }) => {
                broadcast(
                    &mut self.broadcast_tx,
                    SequencedCommand::AppendLog(MaterializedEvent::KafkaSink {
                        id,
                        topic: topic.clone(),
                        insert: true,
                    }),
                );
            }
            SinkConnector::AvroOcf(AvroOcfSinkConnector { path }) => {
                broadcast(
                    &mut self.broadcast_tx,
                    SequencedCommand::AppendLog(MaterializedEvent::AvroOcfSink {
                        id,
                        path: path.clone().into_os_string().into_vec(),
                        insert: true,
                    }),
                );
            }
            _ => (),
        }
        let mut dataflow = DataflowDesc::new(name);
        self.import_source_or_view(&id, &from, &mut dataflow);
        let from_type = self.catalog.get_by_id(&from).desc().unwrap().clone();
        dataflow.add_sink_export(id, from, from_type, connector);
        dataflow.optimize();
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::CreateDataflows(vec![dataflow]),
        );
    }

    pub fn drop_sinks(&mut self, dataflow_names: Vec<GlobalId>) {
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::DropSinks(dataflow_names),
        )
    }

    pub fn drop_indexes(&mut self, indexes: Vec<(GlobalId, &catalog::Index)>) {
        let mut trace_keys = Vec::new();
        for (id, idx) in indexes {
            if let Some(index_state) = self.indexes.remove(&id) {
                if self.log {
                    for time in index_state.upper.frontier().iter() {
                        broadcast(
                            &mut self.broadcast_tx,
                            SequencedCommand::AppendLog(MaterializedEvent::Frontier(
                                id,
                                time.clone(),
                                -1,
                            )),
                        );
                    }
                }
                if let Some(view_state) = self.views.get_mut(&idx.on) {
                    view_state.drop_primary_idx(&idx.keys, id);
                    if view_state.default_idx.is_none() {
                        view_state.queryable = false;
                        self.propagate_queryability(&idx.on);
                    }
                }
                trace_keys.push(id);
            }
        }
        if !trace_keys.is_empty() {
            broadcast(
                &mut self.broadcast_tx,
                SequencedCommand::DropIndexes(trace_keys),
            )
        }
    }

    pub fn enable_feedback(&mut self) -> comm::mpsc::Receiver<WorkerFeedbackWithMeta> {
        let (tx, rx) = self.switchboard.mpsc_limited(self.num_timely_workers);
        broadcast(&mut self.broadcast_tx, SequencedCommand::EnableFeedback(tx));
        rx
    }

    pub fn shutdown(&mut self) {
        broadcast(&mut self.broadcast_tx, SequencedCommand::Shutdown)
    }

    pub fn report_catalog_update(&mut self, id: GlobalId, name: String, insert: bool) {
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::AppendLog(MaterializedEvent::Catalog(id, name, insert)),
        );
    }

    /// Perform maintenance work associated with the coordinator.
    ///
    /// Primarily, this involves sequencing compaction commands, which should be
    /// issued whenever available.
    pub fn maintenance(&mut self) {
        // Take this opportunity to drain `since_update` commands.
        // Don't try to compact to an empty frontier. There may be a good reason to do this
        // in principle, but not in any current Mz use case.
        // (For background, see: https://github.com/MaterializeInc/materialize/pull/1113#issuecomment-559281990)
        self.since_updates
            .retain(|(_, frontier)| !frontier.is_empty());
        if !self.since_updates.is_empty() {
            broadcast(
                &mut self.broadcast_tx,
                SequencedCommand::AllowCompaction(std::mem::replace(
                    &mut self.since_updates,
                    Vec::new(),
                )),
            );
        }
    }

    /// Extracts an optional projection around an optional filter.
    ///
    /// This extraction is done to allow workers to process a larger class of queries
    /// without building explicit dataflows, avoiding latency, allocation and general
    /// load on the system. The worker performs the filter and projection in place.
    fn plan_peek(expr: &mut RelationExpr) -> (Option<Vec<usize>>, Vec<expr::ScalarExpr>) {
        let mut outputs_plan = None;
        if let RelationExpr::Project { input, outputs } = expr {
            outputs_plan = Some(outputs.clone());
            *expr = input.take_dangerous();
        }
        let mut predicates_plan = Vec::new();
        if let RelationExpr::Filter { input, predicates } = expr {
            predicates_plan.extend(predicates.iter().cloned());
            *expr = input.take_dangerous();
        }

        // We only apply this transformation if the result is a `Get`.
        // It is harmful to apply it otherwise, as we materialize more data than
        // we would have if we applied the filter and projection beforehand.
        if let RelationExpr::Get { .. } = expr {
            (outputs_plan, predicates_plan)
        } else {
            if !predicates_plan.is_empty() {
                *expr = expr.take_dangerous().filter(predicates_plan);
            }
            if let Some(outputs) = outputs_plan {
                *expr = expr.take_dangerous().project(outputs);
            }
            (None, Vec::new())
        }
    }

    fn propagate_queryability(&mut self, id: &GlobalId) {
        let mut ids_to_propagate = Vec::new();
        for used_by_id in self.catalog.get_by_id(id).used_by().to_owned() {
            //if view is not materialized
            if self.views.contains_key(&used_by_id) && self.views[&used_by_id].default_idx.is_none()
            {
                let new_queryability = self.views[&used_by_id]
                    .uses_views
                    .iter()
                    .all(|id| self.views[id].queryable);
                if let Some(view_state) = self.views.get_mut(&used_by_id) {
                    // we only need to continue propagating if there is a change in queryability
                    if view_state.queryable != new_queryability {
                        ids_to_propagate.push(used_by_id);
                        view_state.queryable = new_queryability;
                    }
                }
            }
        }
        for id in ids_to_propagate {
            self.propagate_queryability(&id);
        }
    }

    fn find_dependent_indexes(&self, id: &GlobalId) -> Vec<GlobalId> {
        let mut results = Vec::new();
        let view_state = &self.views[id];
        if view_state.primary_idxes.is_empty() {
            for id in view_state.uses_views.iter() {
                results.append(&mut self.find_dependent_indexes(id));
            }
        } else {
            for ids in view_state.primary_idxes.values() {
                results.extend(ids.clone());
            }
        }
        results.sort();
        results.dedup();
        results
    }

    /// A policy for determining the timestamp for a peek.
    ///
    /// The result may be `None` in the case that the `when` policy cannot be satisfied,
    /// which is possible due to the restricted validity of traces (each has a `since`
    /// and `upper` frontier, and are only valid after `since` and sure to be available
    /// not after `upper`).
    fn determine_timestamp(
        &mut self,
        source: &RelationExpr,
        when: PeekWhen,
    ) -> Result<Timestamp, failure::Error> {
        if self.symbiosis.is_some() {
            // In symbiosis mode, we enforce serializability by forcing all
            // PEEKs to peek at the latest input time.
            // TODO(benesch): should this be saturating subtraction, and what should happen
            // when `self.local_input_time` is zero?
            assert!(self.local_input_time > 0);
            return Ok(self.local_input_time - 1);
        }

        // Each involved trace has a validity interval `[since, upper)`.
        // The contents of a trace are only guaranteed to be correct when
        // accumulated at a time greater or equal to `since`, and they
        // are only guaranteed to be currently present for times not
        // greater or equal to `upper`.
        //
        // The plan is to first determine a timestamp, based on the requested
        // timestamp policy, and then determine if it can be satisfied using
        // the compacted arrangements we have at hand. It remains unresolved
        // what to do if it cannot be satisfied (perhaps the query should use
        // a larger timestamp and block, perhaps the user should intervene).
        let mut uses_ids = Vec::new();
        source.global_uses(&mut uses_ids);

        uses_ids.sort();
        uses_ids.dedup();
        if uses_ids.iter().any(|id| {
            if let Some(view_state) = self.views.get(id) {
                !view_state.queryable
            } else {
                true
            }
        }) {
            bail!("Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources");
        }
        uses_ids = uses_ids
            .into_iter()
            .flat_map(|id| self.find_dependent_indexes(&id))
            .collect();

        // First determine the candidate timestamp, which is either the explicitly requested
        // timestamp, or the latest timestamp known to be immediately available.
        let timestamp = match when {
            // Explicitly requested timestamps should be respected.
            PeekWhen::AtTimestamp(timestamp) => timestamp,

            // These two strategies vary in terms of which traces drive the
            // timestamp determination process: either the trace itself or the
            // original sources on which they depend.
            PeekWhen::Immediately => {
                // Form lower bound on available times
                let mut upper = Antichain::new();
                for id in uses_ids.iter() {
                    // To track the meet of `upper` we just extend with the upper frontier.
                    upper.extend(self.upper_of(id).unwrap().iter().cloned());
                }

                // We peek at the largest element not in advance of `upper`, which
                // involves a subtraction. If `upper` contains a zero timestamp there
                // is no "prior" answer, and we do not want to peek at it as it risks
                // hanging awaiting the response to data that may never arrive.
                if let Some(candidate) = upper.elements().get(0) {
                    if *candidate > 0 {
                        candidate.saturating_sub(1)
                    } else {
                        bail!("At least one input has no complete timestamps yet.");
                    }
                } else {
                    // A complete trace can be read in its final form with this time.
                    //
                    // This should only happen for literals that have no sources
                    Timestamp::max_value()
                }
            }
        };

        // Determine the valid lower bound of times that can produce correct outputs.
        // This bound is determined by the arrangements contributing to the query,
        // and does not depend on the transitive sources.
        let mut since = Antichain::from_elem(0);
        for id in uses_ids {
            let prior_since = std::mem::replace(&mut since, Antichain::new());
            let view_since = self.since_of(&id).expect("Since missing at coordinator");
            // To track the join of `since` we should replace with the pointwise
            // join of each element of `since` and `view_since`.
            for new_element in view_since.elements() {
                for old_element in prior_since.elements() {
                    use differential_dataflow::lattice::Lattice;
                    since.insert(new_element.join(old_element));
                }
            }
        }

        // If the timestamp is greater or equal to some element in `since` we are
        // assured that the answer will be correct.
        if since.less_equal(&timestamp) {
            Ok(timestamp)
        } else {
            bail!(
                "Latest available timestamp ({}) is not valid for all inputs",
                timestamp
            );
        }
    }

    /// Updates the upper frontier of a named view.
    fn update_upper(&mut self, name: &GlobalId, mut changes: ChangeBatch<Timestamp>) {
        if let Some(index_state) = self.indexes.get_mut(name) {
            let changes: Vec<_> = index_state.upper.update_iter(changes.drain()).collect();
            if !changes.is_empty() {
                if self.log {
                    for (time, change) in changes {
                        broadcast(
                            &mut self.broadcast_tx,
                            SequencedCommand::AppendLog(MaterializedEvent::Frontier(
                                *name, time, change,
                            )),
                        );
                    }
                }

                // Advance the compaction frontier to trail the new frontier.
                // If the compaction latency is `None` compaction messages are
                // not emitted, and the trace should be broadly useable.
                // TODO: If the frontier advances surprisingly quickly, e.g. in
                // the case of a constant collection, this compaction is actively
                // harmful. We should reconsider compaction policy with an eye
                // towards minimizing unexpected screw-ups.
                if let Some(compaction_latency_ms) = index_state.compaction_latency_ms {
                    // Decline to compact complete collections. This would have the
                    // effect of making the collection unusable. Instead, we would
                    // prefer to compact collections only when we believe it would
                    // reduce the volume of the collection, but we don't have that
                    // information here.
                    if !index_state.upper.frontier().is_empty() {
                        index_state.since.clear();
                        for time in index_state.upper.frontier().iter() {
                            index_state
                                .since
                                .insert(time.saturating_sub(compaction_latency_ms));
                        }
                        self.since_updates
                            .push((name.clone(), index_state.since.elements().to_vec()));
                    }
                }
            }
        }
    }

    /// The upper frontier of a maintained index, if it exists.
    fn upper_of(&self, name: &GlobalId) -> Option<AntichainRef<Timestamp>> {
        if let Some(index_state) = self.indexes.get(name) {
            Some(index_state.upper.frontier())
        } else {
            None
        }
    }

    /// The since frontier of a maintained index, if it exists.
    fn since_of(&self, name: &GlobalId) -> Option<&Antichain<Timestamp>> {
        if let Some(index_state) = self.indexes.get(name) {
            Some(&index_state.since)
        } else {
            None
        }
    }

    /// Inserts a view into the coordinator.
    ///
    /// Initializes managed state and logs the insertion (and removal of any existing view).
    fn insert_view(&mut self, view_id: GlobalId, view: &catalog::View) {
        self.views.remove(&view_id);
        let mut uses = Vec::new();
        view.optimized_expr.as_ref().global_uses(&mut uses);
        uses.sort();
        uses.dedup();
        self.views.insert(
            view_id,
            ViewState::new(
                uses.iter().all(|id| {
                    if let Some(view_state) = self.views.get(&id) {
                        view_state.queryable
                    } else {
                        false
                    }
                }),
                uses,
            ),
        );
    }

    /// Add an index to a view in the coordinator.
    fn insert_index(
        &mut self,
        id: GlobalId,
        index: &catalog::Index,
        latency_ms: Option<Timestamp>,
    ) {
        if let Some(viewstate) = self.views.get_mut(&index.on) {
            viewstate.add_primary_idx(&index.keys, id);
            if !viewstate.queryable {
                viewstate.queryable = true;
                self.propagate_queryability(&index.on);
            }
        } // else the view is temporary
        let index_state = IndexState::new(self.num_timely_workers, latency_ms);
        if self.log {
            for time in index_state.upper.frontier().iter() {
                broadcast(
                    &mut self.broadcast_tx,
                    SequencedCommand::AppendLog(MaterializedEvent::Frontier(id, time.clone(), 1)),
                );
            }
        }
        self.indexes.insert(id, index_state);
    }

    fn handle_statement(
        &mut self,
        session: &Session,
        stmt: sql::Statement,
        params: &sql::Params,
    ) -> Result<(PlanContext, sql::Plan), failure::Error> {
        let pcx = PlanContext::default();
        match sql::plan(&pcx, &self.catalog, session, stmt.clone(), params) {
            Ok(plan) => Ok((pcx, plan)),
            Err(err) => match self.symbiosis {
                Some(ref mut postgres) if postgres.can_handle(&stmt) => {
                    let plan = block_on(postgres.execute(&pcx, &self.catalog, session, &stmt))?;
                    Ok((pcx, plan))
                }
                _ => Err(err),
            },
        }
    }

    fn handle_parse(
        &self,
        session: &mut Session,
        name: String,
        sql: String,
    ) -> Result<(), failure::Error> {
        let stmts = sql::parse(sql)?;
        let (stmt, desc, param_types) = match stmts.len() {
            0 => (None, None, vec![]),
            1 => {
                let stmt = stmts.into_element();
                let (desc, param_types) = match sql::describe(&self.catalog, session, stmt.clone())
                {
                    Ok((desc, param_types)) => (desc, param_types),
                    // Describing the query failed. If we're running in symbiosis with
                    // Postgres, see if Postgres can handle it. Note that Postgres
                    // only handles commands that do not return rows, so the
                    // `RelationDesc` is always `None`.
                    Err(err) => match self.symbiosis {
                        Some(ref postgres) if postgres.can_handle(&stmt) => (None, vec![]),
                        _ => return Err(err),
                    },
                };
                (Some(stmt), desc, param_types)
            }
            n => bail!("expected no more than one query, got {}", n),
        };
        session.set_prepared_statement(name, PreparedStatement::new(stmt, desc, param_types));
        Ok(())
    }
}

fn broadcast(tx: &mut comm::broadcast::Sender<SequencedCommand>, cmd: SequencedCommand) {
    // TODO(benesch): avoid flushing after every send.
    block_on(tx.send(cmd)).unwrap();
}

/// Constructs an [`ExecuteResponse`] that that will send some rows to the
/// client immediately, as opposed to asking the dataflow layer to send along
/// the rows after some computation.
fn send_immediate_rows(rows: Vec<Row>) -> ExecuteResponse {
    let (tx, rx) = futures::channel::oneshot::channel();
    tx.send(PeekResponse::Rows(rows)).unwrap();
    ExecuteResponse::SendingRows(Box::pin(rx.err_into()))
}

pub struct IndexState {
    /// The most recent frontier for new data.
    /// All further changes will be in advance of this bound.
    upper: MutableAntichain<Timestamp>,
    /// The compaction frontier.
    /// All peeks in advance of this frontier will be correct,
    /// but peeks not in advance of this frontier may not be.
    since: Antichain<Timestamp>,
    /// Compaction delay.
    ///
    /// This timestamp drives the advancement of the since frontier as a
    /// function of the upper frontier, trailing it by exactly this much.
    compaction_latency_ms: Option<Timestamp>,
}

impl IndexState {
    /// Creates an empty index state from a number of workers.
    pub fn new(workers: usize, compaction_latency_ms: Option<Timestamp>) -> Self {
        let mut upper = MutableAntichain::new();
        upper.update_iter(Some((0, workers as i64)));
        Self {
            upper,
            since: Antichain::from_elem(0),
            compaction_latency_ms,
        }
    }

    /// Sets the latency behind the collection frontier at which compaction occurs.
    #[allow(dead_code)]
    pub fn set_compaction_latency(&mut self, latency_ms: Option<Timestamp>) {
        self.compaction_latency_ms = latency_ms;
    }
}

/// Per-view state.
pub struct ViewState {
    /// Only views, not sources, on which the view depends
    uses_views: Vec<GlobalId>,
    queryable: bool,
    /// keys of default index
    default_idx: Option<(GlobalId, Vec<ScalarExpr>)>,
    // TODO(andiwang): only allow one primary index?
    /// Currently all indexes are primary indexes
    primary_idxes: BTreeMap<Vec<ScalarExpr>, Vec<GlobalId>>,
    // TODO(andiwang): materialize#220 Implement seconary indexes
    // secondary_idxes: BTreeMap<Vec<ScalarExpr>, Vec<GlobalId>>,
}

impl ViewState {
    fn new(queryable: bool, uses_views: Vec<GlobalId>) -> Self {
        ViewState {
            queryable,
            uses_views,
            default_idx: None,
            primary_idxes: BTreeMap::new(),
            //secondary_idxes: BTreeMap::new(),
        }
    }

    pub fn add_primary_idx(&mut self, primary_idx: &[ScalarExpr], id: GlobalId) {
        if self.default_idx.is_none() {
            self.default_idx = Some((id, primary_idx.to_owned()));
        }
        self.primary_idxes
            .entry(primary_idx.to_owned())
            .or_insert_with(Vec::new)
            .push(id);
    }

    pub fn drop_primary_idx(&mut self, primary_idx: &[ScalarExpr], id: GlobalId) {
        let entry = self.primary_idxes.get_mut(primary_idx).unwrap();
        entry.retain(|i| i != &id);
        if entry.is_empty() {
            self.primary_idxes.remove(primary_idx);
        }
        let is_default = if let Some((_, keys)) = &self.default_idx {
            &keys[..] == primary_idx
        } else {
            unreachable!()
        };
        if is_default {
            self.default_idx = self
                .primary_idxes
                .iter()
                .next()
                .map(|(keys, ids)| (*ids.first().unwrap(), keys.to_owned()));
        }
    }
}

pub fn auto_generate_src_idx(
    index_name: String,
    source_name: FullName,
    source: &catalog::Source,
    source_id: GlobalId,
) -> catalog::Index {
    auto_generate_primary_idx(
        index_name,
        &source.desc.typ().keys,
        source_name,
        source_id,
        &source.desc,
    )
}

pub fn auto_generate_view_idx(
    index_name: String,
    view_name: FullName,
    view: &catalog::View,
    view_id: GlobalId,
) -> catalog::Index {
    auto_generate_primary_idx(
        index_name,
        &view.optimized_expr.as_ref().typ().keys,
        view_name,
        view_id,
        &view.desc,
    )
}

pub fn auto_generate_primary_idx(
    index_name: String,
    keys: &[Vec<usize>],
    on_name: FullName,
    on_id: GlobalId,
    on_desc: &RelationDesc,
) -> catalog::Index {
    let keys = if let Some(keys) = keys.first() {
        keys.clone()
    } else {
        (0..on_desc.typ().column_types.len()).collect()
    };
    catalog::Index {
        create_sql: index_sql(index_name, on_name, &on_desc, &keys),
        plan_cx: PlanContext::default(),
        on: on_id,
        keys: keys.into_iter().map(ScalarExpr::Column).collect(),
    }
}

// TODO(benesch): constructing the canonical CREATE INDEX statement should be
// the responsibility of the SQL package.
fn index_sql(
    index_name: String,
    view_name: FullName,
    view_desc: &RelationDesc,
    keys: &[usize],
) -> String {
    use sql_parser::ast::{Expr, Ident, Statement, Value};

    Statement::CreateIndex {
        name: Ident::new(index_name),
        on_name: sql::normalize::unresolve(view_name),
        key_parts: keys
            .iter()
            .map(|i| match view_desc.get_unambiguous_name(*i) {
                Some(n) => Expr::Identifier(Ident::new(n.to_string())),
                _ => Expr::Value(Value::Number((i + 1).to_string())),
            })
            .collect(),
        if_not_exists: false,
    }
    .to_string()
}

fn open_catalog(
    data_directory: Option<&Path>,
    logging_config: Option<&LoggingConfig>,
    mut optimizer: Optimizer,
) -> Result<Catalog, failure::Error> {
    let path = if let Some(data_directory) = data_directory {
        Some(data_directory.join("catalog"))
    } else {
        None
    };
    let path = path.as_deref();
    Ok(if let Some(logging_config) = logging_config {
        Catalog::open::<SqlSerializer, _>(path, |catalog| {
            for log_src in logging_config.active_logs() {
                let view_name = FullName {
                    database: DatabaseSpecifier::Ambient,
                    schema: "mz_catalog".into(),
                    item: log_src.name().into(),
                };
                let index_name = format!("{}_primary_idx", log_src.name());
                catalog.insert_item(
                    log_src.id(),
                    FullName {
                        database: DatabaseSpecifier::Ambient,
                        schema: "mz_catalog".into(),
                        item: log_src.name().into(),
                    },
                    CatalogItem::Source(catalog::Source {
                        create_sql: "TODO".to_string(),
                        plan_cx: PlanContext::default(),
                        connector: dataflow_types::SourceConnector::Local,
                        desc: log_src.schema(),
                    }),
                );
                catalog.insert_item(
                    log_src.index_id(),
                    FullName {
                        database: DatabaseSpecifier::Ambient,
                        schema: "mz_catalog".into(),
                        item: index_name.clone(),
                    },
                    CatalogItem::Index(catalog::Index {
                        on: log_src.id(),
                        keys: log_src
                            .index_by()
                            .into_iter()
                            .map(ScalarExpr::Column)
                            .collect(),
                        create_sql: index_sql(
                            index_name,
                            view_name,
                            &log_src.schema(),
                            &log_src.index_by(),
                        ),
                        plan_cx: PlanContext::default(),
                    }),
                );
            }

            for log_view in logging_config.active_views() {
                let pcx = PlanContext::default();
                let params = Params {
                    datums: Row::pack(&[]),
                    types: vec![],
                };
                let stmt = sql::parse(log_view.sql.to_owned())
                    .expect("failed to parse bootstrap sql")
                    .into_element();
                match sql::plan(&pcx, catalog, &sql::InternalSession, stmt, &params) {
                    Ok(Plan::CreateView {
                        name: _,
                        view,
                        replace,
                        materialize,
                        if_not_exists,
                    }) => {
                        assert!(replace.is_none());
                        assert!(!if_not_exists);
                        assert!(materialize);
                        let view = catalog::View {
                            create_sql: view.create_sql,
                            plan_cx: pcx,
                            optimized_expr: optimizer
                                .optimize(view.expr, catalog.indexes())
                                .expect("failed to optimize bootstrap sql"),
                            desc: view.desc,
                        };
                        let view_name = FullName {
                            database: DatabaseSpecifier::Ambient,
                            schema: "mz_catalog".into(),
                            item: log_view.name.into(),
                        };
                        let index_name = format!("{}_primary_idx", log_view.name);
                        let index = auto_generate_view_idx(
                            index_name.clone(),
                            view_name.clone(),
                            &view,
                            log_view.id,
                        );
                        catalog.insert_item(log_view.id, view_name, CatalogItem::View(view));
                        catalog.insert_item(
                            log_view.index_id,
                            FullName {
                                database: DatabaseSpecifier::Ambient,
                                schema: "mz_catalog".into(),
                                item: index_name,
                            },
                            CatalogItem::Index(index),
                        );
                    }
                    err => panic!(
                        "internal error: failed to load bootstrap view:\n{}\nerror:\n{:?}",
                        log_view.sql, err
                    ),
                }
            }
        })?
    } else {
        Catalog::open::<SqlSerializer, _>(path, |_| ())?
    })
}

/// Loads the catalog stored at `data_directory` and returns its serialized state.
///
/// There are no guarantees about the format of the serialized state, except that
/// the serialized state for two identical catalogs will compare identically.
pub fn dump_catalog(data_directory: &Path) -> Result<String, failure::Error> {
    let logging_config = LoggingConfig::new(Duration::from_secs(0));
    let catalog = open_catalog(
        Some(data_directory),
        Some(&logging_config),
        Optimizer::default(),
    )?;
    Ok(catalog.dump())
}
