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

use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::iter;
use std::os::unix::ffi::OsStringExt;
use std::path::Path;
use std::thread;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context};
use differential_dataflow::lattice::Lattice;
use futures::executor::block_on;
use futures::future::{self, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{self, StreamExt, TryStreamExt};
use timely::progress::{Antichain, ChangeBatch, Timestamp as _};

use dataflow::source::persistence::PersistenceSender;
use dataflow::{PersistenceMessage, SequencedCommand, WorkerFeedback, WorkerFeedbackWithMeta};
use dataflow_types::logging::LoggingConfig;
use dataflow_types::{
    AvroOcfSinkConnector, DataflowDesc, IndexDesc, KafkaSinkConnector, PeekResponse, SinkConnector,
    SourceConnector, TailSinkConnector, Timestamp, TimestampSourceUpdate, Update,
};
use expr::{
    GlobalId, Id, IdHumanizer, NullaryFunc, OptimizedRelationExpr, RelationExpr, RowSetFinishing,
    ScalarExpr, SourceInstanceId,
};
use ore::thread::JoinHandleExt;
use repr::{ColumnName, Datum, RelationDesc, RelationType, Row, RowPacker};
use sql::ast::display::AstDisplay;
use sql::ast::{
    CreateIndexStatement, CreateTableStatement, DropObjectsStatement, ExplainOptions, ExplainStage,
    ObjectType, Statement,
};
use sql::catalog::Catalog as _;
use sql::names::{DatabaseSpecifier, FullName};
use sql::plan::{MutationKind, Params, PeekWhen, Plan, PlanContext};
use transform::Optimizer;

use self::arrangement_state::{ArrangementFrontiers, Frontiers};
use crate::catalog::builtin::{
    BUILTINS, MZ_AVRO_OCF_SINKS, MZ_CATALOG_NAMES, MZ_DATABASES, MZ_KAFKA_SINKS, MZ_SCHEMAS,
    MZ_VIEW_FOREIGN_KEYS, MZ_VIEW_KEYS,
};
use crate::catalog::{self, Catalog, CatalogItem, SinkConnectorState};
use crate::persistence::{PersistenceConfig, Persister};
use crate::session::{PreparedStatement, Session};
use crate::timestamp::{TimestampConfig, TimestampMessage, Timestamper};
use crate::util::ClientTransmitter;
use crate::{sink_connector, Command, ExecuteResponse, Response, StartupMessage};

mod arrangement_state;

pub enum Message {
    Command(Command),
    Worker(WorkerFeedbackWithMeta),
    AdvanceSourceTimestamp {
        id: SourceInstanceId,
        update: TimestampSourceUpdate,
    },
    StatementReady {
        session: Session,
        tx: ClientTransmitter<ExecuteResponse>,
        result: Result<sql::ast::Statement, anyhow::Error>,
        params: Params,
    },
    SinkConnectorReady {
        session: Session,
        tx: ClientTransmitter<ExecuteResponse>,
        id: GlobalId,
        result: Result<SinkConnector, anyhow::Error>,
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
    pub logging_granularity: Option<Duration>,
    pub data_directory: Option<&'a Path>,
    pub executor: &'a tokio::runtime::Handle,
    pub timestamp: TimestampConfig,
    pub persistence: Option<PersistenceConfig>,
    pub logical_compaction_window: Option<Duration>,
    pub experimental_mode: bool,
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
    /// Maps (global Id of arrangement) -> (frontier information)
    indexes: ArrangementFrontiers<Timestamp>,
    since_updates: Vec<(GlobalId, Antichain<Timestamp>)>,
    /// For each connection running a TAIL command, the name of the dataflow
    /// that is servicing the TAIL. A connection can only run one TAIL at a
    /// time.
    active_tails: HashMap<u32, GlobalId>,
    timestamp_config: TimestampConfig,
    /// Delta from leading edge of an arrangement from which we allow compaction.
    logical_compaction_window_ms: Option<Timestamp>,
    /// Instance count: number of times sources have been instantiated in views. This is used
    /// to associate each new instance of a source with a unique instance id (iid)
    logging_granularity: Option<u64>,
    executor: tokio::runtime::Handle,
    feedback_rx: Option<comm::mpsc::Receiver<WorkerFeedbackWithMeta>>,
    // Temporary place to stash Persister thread startup data between when the coordinator thread
    // is initialized and when the persister thread gets spawned.
    persister: Option<Persister>,
    // Channel to communicate source status updates and shutdown notifications to the persister
    // thread.
    persistence_tx: Option<PersistenceSender>,
    /// The last timestamp we assigned to a read.
    read_lower_bound: Timestamp,
    /// The timestamp that all local inputs have been advanced up to.
    closed_up_to: Timestamp,
    /// Whether or not the most recent operation was a read.
    last_op_was_read: bool,
    /// Whether we need to advance local inputs (i.e., did someone observe a timestamp).
    /// TODO(justin): this is a hack, and does not work right with TAIL.
    need_advance: bool,
}

impl<C> Coordinator<C>
where
    C: comm::Connection,
{
    pub fn new(config: Config<C>) -> Result<Self, anyhow::Error> {
        let mut broadcast_tx = config.switchboard.broadcast_tx(dataflow::BroadcastToken);
        config.executor.enter(|| {
            let res = Self::new_core(config);
            if res.is_err() {
                broadcast(&mut broadcast_tx, SequencedCommand::Shutdown);
            }
            res
        })
    }

    fn new_core(config: Config<C>) -> Result<Self, anyhow::Error> {
        let mut broadcast_tx = config.switchboard.broadcast_tx(dataflow::BroadcastToken);

        let symbiosis = if let Some(symbiosis_url) = config.symbiosis_url {
            Some(block_on(symbiosis::Postgres::open_and_erase(
                symbiosis_url,
            ))?)
        } else {
            None
        };

        let catalog_path = config.data_directory.map(|d| d.join("catalog"));
        let catalog = Catalog::open(catalog::Config {
            path: catalog_path.as_deref(),
            experimental_mode: Some(config.experimental_mode),
            enable_logging: config.logging_granularity.is_some(),
        })?;
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
        let (tx, rx) = config.switchboard.mpsc_limited(config.num_timely_workers);
        broadcast(&mut broadcast_tx, SequencedCommand::EnableFeedback(tx));

        if let Some(granularity) = config.logging_granularity {
            broadcast(
                &mut broadcast_tx,
                SequencedCommand::EnableLogging(LoggingConfig {
                    granularity_ns: granularity.as_nanos(),
                    active_logs: BUILTINS
                        .logs()
                        .map(|src| (src.variant.clone(), src.index_id))
                        .collect(),
                }),
            );
        }

        let (persister, persistence_tx) = if let Some(persistence_config) = config.persistence {
            let (persistence_tx, persistence_rx) = config.switchboard.mpsc();
            broadcast(
                &mut broadcast_tx,
                SequencedCommand::EnablePersistence(persistence_tx.clone()),
            );
            (
                Some(Persister::new(persistence_rx, persistence_config)),
                Some(block_on(persistence_tx.connect()).expect("failed to connect persistence tx")),
            )
        } else {
            (None, None)
        };

        let mut coord = Self {
            switchboard: config.switchboard,
            broadcast_tx,
            num_timely_workers: config.num_timely_workers,
            optimizer: Default::default(),
            catalog,
            symbiosis,
            indexes: ArrangementFrontiers::default(),
            since_updates: Vec::new(),
            active_tails: HashMap::new(),
            logging_granularity: config
                .logging_granularity
                .and_then(|c| c.as_millis().try_into().ok()),
            executor: config.executor.clone(),
            timestamp_config: config.timestamp,
            logical_compaction_window_ms,
            feedback_rx: Some(rx),
            persister,
            persistence_tx,
            closed_up_to: 1,
            read_lower_bound: 1,
            last_op_was_read: false,
            need_advance: true,
        };

        let catalog_entries: Vec<_> = coord
            .catalog
            .iter()
            .map(|entry| (entry.id(), entry.name().clone(), entry.item().clone()))
            .collect();

        for (id, name, item) in &catalog_entries {
            match item {
                CatalogItem::Table(_) | CatalogItem::View(_) => (),
                //currently catalog item rebuild assumes that sinks and
                //indexes are always built individually and does not store information
                //about how it was built. If we start building multiple sinks and/or indexes
                //using a single dataflow, we have to make sure the rebuild process re-runs
                //the same multiple-build dataflow.
                CatalogItem::Source(source) => {
                    // See if we have any previously persisted data that we need to reread.
                    // Only do this if persistence is enabled for Materialize.
                    if let Some(persister) = &coord.persister {
                        let connector = crate::persistence::augment_connector(
                            source.connector.clone(),
                            persister.config.path.clone(),
                            *id,
                        )?;

                        // If we got back a new connector, we need to take some
                        // additional action.
                        if let Some(connector) = connector {
                            coord.enable_source_connector_persistence(*id, connector);
                        }
                    }
                }
                CatalogItem::Sink(sink) => {
                    let builder = match &sink.connector {
                        SinkConnectorState::Pending(builder) => builder,
                        SinkConnectorState::Ready(_) => {
                            panic!("sink already initialized during catalog boot")
                        }
                    };
                    let connector = block_on(sink_connector::build(
                        builder.clone(),
                        sink.with_snapshot,
                        coord.determine_frontier(sink.as_of, sink.from)?,
                        *id,
                    ))
                    .with_context(|| format!("recreating sink {}", name))?;
                    coord.handle_sink_connector_ready(*id, connector);
                }
                CatalogItem::Index(_) => {
                    if BUILTINS.logs().any(|log| log.index_id == *id) {
                        // Indexes on logging views are special, as they are
                        // already installed in the dataflow plane via
                        // `SequencedCommand::EnableLogging`. Just teach the
                        // coordinator of their existence, without creating a
                        // dataflow for the index.
                        //
                        // TODO(benesch): why is this hardcoded to 1000?
                        // Should it not be the same logical compaction window
                        // that everything else uses?
                        coord
                            .indexes
                            .insert(*id, Frontiers::new(coord.num_timely_workers, Some(1_000)));
                    } else {
                        coord.ship_dataflow(coord.build_index_dataflow(*id));
                    }
                }
            }
        }

        for (id, name, _item) in catalog_entries {
            // Mirror each recovered catalog entry.
            coord.report_catalog_update(id, name.to_string(), 1);
        }

        // Insert initial named objects into system tables.
        // Databases and named schemas.
        let dbs: Vec<(String, i64, Vec<(String, i64)>)> = coord
            .catalog
            .databases()
            .map(|(name, database)| {
                (
                    name.to_string(),
                    database.id,
                    database
                        .schemas
                        .iter()
                        .map(|(schema_name, schema)| (schema_name.to_string(), schema.id))
                        .collect(),
                )
            })
            .collect();
        for (database_name, database_id, schemas) in dbs {
            coord.update_catalog_view(
                MZ_DATABASES.id,
                iter::once((
                    Row::pack(&[
                        Datum::String(&database_id.to_string()),
                        Datum::String(&database_name),
                    ]),
                    1,
                )),
            );
            for (schema_name, schema_id) in schemas {
                coord.update_catalog_view(
                    MZ_SCHEMAS.id,
                    iter::once((
                        Row::pack(&[
                            Datum::String(&schema_id.to_string()),
                            Datum::String(&database_id.to_string()),
                            Datum::String(&schema_name),
                            Datum::String("USER"),
                        ]),
                        1,
                    )),
                )
            }
        }
        // Ambient schemas.
        let ambient_schemas: Vec<(String, i64)> = coord
            .catalog
            .ambient_schemas()
            .map(|(schema_name, schema)| (schema_name.to_string(), schema.id))
            .collect();
        for (schema_name, schema_id) in ambient_schemas {
            coord.update_catalog_view(
                MZ_SCHEMAS.id,
                iter::once((
                    Row::pack(&[
                        Datum::String(&schema_id.to_string()),
                        Datum::String("AMBIENT"),
                        Datum::String(&schema_name),
                        Datum::String("SYSTEM"),
                    ]),
                    1,
                )),
            )
        }
        // The ambient "mz_temp" schema has a single GlobalId: -1.
        coord.update_catalog_view(
            MZ_SCHEMAS.id,
            iter::once((
                Row::pack(&[
                    Datum::String("-1"),
                    Datum::String("AMBIENT"),
                    Datum::String("mz_temp"),
                    Datum::String("SYSTEM"),
                ]),
                1,
            )),
        );
        // Todo@jldlaughlin: insert rest of named objects!

        // Announce primary and foreign key relationships.
        if coord.logging_granularity.is_some() {
            for log in BUILTINS.logs() {
                let log_id = &log.id.to_string();
                coord.update_catalog_view(
                    MZ_VIEW_KEYS.id,
                    log.variant.desc().typ().keys.iter().enumerate().flat_map(
                        move |(index, key)| {
                            key.iter().map(move |k| {
                                let row = Row::pack(&[
                                    Datum::String(log_id),
                                    Datum::Int64(*k as i64),
                                    Datum::Int64(index as i64),
                                ]);
                                (row, 1)
                            })
                        },
                    ),
                );

                coord.update_catalog_view(
                    MZ_VIEW_FOREIGN_KEYS.id,
                    log.variant.foreign_keys().into_iter().enumerate().flat_map(
                        move |(index, (parent, pairs))| {
                            let parent_id = BUILTINS
                                .logs()
                                .find(|src| src.variant == parent)
                                .unwrap()
                                .id
                                .to_string();
                            pairs.into_iter().map(move |(c, p)| {
                                let row = Row::pack(&[
                                    Datum::String(&log_id),
                                    Datum::Int64(c as i64),
                                    Datum::String(&parent_id),
                                    Datum::Int64(p as i64),
                                    Datum::Int64(index as i64),
                                ]);
                                (row, 1)
                            })
                        },
                    ),
                );
            }
        }
        Ok(coord)
    }

    /// Assign a timestamp for a read.
    fn get_read_ts(&mut self) -> Timestamp {
        let ts = self.get_ts();
        self.last_op_was_read = true;
        self.read_lower_bound = ts;
        ts
    }

    /// Assign a timestamp for a write. Writes following reads must ensure that they are assigned a
    /// strictly larger timestamp to ensure they are not visible to any real-time earlier reads.
    fn get_write_ts(&mut self) -> Timestamp {
        let ts = if self.last_op_was_read {
            self.last_op_was_read = false;
            cmp::max(self.get_ts(), self.read_lower_bound + 1)
        } else {
            self.get_ts()
        };
        self.read_lower_bound = cmp::max(ts, self.closed_up_to);
        self.read_lower_bound
    }

    /// Fetch a new timestamp.
    fn get_ts(&mut self) -> Timestamp {
        // Next time we have a chance, we will force all local inputs forward.
        self.need_advance = true;
        // This is a hack. In a perfect world we would represent time as having a "real" dimension
        // and a "coordinator" dimension so that clients always observed linearizability from
        // things the coordinator did without being related to the real dimension.
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("failed to get millis since epoch")
            .as_millis()
            .try_into()
            .expect("current time did not fit into u64");

        if ts < self.read_lower_bound {
            self.read_lower_bound
        } else {
            ts
        }
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
        let mut timestamper =
            Timestamper::new(&self.timestamp_config, internal_cmd_tx.clone(), ts_rx);
        let executor = self.executor.clone();
        let _timestamper_thread =
            thread::spawn(move || executor.enter(|| timestamper.update())).join_on_drop();

        let persister = self.persister.take();

        if let Some(mut persister) = persister {
            tokio::spawn(async move { persister.run().await });
        }

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
                    let catalog = self.catalog.for_session(&session);
                    if catalog
                        .resolve_database(catalog.default_database())
                        .is_err()
                    {
                        messages.push(StartupMessage::UnknownSessionDatabase);
                    }
                    self.catalog.create_temporary_schema(session.conn_id());
                    ClientTransmitter::new(tx).send(Ok(messages), session)
                }
                Message::Command(Command::Execute {
                    portal_name,
                    session,
                    tx,
                }) => {
                    let result = session
                        .get_portal(&portal_name)
                        .ok_or_else(|| anyhow::format_err!("portal does not exist {:?}", portal_name))
                        .and_then(|portal| {
                            session
                                .get_prepared_statement(&portal.statement_name)
                                .ok_or_else(|| anyhow::format_err!(
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
                                let result = sql::pure::purify(stmt).await;
                                internal_cmd_tx
                                    .send(Message::StatementReady {
                                        session,
                                        tx: ClientTransmitter::new(tx),
                                        result,
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
                    session,
                    tx,
                    result,
                    params,
                } => match result.and_then(|stmt| self.handle_statement(&session, stmt, &params)) {
                    Ok((pcx, plan)) => self.sequence_plan(&internal_cmd_tx, tx, session, pcx, plan),
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

                Message::Command(Command::Describe {
                    name,
                    stmt,
                    param_types,
                    mut session,
                    tx,
                }) => {
                    let result = self.handle_describe(&mut session, name, stmt, param_types);
                    let _ = tx.send(Response { result, session });
                }

                Message::Command(Command::CancelRequest { conn_id }) => {
                    self.handle_cancel(conn_id);
                }

                Message::Command(Command::DumpCatalog { tx }) => {
                    let _ = tx.send(self.catalog.dump());
                }

                Message::Command(Command::Terminate { conn_id }) => {
                    self.handle_terminate(conn_id);
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
                    message: WorkerFeedback::CreateSource(src_instance_id),
                }) => {
                    if let Some(entry) = self.catalog.try_get_by_id(src_instance_id.source_id) {
                        if let CatalogItem::Source(s) = entry.item() {
                            ts_tx
                                .send(TimestampMessage::Add(src_instance_id, s.connector.clone()))
                                .expect("Failed to send CREATE Instance notice to timestamper");
                        } else {
                            panic!("A non-source is re-using the same source ID");
                        }
                    } else {
                        // Someone already dropped the source
                    }
                }

                Message::AdvanceSourceTimestamp { id, update } => {
                    broadcast(
                        &mut self.broadcast_tx,
                        SequencedCommand::AdvanceSourceTimestamp { id, update },
                    );
                }
                Message::Shutdown => {
                    ts_tx.send(TimestampMessage::Shutdown).unwrap();

                    if let Some(persistence_tx) = &mut self.persistence_tx {
                        block_on(persistence_tx.send(PersistenceMessage::Shutdown))
                            .expect("failed to send shutdown message to persistence thread");
                    }
                    broadcast(&mut self.broadcast_tx, SequencedCommand::Shutdown);
                    break;
                }
            }

            let needed = self.need_advance;
            let mut next_ts = self.get_ts();
            self.need_advance = false;
            if next_ts <= self.read_lower_bound {
                next_ts = self.read_lower_bound + 1;
            }
            // TODO(justin): this is pretty hacky, and works more-or-less because this frequency
            // lines up with that used in the logging views.
            if needed
                || self.logging_granularity.is_some()
                    && next_ts / self.logging_granularity.unwrap()
                        > self.closed_up_to / self.logging_granularity.unwrap()
            {
                if next_ts > self.closed_up_to {
                    broadcast(
                        &mut self.broadcast_tx,
                        SequencedCommand::AdvanceAllLocalInputs {
                            advance_to: next_ts,
                        },
                    );
                    self.closed_up_to = next_ts;
                }
            }
        }

        // Cleanly drain any pending messages from the worker before shutting
        // down.
        drop(internal_cmd_tx);
        while block_on(messages.next()).is_some() {}
    }

    /// Updates the upper frontier of a named view.
    fn update_upper(&mut self, name: &GlobalId, mut changes: ChangeBatch<Timestamp>) {
        if let Some(index_state) = self.indexes.get_mut(name) {
            let changes: Vec<_> = index_state.upper.update_iter(changes.drain()).collect();
            if !changes.is_empty() {
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
                        let mut compaction_frontier = Antichain::new();
                        for time in index_state.upper.frontier().iter() {
                            compaction_frontier.insert(
                                compaction_latency_ms
                                    * (time.saturating_sub(compaction_latency_ms)
                                        / compaction_latency_ms),
                            );
                        }
                        if index_state.since != compaction_frontier {
                            index_state.advance_since(&compaction_frontier);
                            self.since_updates
                                .push((name.clone(), index_state.since.clone()));
                        }
                    }
                }
            }
        }
    }

    /// Perform maintenance work associated with the coordinator.
    ///
    /// Primarily, this involves sequencing compaction commands, which should be
    /// issued whenever available.
    fn maintenance(&mut self) {
        // Take this opportunity to drain `since_update` commands.
        // Don't try to compact to an empty frontier. There may be a good reason to do this
        // in principle, but not in any current Mz use case.
        // (For background, see: https://github.com/MaterializeInc/materialize/pull/1113#issuecomment-559281990)
        self.since_updates
            .retain(|(_, frontier)| frontier != &Antichain::new());
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

    fn handle_statement(
        &mut self,
        session: &Session,
        stmt: sql::ast::Statement,
        params: &sql::plan::Params,
    ) -> Result<(PlanContext, sql::plan::Plan), anyhow::Error> {
        let pcx = PlanContext::default();

        // When symbiosis mode is enabled, use symbiosis planning for:
        //  - CREATE TABLE
        //  - DROP TABLE
        //  - INSERT
        // When these statements are routed through symbiosis, table information
        // is created and maintained locally, which is required for other statements
        // to be executed correctly.
        if let Statement::CreateTable(CreateTableStatement { .. })
        | Statement::DropObjects(DropObjectsStatement {
            object_type: ObjectType::Table,
            ..
        })
        | Statement::Insert { .. } = &stmt
        {
            if let Some(ref mut postgres) = self.symbiosis {
                let plan =
                    block_on(postgres.execute(&pcx, &self.catalog.for_session(session), &stmt))?;
                return Ok((pcx, plan));
            }
        }

        match sql::plan::plan(
            &pcx,
            &self.catalog.for_session(session),
            stmt.clone(),
            params,
        ) {
            Ok(plan) => Ok((pcx, plan)),
            Err(err) => match self.symbiosis {
                Some(ref mut postgres) if postgres.can_handle(&stmt) => {
                    let plan = block_on(postgres.execute(
                        &pcx,
                        &self.catalog.for_session(session),
                        &stmt,
                    ))?;
                    Ok((pcx, plan))
                }
                _ => Err(err),
            },
        }
    }

    fn handle_describe(
        &self,
        session: &mut Session,
        name: String,
        stmt: Option<Statement>,
        param_types: Vec<Option<pgrepr::Type>>,
    ) -> Result<(), anyhow::Error> {
        let (desc, param_types) = if let Some(stmt) = stmt.clone() {
            match sql::plan::describe(
                &self.catalog.for_session(session),
                stmt.clone(),
                &param_types,
            ) {
                Ok((desc, param_types)) => (desc, param_types),
                // Describing the query failed. If we're running in symbiosis with
                // Postgres, see if Postgres can handle it. Note that Postgres
                // only handles commands that do not return rows, so the
                // `RelationDesc` is always `None`.
                Err(err) => match self.symbiosis {
                    Some(ref postgres) if postgres.can_handle(&stmt) => (None, vec![]),
                    _ => return Err(err),
                },
            }
        } else {
            (None, vec![])
        };
        session.set_prepared_statement(name, PreparedStatement::new(stmt, desc, param_types));
        Ok(())
    }

    /// Instruct the dataflow layer to cancel any ongoing, interactive work for
    /// the named `conn_id`. This means canceling the active PEEK or TAIL, if
    /// one exists.
    ///
    /// NOTE(benesch): this function makes the assumption that a connection can
    /// only have one active query at a time. This is true today, but will not
    /// be true once we have full support for portals.
    fn handle_cancel(&mut self, conn_id: u32) {
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

    /// Terminate any temporary objects created by the named `conn_id`
    /// stored on the Coordinator.
    fn handle_terminate(&mut self, conn_id: u32) {
        if let Some(name) = self.active_tails.remove(&conn_id) {
            self.drop_sinks(vec![name]);
        }

        // Remove all temporary items created by the conn_id.
        let ops = self.catalog.drop_temp_item_ops(conn_id);
        self.catalog_transact(ops)
            .expect("unable to drop temporary items for conn_id");
        self.catalog
            .drop_temporary_schema(conn_id)
            .expect("unable to drop temporary schema");
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

        self.ship_dataflow(self.build_sink_dataflow(name.to_string(), id, sink.from, connector))
    }

    fn report_catalog_update(&mut self, id: GlobalId, name: String, diff: isize) {
        let row = Row::pack(&[Datum::String(&id.to_string()), Datum::String(&name)]);
        self.update_catalog_view(MZ_CATALOG_NAMES.id, iter::once((row, diff)));
    }

    /// Insert a single row into a given catalog view.
    fn update_catalog_view<I>(&mut self, index_id: GlobalId, updates: I)
    where
        I: IntoIterator<Item = (Row, isize)>,
    {
        let timestamp = self.get_write_ts();
        let updates = updates
            .into_iter()
            .map(|(row, diff)| Update {
                row,
                diff,
                timestamp,
            })
            .collect();
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::Insert {
                id: index_id,
                updates,
            },
        );
    }

    fn sequence_plan(
        &mut self,
        internal_cmd_tx: &futures::channel::mpsc::UnboundedSender<Message>,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        pcx: PlanContext,
        plan: Plan,
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
                table,
                if_not_exists,
            } => tx.send(
                self.sequence_create_table(pcx, name, table, if_not_exists),
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
                with_snapshot,
                as_of,
                if_not_exists,
            } => self.sequence_create_sink(
                pcx,
                internal_cmd_tx.clone(),
                tx,
                session,
                name,
                sink,
                with_snapshot,
                as_of,
                if_not_exists,
            ),

            Plan::CreateView {
                name,
                view,
                replace,
                materialize,
                if_not_exists,
            } => tx.send(
                self.sequence_create_view(
                    pcx,
                    name,
                    view,
                    replace,
                    session.conn_id(),
                    materialize,
                    if_not_exists,
                ),
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
                self.sequence_peek(session.conn_id(), source, when, finishing, materialize),
                session,
            ),

            Plan::Tail {
                id,
                ts,
                with_snapshot,
            } => tx.send(
                self.sequence_tail(session.conn_id(), id, with_snapshot, ts),
                session,
            ),

            Plan::SendRows(rows) => tx.send(Ok(send_immediate_rows(rows)), session),

            Plan::ExplainPlan {
                raw_plan,
                decorrelated_plan,
                row_set_finishing,
                stage,
                options,
            } => tx.send(
                self.sequence_explain_plan(
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

            Plan::Insert { id, values } => tx.send(self.sequence_insert(id, values), session),

            Plan::AlterItemRename {
                id,
                to_name,
                object_type,
            } => tx.send(
                self.sequence_alter_item_rename(id, to_name, object_type),
                session,
            ),
        }
    }

    fn sequence_create_database(
        &mut self,
        name: String,
        if_not_exists: bool,
    ) -> Result<ExecuteResponse, anyhow::Error> {
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
    ) -> Result<ExecuteResponse, anyhow::Error> {
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
        table: sql::plan::Table,
        if_not_exists: bool,
    ) -> Result<ExecuteResponse, anyhow::Error> {
        let table_id = self.catalog.allocate_id()?;
        let table = catalog::Table {
            create_sql: table.create_sql,
            plan_cx: pcx,
            desc: table.desc,
        };
        let index_id = self.catalog.allocate_id()?;
        let mut index_name = name.clone();
        index_name.item += "_primary_idx";
        let index =
            auto_generate_primary_idx(index_name.item.clone(), name.clone(), table_id, &table.desc);
        match self.catalog_transact(vec![
            catalog::Op::CreateItem {
                id: table_id,
                name,
                item: CatalogItem::Table(table),
            },
            catalog::Op::CreateItem {
                id: index_id,
                name: index_name,
                item: CatalogItem::Index(index),
            },
        ]) {
            Ok(_) => {
                self.ship_dataflow(self.build_index_dataflow(index_id));
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
        source: sql::plan::Source,
        if_not_exists: bool,
        materialized: bool,
    ) -> Result<ExecuteResponse, anyhow::Error> {
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
        let index_id = if materialized {
            let mut index_name = name.clone();
            index_name.item += "_primary_idx";
            let index =
                auto_generate_primary_idx(index_name.item.clone(), name, source_id, &source.desc);
            let index_id = self.catalog.allocate_id()?;
            ops.push(catalog::Op::CreateItem {
                id: index_id,
                name: index_name,
                item: CatalogItem::Index(index),
            });
            Some(index_id)
        } else {
            None
        };
        match self.catalog_transact(ops) {
            Ok(()) => {
                if let Some(index_id) = index_id {
                    self.ship_dataflow(self.build_index_dataflow(index_id));
                }

                self.enable_source_connector_persistence(source_id, source.connector);
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
        sink: sql::plan::Sink,
        with_snapshot: bool,
        as_of: Option<u64>,
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

        let frontier = match self.determine_frontier(as_of, sink.from) {
            Ok(frontier) => frontier,
            Err(e) => {
                tx.send(Err(e), session);
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
                with_snapshot,
                as_of,
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
                    result: sink_connector::build(connector_builder, with_snapshot, frontier, id)
                        .await,
                })
                .await
                .expect("sending to internal_cmd_tx cannot fail");
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn sequence_create_view(
        &mut self,
        pcx: PlanContext,
        name: FullName,
        view: sql::plan::View,
        replace: Option<GlobalId>,
        conn_id: u32,
        materialize: bool,
        if_not_exists: bool,
    ) -> Result<ExecuteResponse, anyhow::Error> {
        let mut ops = vec![];
        if let Some(id) = replace {
            ops.extend(self.catalog.drop_items_ops(&[id]));
        }
        let view_id = self.catalog.allocate_id()?;
        // Optimize the expression so that we can form an accurately typed description.
        let optimized_expr = self.optimizer.optimize(view.expr, self.catalog.indexes())?;
        let desc = RelationDesc::new(optimized_expr.as_ref().typ(), view.column_names);
        let view = catalog::View {
            create_sql: view.create_sql,
            plan_cx: pcx,
            optimized_expr,
            desc,
            conn_id: if view.temporary { Some(conn_id) } else { None },
        };
        ops.push(catalog::Op::CreateItem {
            id: view_id,
            name: name.clone(),
            item: CatalogItem::View(view.clone()),
        });
        let index_id = if materialize {
            let mut index_name = name.clone();
            index_name.item += "_primary_idx";
            let index =
                auto_generate_primary_idx(index_name.item.clone(), name, view_id, &view.desc);

            let index_id = self.catalog.allocate_id()?;
            ops.push(catalog::Op::CreateItem {
                id: index_id,
                name: index_name,
                item: CatalogItem::Index(index),
            });
            Some(index_id)
        } else {
            None
        };
        match self.catalog_transact(ops) {
            Ok(()) => {
                if let Some(index_id) = index_id {
                    self.ship_dataflow(self.build_index_dataflow(index_id));
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
        index: sql::plan::Index,
        if_not_exists: bool,
    ) -> Result<ExecuteResponse, anyhow::Error> {
        let index = catalog::Index {
            create_sql: index.create_sql,
            plan_cx: pcx,
            keys: index.keys,
            on: index.on,
        };
        let id = self.catalog.allocate_id()?;
        let op = catalog::Op::CreateItem {
            id,
            name,
            item: CatalogItem::Index(index),
        };
        match self.catalog_transact(vec![op]) {
            Ok(()) => {
                self.ship_dataflow(self.build_index_dataflow(id));
                Ok(ExecuteResponse::CreatedIndex { existed: false })
            }
            Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedIndex { existed: true }),
            Err(err) => Err(err),
        }
    }

    fn sequence_drop_database(&mut self, name: String) -> Result<ExecuteResponse, anyhow::Error> {
        let ops = self.catalog.drop_database_ops(name);
        self.catalog_transact(ops)?;
        Ok(ExecuteResponse::DroppedDatabase)
    }

    fn sequence_drop_schema(
        &mut self,
        database_name: DatabaseSpecifier,
        schema_name: String,
    ) -> Result<ExecuteResponse, anyhow::Error> {
        let ops = self.catalog.drop_schema_ops(database_name, schema_name);
        self.catalog_transact(ops)?;
        Ok(ExecuteResponse::DroppedSchema)
    }

    fn sequence_drop_items(
        &mut self,
        items: Vec<GlobalId>,
        ty: ObjectType,
    ) -> Result<ExecuteResponse, anyhow::Error> {
        let ops = self.catalog.drop_items_ops(&items);
        self.catalog_transact(ops)?;
        Ok(match ty {
            ObjectType::Schema => unreachable!(),
            ObjectType::Source => {
                for id in items.iter() {
                    if let Some(persistence_tx) = &mut self.persistence_tx {
                        block_on(persistence_tx.send(PersistenceMessage::DropSource(*id)))
                            .expect("failed to send DROP SOURCE to persistence thread");
                    }
                }
                ExecuteResponse::DroppedSource
            }
            ObjectType::View => ExecuteResponse::DroppedView,
            ObjectType::Table => ExecuteResponse::DroppedTable,
            ObjectType::Sink => ExecuteResponse::DroppedSink,
            ObjectType::Index => ExecuteResponse::DroppedIndex,
        })
    }

    fn sequence_show_all_variables(
        &mut self,
        session: &Session,
    ) -> Result<ExecuteResponse, anyhow::Error> {
        let mut row_packer = RowPacker::new();
        Ok(send_immediate_rows(
            session
                .vars()
                .iter()
                .map(|v| {
                    row_packer.pack(&[
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
    ) -> Result<ExecuteResponse, anyhow::Error> {
        let variable = session.get(&name)?;
        let row = Row::pack(&[Datum::String(&variable.value())]);
        Ok(send_immediate_rows(vec![row]))
    }

    fn sequence_set_variable(
        &self,
        session: &mut Session,
        name: String,
        value: String,
    ) -> Result<ExecuteResponse, anyhow::Error> {
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
    ) -> Result<ExecuteResponse, anyhow::Error> {
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
                if let Some(index_id) = self.catalog.default_index_for(*id) {
                    (true, index_id)
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

            if !fast_path {
                // Slow path. We need to perform some computation, so build
                // a new transient dataflow that will be dropped after the
                // peek completes.
                let typ = source.as_ref().typ();
                let key: Vec<_> = (0..typ.arity()).map(ScalarExpr::Column).collect();
                let view_id = self.catalog.allocate_id()?;
                let mut dataflow = DataflowDesc::new(format!("temp-view-{}", view_id));
                dataflow.set_as_of(Antichain::from_elem(timestamp));
                self.import_view_into_dataflow(&view_id, &source, &mut dataflow);
                dataflow.add_index_to_build(index_id, view_id, typ.clone(), key.clone());
                dataflow.add_index_export(index_id, view_id, typ, key);
                self.ship_dataflow(dataflow);
            }

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
                self.drop_indexes(vec![index_id]);
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
        with_snapshot: bool,
        ts: Option<Timestamp>,
    ) -> Result<ExecuteResponse, anyhow::Error> {
        // Determine the frontier of updates to tail *from*.
        // Updates greater or equal to this frontier will be produced.
        let frontier = self.determine_frontier(ts, source_id)?;
        let sink_name = format!(
            "tail-source-{}",
            self.catalog
                .humanize_id(Id::Global(source_id))
                .expect("Source id is known to exist in catalog")
        );
        let sink_id = self.catalog.allocate_id()?;
        self.active_tails.insert(conn_id, sink_id);
        let (tx, rx) = self.switchboard.mpsc_limited(self.num_timely_workers);

        self.ship_dataflow(self.build_sink_dataflow(
            sink_name,
            sink_id,
            source_id,
            SinkConnector::Tail(TailSinkConnector {
                tx,
                frontier,
                strict: !with_snapshot,
            }),
        ));
        Ok(ExecuteResponse::Tailing { rx })
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
    ) -> Result<Timestamp, anyhow::Error> {
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
        let uses_ids = &source.global_uses();
        let (index_ids, indexes_complete) = self.catalog.nearest_indexes(&uses_ids);

        // First determine the candidate timestamp, which is either the explicitly requested
        // timestamp, or the latest timestamp known to be immediately available.
        let timestamp = match when {
            // Explicitly requested timestamps should be respected.
            PeekWhen::AtTimestamp(timestamp) => timestamp,

            // These two strategies vary in terms of which traces drive the
            // timestamp determination process: either the trace itself or the
            // original sources on which they depend.
            PeekWhen::Immediately => {
                if !indexes_complete {
                    bail!("Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources");
                }
                if uses_ids.iter().any(|id| self.catalog.uses_tables(*id)) {
                    // If the view depends on any tables, we enforce
                    // linearizability by choosing the latest input time.
                    self.get_read_ts()
                } else {
                    let upper = self.indexes.greatest_open_upper(index_ids.iter().cloned());
                    // We peek at the largest element not in advance of `upper`, which
                    // involves a subtraction. If `upper` contains a zero timestamp there
                    // is no "prior" answer, and we do not want to peek at it as it risks
                    // hanging awaiting the response to data that may never arrive.
                    if let Some(candidate) = upper.elements().get(0) {
                        if *candidate > 0 {
                            candidate.saturating_sub(1)
                        } else {
                            let unstarted = index_ids
                                .iter()
                                .filter(|id| {
                                    self.indexes
                                        .upper_of(id)
                                        .expect("id not found")
                                        .less_equal(&0)
                                })
                                .collect::<Vec<_>>();
                            bail!(
                                "At least one input has no complete timestamps yet: {:?}",
                                unstarted
                            );
                        }
                    } else {
                        // A complete trace can be read in its final form with this time.
                        //
                        // This should only happen for literals that have no sources
                        Timestamp::max_value()
                    }
                }
            }
        };

        // Determine the valid lower bound of times that can produce correct outputs.
        // This bound is determined by the arrangements contributing to the query,
        // and does not depend on the transitive sources.
        let since = self.indexes.least_valid_since(index_ids.iter().cloned());

        // If the timestamp is greater or equal to some element in `since` we are
        // assured that the answer will be correct.
        if since.less_equal(&timestamp) {
            Ok(timestamp)
        } else {
            let invalid = index_ids
                .iter()
                .filter(|id| {
                    !self
                        .indexes
                        .since_of(id)
                        .expect("id not found")
                        .less_equal(&timestamp)
                })
                .map(|id| (id, self.indexes.since_of(id)))
                .collect::<Vec<_>>();
            bail!(
                "Timestamp ({}) is not valid for all inputs: {:?}",
                timestamp,
                invalid
            );
        }
    }

    /// Determine the frontier of updates to start *from*.
    /// Updates greater or equal to this frontier will be produced.
    fn determine_frontier(
        &mut self,
        as_of: Option<u64>,
        source_id: GlobalId,
    ) -> Result<Antichain<u64>, anyhow::Error> {
        let frontier = if let Some(ts) = as_of {
            // If a timestamp was explicitly requested, use that.
            Antichain::from_elem(self.determine_timestamp(
                &RelationExpr::Get {
                    id: Id::Global(source_id),
                    // TODO(justin): find a way to avoid synthesizing an arbitrary relation type.
                    typ: RelationType::empty(),
                },
                PeekWhen::AtTimestamp(ts),
            )?)
        }
        // TODO: The logic that follows is at variance from PEEK logic which consults the
        // "queryable" state of its inputs. We might want those to line up, but it is only
        // a "might".
        else if let Some(index_id) = self.catalog.default_index_for(source_id) {
            let upper = self
                .indexes
                .upper_of(&index_id)
                .expect("name missing at coordinator");

            if let Some(ts) = upper.get(0) {
                Antichain::from_elem(ts.saturating_sub(1))
            } else {
                Antichain::from_elem(Timestamp::max_value())
            }
        } else {
            // TODO: This should more carefully consider `since` frontiers of its input.
            // This will be forcibly corrected if any inputs are compacted.
            Antichain::from_elem(0)
        };
        Ok(frontier)
    }

    fn sequence_explain_plan(
        &mut self,
        raw_plan: sql::plan::RelationExpr,
        decorrelated_plan: expr::RelationExpr,
        row_set_finishing: Option<RowSetFinishing>,
        stage: ExplainStage,
        options: ExplainOptions,
    ) -> Result<ExecuteResponse, anyhow::Error> {
        let explanation_string = match stage {
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
                let mut explanation = decorrelated_plan.explain(&self.catalog);
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
                    .optimize(decorrelated_plan, self.catalog.indexes())?
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
    ) -> Result<ExecuteResponse, anyhow::Error> {
        let timestamp = self.get_write_ts();
        let updates = updates
            .into_iter()
            .map(|(row, diff)| Update {
                row,
                diff,
                timestamp,
            })
            .collect();

        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::Insert { id, updates },
        );

        Ok(match kind {
            MutationKind::Delete => ExecuteResponse::Deleted(affected_rows),
            MutationKind::Insert => ExecuteResponse::Inserted(affected_rows),
            MutationKind::Update => ExecuteResponse::Updated(affected_rows),
        })
    }

    fn sequence_insert(
        &mut self,
        id: GlobalId,
        values: expr::RelationExpr,
    ) -> Result<ExecuteResponse, anyhow::Error> {
        match self
            .optimizer
            .optimize(values, self.catalog.indexes())?
            .into_inner()
        {
            RelationExpr::Constant { rows, typ: _ } => {
                let desc = self.catalog.get_by_id(&id).desc()?;
                for (row, _) in &rows {
                    for (datum, (name, typ)) in row.unpack().iter().zip(desc.iter()) {
                        if datum == &Datum::Null && !typ.nullable {
                            bail!(
                                "NULL value in column {} violates not-null constraint",
                                name.unwrap_or(&ColumnName::from("unnamed column"))
                            )
                        }
                    }
                }

                let affected_rows = rows.len();
                self.sequence_send_diffs(id, rows, affected_rows, MutationKind::Insert)
            }
            other => bail!("INSERT statement expected values, found {:?}", other),
        }
    }

    fn sequence_alter_item_rename(
        &mut self,
        id: Option<GlobalId>,
        to_name: String,
        object_type: ObjectType,
    ) -> Result<ExecuteResponse, anyhow::Error> {
        let id = match id {
            Some(id) => id,
            // None is generated by `IF EXISTS`
            None => return Ok(ExecuteResponse::AlteredObject(object_type)),
        };
        let op = catalog::Op::RenameItem { id, to_name };
        match self.catalog_transact(vec![op]) {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(object_type)),
            Err(err) => Err(err),
        }
    }

    fn catalog_transact(&mut self, ops: Vec<catalog::Op>) -> Result<(), anyhow::Error> {
        let mut sources_to_drop = vec![];
        let mut sinks_to_drop = vec![];
        let mut indexes_to_drop = vec![];

        let statuses = self.catalog.transact(ops)?;
        for status in &statuses {
            match status {
                catalog::OpStatus::CreatedDatabase { name, id } => {
                    self.update_catalog_view(
                        MZ_DATABASES.id,
                        iter::once((
                            Row::pack(&[Datum::String(&id.to_string()), Datum::String(name)]),
                            1,
                        )),
                    );
                }
                catalog::OpStatus::CreatedSchema {
                    database_id,
                    schema_id,
                    schema_name,
                } => {
                    self.update_catalog_view(
                        MZ_SCHEMAS.id,
                        iter::once((
                            Row::pack(&[
                                Datum::String(&schema_id.to_string()),
                                Datum::String(&database_id.to_string()),
                                Datum::String(schema_name),
                                Datum::String("USER"),
                            ]),
                            1,
                        )),
                    );
                }
                catalog::OpStatus::CreatedItem(id) => {
                    let name = self.catalog.humanize_id(expr::Id::Global(*id)).unwrap();
                    self.report_catalog_update(*id, name, 1);
                }
                catalog::OpStatus::DroppedDatabase { name, id } => {
                    self.update_catalog_view(
                        MZ_DATABASES.id,
                        iter::once((
                            Row::pack(&[Datum::String(&id.to_string()), Datum::String(name)]),
                            -1,
                        )),
                    );
                }
                catalog::OpStatus::DroppedSchema {
                    database_id,
                    schema_id,
                    schema_name,
                } => {
                    self.update_catalog_view(
                        MZ_SCHEMAS.id,
                        iter::once((
                            Row::pack(&[
                                Datum::String(&schema_id.to_string()),
                                Datum::String(&database_id.to_string()),
                                Datum::String(schema_name),
                                Datum::String("USER"),
                            ]),
                            -1,
                        )),
                    );
                }
                catalog::OpStatus::DroppedItem(entry) => {
                    self.report_catalog_update(entry.id(), entry.name().to_string(), -1);
                    match entry.item() {
                        CatalogItem::Table(_) | CatalogItem::Source(_) => {
                            sources_to_drop.push(entry.id());
                        }
                        CatalogItem::View(_) => (),
                        CatalogItem::Sink(catalog::Sink {
                            connector: SinkConnectorState::Ready(connector),
                            ..
                        }) => {
                            sinks_to_drop.push(entry.id());
                            match connector {
                                SinkConnector::Kafka(KafkaSinkConnector { topic, .. }) => {
                                    let row = Row::pack(&[
                                        Datum::String(entry.id().to_string().as_str()),
                                        Datum::String(topic.as_str()),
                                    ]);
                                    self.update_catalog_view(
                                        MZ_KAFKA_SINKS.id,
                                        iter::once((row, -1)),
                                    );
                                }
                                SinkConnector::AvroOcf(AvroOcfSinkConnector { path, .. }) => {
                                    let row = Row::pack(&[
                                        Datum::String(entry.id().to_string().as_str()),
                                        Datum::Bytes(&path.clone().into_os_string().into_vec()),
                                    ]);
                                    self.update_catalog_view(
                                        MZ_AVRO_OCF_SINKS.id,
                                        iter::once((row, -1)),
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
                        CatalogItem::Index(_) => indexes_to_drop.push(entry.id()),
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

    fn drop_sinks(&mut self, dataflow_names: Vec<GlobalId>) {
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::DropSinks(dataflow_names),
        )
    }

    fn drop_indexes(&mut self, indexes: Vec<GlobalId>) {
        let mut trace_keys = Vec::new();
        for id in indexes {
            if self.indexes.remove(&id).is_some() {
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

    /// Imports the view, source, or table with `id` into the provided
    /// dataflow description.
    fn import_into_dataflow(&self, id: &GlobalId, dataflow: &mut DataflowDesc) {
        if dataflow.objects_to_build.iter().any(|bd| &bd.id == id)
            || dataflow.source_imports.iter().any(|(i, _)| i == id)
        {
            return;
        }
        // A valid index is any index on `id` that is known to the dataflow
        // layer, as indicated by its presence in `self.indexes`.
        let valid_index = self.catalog.indexes()[id]
            .iter()
            .find(|(id, _keys)| self.indexes.contains_key(*id));
        if let Some((index_id, keys)) = valid_index {
            let index_desc = IndexDesc {
                on_id: *id,
                keys: keys.to_vec(),
            };
            let desc = self
                .catalog
                .get_by_id(id)
                .desc()
                .expect("indexes can only be built on items with descs");
            dataflow.add_index_import(*index_id, index_desc, desc.typ().clone(), *id);
        } else {
            match self.catalog.get_by_id(id).item() {
                CatalogItem::Table(table) => {
                    dataflow.add_source_import(*id, SourceConnector::Local, table.desc.clone());
                }
                CatalogItem::Source(source) => {
                    dataflow.add_source_import(*id, source.connector.clone(), source.desc.clone());
                }
                CatalogItem::View(view) => {
                    self.import_view_into_dataflow(id, &view.optimized_expr, dataflow);
                }
                _ => unreachable!(),
            }
        }
    }

    /// Imports the view with the specified ID and expression into the provided
    /// dataflow description.
    fn import_view_into_dataflow(
        &self,
        view_id: &GlobalId,
        view: &OptimizedRelationExpr,
        dataflow: &mut DataflowDesc,
    ) {
        // TODO: We only need to import Get arguments for which we cannot find arrangements.
        view.as_ref().visit(&mut |e| {
            if let RelationExpr::Get {
                id: Id::Global(id),
                typ: _,
            } = e
            {
                self.import_into_dataflow(&id, dataflow);
                dataflow.add_dependency(*view_id, *id)
            }
        });
        // Collect sources, views, and indexes used.
        view.as_ref().visit(&mut |e| {
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
                        let indexes = &self.catalog.indexes()[on_id];
                        if let Some((id, _)) = indexes.iter().find(|(_id, keys)| keys == key_set) {
                            dataflow.add_index_import(*id, index_desc, typ.clone(), *view_id);
                        }
                    }
                }
            }
        });
        dataflow.add_view_to_build(*view_id, view.clone(), view.as_ref().typ());
    }

    /// Builds a dataflow description for the index with the specified ID.
    fn build_index_dataflow(&self, id: GlobalId) -> DataflowDesc {
        let index_entry = self.catalog.get_by_id(&id);
        let index = match index_entry.item() {
            CatalogItem::Index(index) => index,
            _ => unreachable!("cannot create index dataflow on non-index"),
        };
        let on_entry = self.catalog.get_by_id(&index.on);
        let on_type = on_entry.desc().unwrap().typ().clone();
        let mut dataflow = DataflowDesc::new(index_entry.name().to_string());
        self.import_into_dataflow(&index.on, &mut dataflow);
        dataflow.add_index_to_build(id, index.on.clone(), on_type.clone(), index.keys.clone());
        dataflow.add_index_export(id, index.on, on_type, index.keys.clone());
        dataflow
    }

    /// Builds a dataflow description for the sink with the specified name,
    /// ID, source, and output connector.
    fn build_sink_dataflow(
        &self,
        name: String,
        id: GlobalId,
        from: GlobalId,
        connector: SinkConnector,
    ) -> DataflowDesc {
        let mut dataflow = DataflowDesc::new(name);
        dataflow.set_as_of(connector.get_frontier());
        self.import_into_dataflow(&from, &mut dataflow);
        let from_type = self.catalog.get_by_id(&from).desc().unwrap().clone();
        dataflow.add_sink_export(id, from, from_type, connector);
        dataflow
    }

    /// Finalizes a dataflow and then broadcasts it to all workers.
    ///
    /// Finalization includes optimization, but also validation of various
    /// invariants such as ensuring that the `as_of` frontier is in advance of
    /// the various `since` frontiers of participating data inputs.
    ///
    /// In particular, there are requirement on the `as_of` field for the dataflow
    /// and the `since` frontiers of created arrangements, as a function of the `since`
    /// frontiers of dataflow inputs (sources and imported arrangements).
    fn ship_dataflow(&mut self, mut dataflow: DataflowDesc) {
        // The identity for `join` is the minimum element.
        let mut since = Antichain::from_elem(Timestamp::minimum());

        // TODO: Populate "valid from" information for each source.
        // For each source, ... do nothing because we don't track `since` for sources.
        // for (instance_id, _description) in dataflow.source_imports.iter() {
        //     // TODO: Extract `since` information about each source and apply here. E.g.
        //     since.join_assign(&self.source_info[instance_id].since);
        // }

        // For each imported arrangement, lower bound `since` by its own frontier.
        for (global_id, (_description, _typ)) in dataflow.index_imports.iter() {
            since.join_assign(
                self.indexes
                    .since_of(global_id)
                    .expect("global id missing at coordinator"),
            );
        }

        // For each produced arrangement, start tracking the arrangement with
        // a compaction frontier of at least `since`.
        for (global_id, _description, _typ) in dataflow.index_exports.iter() {
            let mut frontiers =
                Frontiers::new(self.num_timely_workers, self.logical_compaction_window_ms);
            frontiers.advance_since(&since);
            self.indexes.insert(*global_id, frontiers);
        }

        for (id, sink) in &dataflow.sink_exports {
            match &sink.connector {
                SinkConnector::Kafka(KafkaSinkConnector { topic, .. }) => {
                    let row = Row::pack(&[
                        Datum::String(&id.to_string()),
                        Datum::String(topic.as_str()),
                    ]);
                    self.update_catalog_view(MZ_KAFKA_SINKS.id, iter::once((row, 1)));
                }
                SinkConnector::AvroOcf(AvroOcfSinkConnector { path, .. }) => {
                    let row = Row::pack(&[
                        Datum::String(&id.to_string()),
                        Datum::Bytes(&path.clone().into_os_string().into_vec()),
                    ]);
                    self.update_catalog_view(MZ_AVRO_OCF_SINKS.id, iter::once((row, 1)));
                }
                _ => (),
            }
        }

        // TODO: Produce "valid from" information for each sink.
        // For each sink, ... do nothing because we don't yield `since` for sinks.
        // for (global_id, _description) in dataflow.sink_exports.iter() {
        //     // TODO: assign `since` to a "valid from" element of the sink. E.g.
        //     self.sink_info[global_id].valid_from(&since);
        // }

        // Ensure that the dataflow's `as_of` is at least `since`.
        if let Some(as_of) = &mut dataflow.as_of {
            // If we have requested a specific time that is invalid .. someone errored.
            use timely::order::PartialOrder;
            if !(<_ as PartialOrder>::less_equal(&since, as_of)) {
                // This can occur in SINK and TAIL at the moment. Their behaviors are fluid enough
                // that we just correct to avoid producing incorrect output updates, but we should
                // fix the root of the problem in a more principled manner.
                log::error!(
                    "Dataflow {} requested as_of ({:?}) not >= since ({:?}); correcting",
                    dataflow.debug_name,
                    as_of,
                    since
                );
                as_of.join_assign(&since);
            }
        } else {
            // Bind the since frontier to the dataflow description.
            dataflow.set_as_of(since);
        }

        // Optimize the dataflow across views, and any other ways that appeal.
        transform::optimize_dataflow(&mut dataflow);

        // Finalize the dataflow by broadcasting its construction to all workers.
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::CreateDataflows(vec![dataflow]),
        );
    }

    // Handle metadata surrounding marking a source as persisted.
    fn enable_source_connector_persistence(
        &mut self,
        id: GlobalId,
        source_connector: SourceConnector,
    ) {
        if let SourceConnector::External { connector, .. } = &source_connector {
            if connector.persistence_enabled() {
                if let Some(persistence_tx) = &mut self.persistence_tx {
                    block_on(persistence_tx.send(PersistenceMessage::AddSource(id)))
                        .expect("failed to send CREATE SOURCE notification to persistence thread");
                    self.catalog.set_source_connector(id, source_connector);
                } else {
                    log::error!(
                        "trying to create a persistent source ({}) but persistence is disabled.",
                        id
                    );
                }
            }
        }
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

fn auto_generate_primary_idx(
    index_name: String,
    on_name: FullName,
    on_id: GlobalId,
    on_desc: &RelationDesc,
) -> catalog::Index {
    let default_key = on_desc.typ().default_key();

    catalog::Index {
        create_sql: index_sql(index_name, on_name, &on_desc, &default_key),
        plan_cx: PlanContext::default(),
        on: on_id,
        keys: default_key.iter().map(|k| ScalarExpr::Column(*k)).collect(),
    }
}

// TODO(benesch): constructing the canonical CREATE INDEX statement should be
// the responsibility of the SQL package.
pub fn index_sql(
    index_name: String,
    view_name: FullName,
    view_desc: &RelationDesc,
    keys: &[usize],
) -> String {
    use sql::ast::{Expr, Ident, Value};

    CreateIndexStatement {
        name: Some(Ident::new(index_name)),
        on_name: sql::normalize::unresolve(view_name),
        key_parts: Some(
            keys.iter()
                .map(|i| match view_desc.get_unambiguous_name(*i) {
                    Some(n) => Expr::Identifier(vec![Ident::new(n.to_string())]),
                    _ => Expr::Value(Value::Number((i + 1).to_string())),
                })
                .collect(),
        ),
        if_not_exists: false,
    }
    .to_ast_string_stable()
}
