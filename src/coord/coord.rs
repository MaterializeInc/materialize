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
use std::fs;
use std::iter;
use std::path::Path;

use failure::bail;
use futures::executor::block_on;
use futures::future::FutureExt;
use futures::future::{self, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{self, StreamExt, TryStreamExt};
use timely::progress::frontier::{Antichain, AntichainRef, MutableAntichain};
use timely::progress::ChangeBatch;

use catalog::names::{DatabaseSpecifier, FullName};
use catalog::{Catalog, CatalogItem};
use dataflow::logging::materialized::MaterializedEvent;
use dataflow::{SequencedCommand, WorkerFeedback, WorkerFeedbackWithMeta};
use dataflow_types::logging::LoggingConfig;
use dataflow_types::{
    DataflowDesc, IndexDesc, PeekResponse, PeekWhen, SinkConnector, TailSinkConnector, Timestamp,
    Update,
};
use expr::transform::Optimizer;
use expr::{
    EvalEnv, GlobalId, Id, IdHumanizer, OptimizedRelationExpr, RelationExpr, ScalarExpr,
    SourceInstanceId,
};
use futures::Stream;
use ore::{collections::CollectionExt, future::MaybeFuture};
use repr::{ColumnName, Datum, RelationDesc, RelationType, Row};
use sql::{MutationKind, ObjectType, Plan, Session};
use sql::{Params, PreparedStatement};

use crate::persistence::SqlSerializer;
use crate::timestamp::{TimestampChannel, TimestampMessage};
use crate::{Command, ExecuteResponse, Response, StartupMessage};

type ClientTx = futures::channel::oneshot::Sender<Response<ExecuteResponse>>;

enum Message {
    Command(Command),
    Worker(WorkerFeedbackWithMeta),
    PlanReady(Session, ClientTx, Result<Plan, failure::Error>, u32),
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
    pub ts_channel: Option<TimestampChannel>,
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
    /// Channel for exchanging timestamping information. None if timestamping not activated
    source_updates: Option<TimestampChannel>,
    /// Instance count: number of times sources have been instantiated in views. This is used
    /// to associate each new instance of a source with a unique instance id (iid)
    local_input_time: Timestamp,
    log: bool,
    executor: Option<tokio::runtime::Handle>,
    feedback_rx: Option<comm::mpsc::Receiver<WorkerFeedbackWithMeta>>,
}

impl<C> Coordinator<C>
where
    C: comm::Connection,
{
    pub fn new(config: Config<C>) -> Result<Self, failure::Error> {
        let mut broadcast_tx = config.switchboard.broadcast_tx(dataflow::BroadcastToken);

        let symbiosis = if let Some(symbiosis_url) = config.symbiosis_url {
            Some(
                config
                    .executor
                    .enter(|| block_on(symbiosis::Postgres::open_and_erase(symbiosis_url)))?,
            )
        } else {
            None
        };

        let catalog_path = if let Some(data_directory) = config.data_directory {
            fs::create_dir_all(data_directory)?;
            Some(data_directory.join("catalog"))
        } else {
            None
        };

        let mut optimizer = Optimizer::default();

        let catalog_path = catalog_path.as_deref();
        let catalog = if let Some(logging_config) = config.logging {
            Catalog::open::<SqlSerializer, _>(catalog_path, |catalog| {
                for log_src in logging_config.active_logs() {
                    let view_name = FullName {
                        database: DatabaseSpecifier::Ambient,
                        schema: "mz_catalog".into(),
                        item: log_src.name().into(),
                    };
                    let index_name = format!("{}_primary_idx", log_src.name());
                    catalog.insert_item(
                        log_src.id(),
                        view_name.clone(),
                        CatalogItem::View(catalog::View {
                            create_sql: "<system log>".to_string(),
                            // Dummy placeholder
                            expr: OptimizedRelationExpr::declare_optimized(RelationExpr::constant(
                                vec![vec![]],
                                log_src.schema().typ().clone(),
                            )),
                            eval_env: EvalEnv::default(),
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
                            eval_env: EvalEnv::default(),
                        }),
                    );
                }

                for log_view in logging_config.active_views() {
                    let params = Params {
                        datums: Row::pack(&[]),
                        types: vec![],
                    };
                    let stmt = sql::parse(log_view.sql.to_owned())
                        .expect("failed to parse bootstrap sql")
                        .into_element();
                    match sql::plan(catalog, &sql::InternalSession, stmt, &params) {
                        MaybeFuture::Immediate(Some(Ok(Plan::CreateView {
                            name: _,
                            view,
                            replace,
                            materialize,
                        }))) => {
                            assert!(replace.is_none());
                            assert!(materialize);
                            let eval_env = EvalEnv::default();
                            let view = catalog::View {
                                create_sql: view.create_sql,
                                expr: optimizer.optimize(view.expr, catalog.indexes(), &eval_env),
                                eval_env,
                                desc: view.desc,
                            };
                            let view_name = FullName {
                                database: DatabaseSpecifier::Ambient,
                                schema: "mz_catalog".into(),
                                item: log_view.name.into(),
                            };
                            let index_name = format!("{}_primary_idx", log_view.name);
                            let index = auto_generate_primary_idx(
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
            Catalog::open::<SqlSerializer, _>(catalog_path, |_| ())?
        };

        let executor = config.executor;
        executor.enter(move || {
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
                executor: Some(config.executor.clone()),
                source_updates: config.ts_channel,
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
                    CatalogItem::Source(_) => (),
                    CatalogItem::View(view) => {
                        coord.insert_view(id, &view);
                    }
                    CatalogItem::Sink(sink) => {
                        coord.create_sink_dataflow(name.to_string(), id, sink);
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
        self.executor
            .take()
            .expect("serve called twice on coordinator")
            .enter(|| {
                let streams: Vec<Box<dyn Stream<Item = Result<Message, comm::Error>> + Unpin>> = vec![
                                        Box::new(
                                            cmd_rx
                                                .map(Message::Command)
                                                .chain(stream::once(future::ready(Message::Shutdown)))
                                                .map(Ok),
                                        ),
                                        Box::new(self.feedback_rx.take().unwrap().map_ok(Message::Worker)),
                                    ];

                let mut messages = stream::select_all(streams);

               while let Some(msg) = block_on(messages.next()) {
                    // Check for timestamp updates
                    if let Some(source_updates) = &self.source_updates {
                        while let Ok(update) = source_updates.receiver.try_recv() {
                            match update {
                                TimestampMessage::BatchedUpdate(timestamp, updates) => {
                                    for (id, offset) in updates {
                                        broadcast(
                                            &mut self.broadcast_tx,
                                            SequencedCommand::AdvanceSourceTimestamp {
                                                id,
                                                timestamp,
                                                offset,
                                            },
                                        );
                                    }
                                }
                                TimestampMessage::Update(id, timestamp, offset) => {
                                    broadcast(
                                        &mut self.broadcast_tx,
                                        SequencedCommand::AdvanceSourceTimestamp {
                                            id,
                                            timestamp,
                                            offset,
                                        },
                                    );
                                },
                               _ => {}
                            }
                        }
                    }

                    match msg.expect("coordinator message receiver failed") {
                        Message::Command(Command::Startup {
                            session,
                            tx,
                        }) => {
                            let mut messages = vec![];
                            if self.catalog.database_resolver(session.database()).is_err() {
                                messages.push(StartupMessage::UnknownSessionDatabase);
                            }
                            let _ = tx.send(Response { result: Ok(messages), session });
                        }
                        Message::Command(Command::Execute {
                            portal_name,
                            session,
                            conn_id,
                            tx,
                        }) => {
                            let result = self.handle_begin_execute(session, portal_name, tx);
                            match result {
                                MaybeFuture::Immediate(val) => {
                                    let (mut session, tx, result) = val.unwrap();
                                    let result = result.and_then(|plan| {
                                        self.sequence_plan(&mut session, plan, conn_id)
                                    });
                                    let _ = tx.send(Response { result, session });
                                }
                                MaybeFuture::Future(fut) => {
                                    let (self_tx, self_rx) = futures::channel::oneshot::channel();
                                    let self_rx = stream::once(self_rx.map(|res| res.unwrap()));
                                    messages.push(Box::new(self_rx));
                                    let fut = async move {
                                        let (session, tx, result) = fut.await;
                                        self_tx
                                            .send(Ok(Message::PlanReady(
                                                session, tx, result, conn_id,
                                            )))
                                            .map_err(|_e| "(comm error)")
                                            .expect("Unexpected coordinator communication failure");
                                    };
                                    tokio::spawn(fut);
                                }
                            }
                        }

                        Message::PlanReady(mut session, tx, result, conn_id) => {
                            let result = result
                                .and_then(|plan| self.sequence_plan(&mut session, plan, conn_id));

                            let _ = tx.send(Response { result, session });
                        }

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

                        Message::Shutdown => {
                            if let Some(channel) = &mut self.source_updates {
                                channel.sender.send(TimestampMessage::Shutdown).unwrap();
                            }
                            self.shutdown();
                            break;
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
                            message: WorkerFeedback::DroppedSource(source_id)}) => {
                            // Notify timestamping thread that source has been dropped
                            if let Some(channel) = &mut self.source_updates{
                                channel.sender.send(TimestampMessage::DropInstance(source_id)).expect("Failed to send Drop Instance notice to Coordinator");
                            }
                        },
                        Message::Worker(WorkerFeedbackWithMeta {
                                            worker_id: _,
                                            message: WorkerFeedback::CreateSource(source_id,ksc,consistency)}) => {
                            // Notify timestamping thread that source has been created
                            if let Some(channel) = &mut self.source_updates{
                                channel.sender.send(TimestampMessage::Add(source_id, ksc.addr, ksc.topic,consistency)).expect("Failed to send CREATE Instance notice to Coordinator");
                            }
                        }
                    }
                }

                // Cleanly drain any pending messages from the worker before shutting
                // down.
                while let Some(msg) = block_on(messages.next()) {
                    match msg.expect("coordinator message receiver failed") {
                        Message::Command(_) | Message::Shutdown => unreachable!(),
                        Message::Worker(_) | Message::PlanReady(_, _, _, _) => (),
                   }
                }
            });
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

    pub fn sequence_plan(
        &mut self,
        session: &mut Session,
        plan: Plan,
        conn_id: u32,
    ) -> Result<ExecuteResponse, failure::Error> {
        match plan {
            Plan::CreateDatabase {
                name,
                if_not_exists,
            } => {
                let ops = vec![
                    catalog::Op::CreateDatabase { name: name.clone() },
                    catalog::Op::CreateSchema {
                        database_name: DatabaseSpecifier::Name(name),
                        schema_name: "public".into(),
                    },
                ];
                match self.catalog_transact(ops) {
                    Ok(_) => Ok(ExecuteResponse::CreatedDatabase { existed: false }),
                    Err(_) if if_not_exists => {
                        Ok(ExecuteResponse::CreatedDatabase { existed: true })
                    }
                    Err(err) => Err(err),
                }
            }

            Plan::CreateSchema {
                database_name,
                schema_name,
                if_not_exists,
            } => {
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

            Plan::CreateTable {
                name,
                desc,
                if_not_exists,
            } => {
                let view_id = self.catalog.allocate_id()?;
                let view = catalog::View {
                    create_sql: "<created by CREATE TABLE>".to_string(),
                    expr: OptimizedRelationExpr::declare_optimized(
                        // TODO: Adding a second `vec![]` here avoids some defect where
                        // uniqueness of the constant expression results in incorrect
                        // computation when using tables; this happens when we use the
                        // type information (unique keys) to recommend which columns to
                        // use for a default arrangement.
                        RelationExpr::constant(vec![vec![], vec![]], desc.typ().clone()),
                    ),
                    eval_env: EvalEnv::default(),
                    desc,
                };
                let index_id = self.catalog.allocate_id()?;
                let mut index_name = name.clone();
                index_name.item += "_primary_idx";
                let index = auto_generate_primary_idx(
                    index_name.item.clone(),
                    name.clone(),
                    &view,
                    view_id,
                );
                match self.catalog_transact(vec![
                    catalog::Op::CreateItem {
                        id: view_id,
                        name: name.clone(),
                        item: CatalogItem::View(view.clone()),
                    },
                    catalog::Op::CreateItem {
                        id: index_id,
                        name: index_name,
                        item: CatalogItem::Index(index.clone()),
                    },
                ]) {
                    Ok(_) => {
                        self.insert_view(view_id, &view);
                        broadcast(
                            &mut self.broadcast_tx,
                            SequencedCommand::CreateLocalInput {
                                name: name.to_string(),
                                index_id,
                                index: IndexDesc {
                                    on_id: index.on,
                                    keys: index.keys.clone(),
                                },
                                on_type: view.desc.typ().clone(),
                                advance_to: self.local_input_time,
                            },
                        );
                        self.insert_index(index_id, &index, None);
                        Ok(ExecuteResponse::CreatedTable { existed: false })
                    }
                    Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedTable { existed: true }),
                    Err(err) => Err(err),
                }
            }

            Plan::CreateSource {
                name,
                source,
                if_not_exists,
            } => {
                let source = catalog::Source {
                    create_sql: source.create_sql,
                    connector: source.connector,
                    desc: source.desc,
                };
                let source_id = self.catalog.allocate_id()?;
                let op = catalog::Op::CreateItem {
                    id: source_id,
                    name,
                    item: CatalogItem::Source(source),
                };
                match self.catalog_transact(vec![op]) {
                    Ok(()) => Ok(ExecuteResponse::CreatedSource { existed: false }),
                    Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedSource { existed: true }),
                    Err(err) => Err(err),
                }
            }

            Plan::CreateSink {
                name,
                sink,
                if_not_exists,
            } => {
                let sink = catalog::Sink {
                    create_sql: sink.create_sql,
                    from: sink.from,
                    connector: sink.connector,
                };
                let id = self.catalog.allocate_id()?;
                let op = catalog::Op::CreateItem {
                    id,
                    name: name.clone(),
                    item: CatalogItem::Sink(sink.clone()),
                };
                match self.catalog_transact(vec![op]) {
                    Ok(()) => {
                        self.create_sink_dataflow(name.to_string(), id, sink);
                        Ok(ExecuteResponse::CreatedSink { existed: false })
                    }
                    Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedSink { existed: true }),
                    Err(err) => Err(err),
                }
            }

            Plan::CreateView {
                name,
                view,
                replace,
                materialize,
            } => {
                let mut ops = vec![];
                if let Some(id) = replace {
                    ops.extend(self.catalog.drop_items_ops(&[id]));
                }
                let view_id = self.catalog.allocate_id()?;
                let eval_env = EvalEnv::default();
                let view = catalog::View {
                    create_sql: view.create_sql,
                    expr: self
                        .optimizer
                        .optimize(view.expr, self.catalog.indexes(), &eval_env),
                    desc: view.desc,
                    eval_env,
                };
                ops.push(catalog::Op::CreateItem {
                    id: view_id,
                    name: name.clone(),
                    item: CatalogItem::View(view.clone()),
                });
                let (index_id, index) = if materialize {
                    let mut index_name = name.clone();
                    index_name.item += "_primary_idx";
                    let index = auto_generate_primary_idx(
                        index_name.item.clone(),
                        name.clone(),
                        &view,
                        view_id,
                    );
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
                self.catalog_transact(ops)?;
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
                Ok(ExecuteResponse::CreatedView)
            }

            Plan::CreateIndex {
                name,
                index,
                if_not_exists,
            } => {
                let index = catalog::Index {
                    create_sql: index.create_sql,
                    keys: index.keys,
                    on: index.on,
                    eval_env: EvalEnv::default(),
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

            Plan::DropDatabase { name } => {
                let ops = self.catalog.drop_database_ops(name);
                self.catalog_transact(ops)?;
                Ok(ExecuteResponse::DroppedDatabase)
            }

            Plan::DropSchema {
                database_name,
                schema_name,
            } => {
                let ops = self.catalog.drop_schema_ops(database_name, schema_name);
                self.catalog_transact(ops)?;
                Ok(ExecuteResponse::DroppedSchema)
            }

            Plan::DropItems { items, ty } => {
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

            Plan::EmptyQuery => Ok(ExecuteResponse::EmptyQuery),

            Plan::ShowAllVariables => Ok(send_immediate_rows(
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
            )),

            Plan::ShowVariable(name) => {
                let variable = session.get(&name)?;
                let row = Row::pack(&[Datum::String(&variable.value())]);
                Ok(send_immediate_rows(vec![row]))
            }

            Plan::SetVariable { name, value } => {
                session.set(&name, &value)?;
                Ok(ExecuteResponse::SetVariable { name })
            }

            Plan::StartTransaction => {
                session.start_transaction();
                Ok(ExecuteResponse::StartTransaction)
            }

            Plan::Commit => {
                session.end_transaction();
                Ok(ExecuteResponse::Commit)
            }

            Plan::Rollback => {
                session.end_transaction();
                Ok(ExecuteResponse::Rollback)
            }

            Plan::Peek {
                source,
                when,
                finishing,
                materialize,
            } => {
                let timestamp = self.determine_timestamp(&source, when)?;
                let eval_env = EvalEnv {
                    wall_time: Some(chrono::Utc::now()),
                    logical_time: Some(timestamp),
                };
                // TODO (wangandi): Is there anything that optimizes to a
                // constant expression that originally contains a global get? Is
                // there anything not containing a global get that cannot be
                // optimized to a constant expression?
                let mut source = self
                    .optimizer
                    .optimize(source, self.catalog.indexes(), &eval_env);

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
                        if let Some(Some((index_id, _))) =
                            self.views.get(&id).map(|v| &v.default_idx)
                        {
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
                            expr: source,
                            desc,
                            eval_env: eval_env.clone(),
                        };
                        self.build_view_collection(&view_id, &view, &mut dataflow);
                        let index =
                            auto_generate_primary_idx(index_name, view_name, &view, view_id);
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
                            eval_env,
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
                                    let out: Result<_, comm::Error> = Ok(PeekResponse::Rows(memo));
                                    future::ready(out)
                                }
                                _ => future::ok(PeekResponse::Canceled),
                            }
                        })
                        .map_ok(move |mut resp| {
                            if let PeekResponse::Rows(rows) = &mut resp {
                                finishing.finish(rows)
                            }
                            resp
                        })
                        .err_into();

                    Ok(ExecuteResponse::SendRows(Box::pin(rows_rx)))
                }
            }

            Plan::Tail(source) => {
                let source_id = source.id();
                let index_id = if let Some(Some((index_id, _))) = self
                    .views
                    .get(&source_id)
                    .map(|view_state| &view_state.default_idx)
                {
                    index_id
                } else {
                    bail!("Cannot tail a view that has not been materialized.")
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
                let since = self
                    .upper_of(index_id)
                    .expect("name missing at coordinator")
                    .get(0)
                    .copied()
                    .unwrap_or(Timestamp::max_value());
                let sink = catalog::Sink {
                    create_sql: "<ignored>".into(),
                    from: source_id,
                    connector: SinkConnector::Tail(TailSinkConnector { tx, since }),
                };
                self.create_sink_dataflow(sink_name, sink_id, sink);
                Ok(ExecuteResponse::Tailing { rx })
            }

            Plan::SendRows(rows) => Ok(send_immediate_rows(rows)),

            Plan::ExplainPlan(relation_expr) => {
                let eval_env = EvalEnv {
                    wall_time: Some(chrono::Utc::now()),
                    logical_time: Some(0),
                };
                let relation_expr =
                    self.optimizer
                        .optimize(relation_expr, self.catalog.indexes(), &eval_env);
                let pretty = relation_expr.as_ref().pretty_humanized(&self.catalog);
                let rows = vec![Row::pack(&[Datum::from(&*pretty)])];
                Ok(send_immediate_rows(rows))
            }

            Plan::SendDiffs {
                id,
                updates,
                affected_rows,
                kind,
            } => {
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

            Plan::ShowViews {
                ids,
                full,
                materialized: show_materialized,
            } => {
                let view_information = ids
                    .into_iter()
                    .filter_map(|(name, id)| {
                        let class = match id {
                            GlobalId::System(_) => "SYSTEM",
                            GlobalId::User(_) => "USER",
                        };
                        if let Some(view_state) = self.views.get(&id) {
                            if !show_materialized || view_state.default_idx.is_some() {
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
                        if full {
                            if show_materialized {
                                Row::pack(&[Datum::from(name.as_str()), Datum::from(class)])
                            } else {
                                Row::pack(&[
                                    Datum::from(name.as_str()),
                                    Datum::from(class),
                                    Datum::from(queryable),
                                    Datum::from(materialized),
                                ])
                            }
                        } else {
                            Row::pack(&[Datum::from(name.as_str())])
                        }
                    })
                    .collect::<Vec<_>>();
                rows.sort_unstable_by(move |a, b| a.unpack_first().cmp(&b.unpack_first()));
                Ok(send_immediate_rows(rows))
            }
        }
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
                        CatalogItem::Source(_) => sources_to_drop.push(entry.id()),
                        CatalogItem::View(_) => views_to_drop.push(entry.id()),
                        CatalogItem::Sink(_) => sinks_to_drop.push(entry.id()),
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
        match self.catalog.get_by_id(id).item() {
            CatalogItem::Source(source) => {
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
                if let Some((index_id, keys)) = &self.views[id].default_idx {
                    let index_desc = IndexDesc {
                        on_id: *id,
                        keys: keys.to_vec(),
                    };
                    dataflow.add_index_import(*index_id, index_desc, view.desc.typ().clone(), *id);
                } else {
                    self.build_view_collection(id, &view, dataflow);
                }
            }

            _ => unreachable!(),
        }
    }

    fn build_view_collection(
        &self,
        view_id: &GlobalId,
        view: &catalog::View,
        dataflow: &mut DataflowDesc,
    ) {
        // TODO: We only need to import Get arguments for which we cannot find arrangements.
        view.expr.as_ref().visit(&mut |e| {
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
        view.expr.as_ref().visit(&mut |e| {
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
                            if let Some(index) = view.primary_idxes.get(key_set) {
                                dataflow.add_index_import(
                                    *index,
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
            view.expr.clone(),
            view.eval_env.clone(),
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
        dataflow.add_index_to_build(
            *id,
            index.on.clone(),
            on_type.clone(),
            index.keys.clone(),
            index.eval_env.clone(),
        );
        dataflow.add_index_export(*id, index.on, on_type, index.keys.clone());
        // TODO: should we still support creating multiple dataflows with a single command,
        // Or should it all be compacted into a single DataflowDesc with multiple exports?
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::CreateDataflows(vec![dataflow]),
        );
        self.insert_index(*id, &index, None);
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

    fn create_sink_dataflow(&mut self, name: String, id: GlobalId, sink: catalog::Sink) {
        let mut dataflow = DataflowDesc::new(name);
        self.import_source_or_view(&id, &sink.from, &mut dataflow);
        let from_type = self.catalog.get_by_id(&sink.from).desc().unwrap().clone();
        dataflow.add_sink_export(id, sink.from, from_type, sink.connector);
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
                    view_state.drop_primary_idx(&idx.keys);
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
            for id in view_state.primary_idxes.values() {
                results.push(*id);
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
            bail!("Cannot construct query out of existing materialized views");
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
    pub fn update_upper(&mut self, name: &GlobalId, mut changes: ChangeBatch<Timestamp>) {
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
                    let mut since = Antichain::new();
                    for time in index_state.upper.frontier().iter() {
                        since.insert(time.saturating_sub(compaction_latency_ms));
                    }
                    self.since_updates
                        .push((name.clone(), since.elements().to_vec()));
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
        let mut uses_views = Vec::new();
        view.expr.as_ref().global_uses(&mut uses_views);
        uses_views.sort();
        uses_views.dedup();
        let (queryable, uses_views): (Vec<_>, Vec<_>) = uses_views
            .into_iter()
            .filter_map(|id| {
                if let Some(view_state) = self.views.get(&id) {
                    Some((view_state.queryable, id))
                } else {
                    None
                }
            })
            .unzip();
        self.views.insert(
            view_id,
            ViewState::new(
                !queryable.is_empty() && queryable.iter().all(|q| *q),
                uses_views,
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
        let mut index_state = IndexState::new(self.num_timely_workers);
        if latency_ms.is_some() {
            index_state.set_compaction_latency(latency_ms);
        }
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
    ) -> MaybeFuture<'static, Result<sql::Plan, failure::Error>> {
        let plan_result = sql::plan(&self.catalog, session, stmt.clone(), params);
        // Try Postgres if we realize synchronously that we failed.
        if let MaybeFuture::Immediate(Some(Err(err))) = plan_result {
            match self.symbiosis {
                Some(ref mut postgres) if postgres.can_handle(&stmt) => {
                    block_on(postgres.execute(&self.catalog, session, &stmt))
                }
                _ => Err(err),
            }
            .into()
        // Otherwise, just return the future.
        // Nothing that we do asynchronously could
        // possibly work in Postgres anyway, so don't bother
        // piping through the logic to try in symbiosis mode in this case.
        } else {
            plan_result
        }
    }

    fn handle_begin_execute(
        &mut self,
        session: Session,
        portal_name: String,
        tx: ClientTx,
    ) -> MaybeFuture<'static, (Session, ClientTx, Result<Plan, failure::Error>)> {
        let res = session
            .get_portal(&portal_name)
            .ok_or_else(|| failure::format_err!("portal does not exist {:?}", portal_name))
            .and_then(|portal| {
                session
                    .get_prepared_statement(&portal.statement_name)
                    .ok_or_else(|| {
                        failure::format_err!(
                            "statement for portal does not exist portal={:?} statement={:?}",
                            portal_name,
                            portal.statement_name
                        )
                    })
                    .map(|ps| (portal, ps))
            });
        let (portal, prepared) = match res {
            Ok((portal, prepared)) => (portal, prepared),
            Err(e) => {
                return (session, tx, Err(e)).into();
            }
        };
        match prepared.sql() {
            Some(stmt) => self
                .handle_statement(&session, stmt.clone(), &portal.parameters)
                .map(|res| (session, tx, res)),
            None => (session, tx, Ok(Plan::EmptyQuery)).into(),
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
    ExecuteResponse::SendRows(Box::pin(rx.err_into()))
}

pub struct IndexState {
    /// The most recent frontier for new data.
    /// All further changes will be in advance of this bound.
    upper: MutableAntichain<Timestamp>,
    /// The compaction frontier.
    /// All peeks in advance of this frontier will be correct,
    /// but peeks not in advance of this frontier may not be.
    #[allow(dead_code)]
    since: Antichain<Timestamp>,
    /// Compaction delay.
    ///
    /// This timestamp drives the advancement of the since frontier as a
    /// function of the upper frontier, trailing it by exactly this much.
    compaction_latency_ms: Option<Timestamp>,
}

impl IndexState {
    /// Creates an empty index state from a number of workers.
    pub fn new(workers: usize) -> Self {
        let mut upper = MutableAntichain::new();
        upper.update_iter(Some((0, workers as i64)));
        Self {
            upper,
            since: Antichain::from_elem(0),
            compaction_latency_ms: Some(60_000),
        }
    }

    /// Sets the latency behind the collection frontier at which compaction occurs.
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
    primary_idxes: BTreeMap<Vec<ScalarExpr>, GlobalId>,
    // TODO(andiwang): materialize#220 Implement seconary indexes
    // secondary_idxes: BTreeMap<Vec<ScalarExpr>, GlobalId>,
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
        self.primary_idxes.insert(primary_idx.to_owned(), id);
    }

    pub fn drop_primary_idx(&mut self, primary_idx: &[ScalarExpr]) {
        self.primary_idxes.remove(primary_idx);
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
                .map(|(keys, id)| (*id, keys.to_owned()));
        }
    }
}

pub fn auto_generate_primary_idx(
    index_name: String,
    view_name: FullName,
    view: &catalog::View,
    view_id: GlobalId,
) -> catalog::Index {
    let keys = view.expr.as_ref().typ().keys;
    let keys = if let Some(keys) = keys.first() {
        keys.clone()
    } else {
        (0..view.desc.typ().column_types.len()).collect()
    };
    catalog::Index {
        create_sql: index_sql(index_name, view_name, &view.desc, &keys),
        on: view_id,
        keys: keys.into_iter().map(ScalarExpr::Column).collect(),
        eval_env: EvalEnv::default(),
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
        name: Ident {
            value: index_name,
            quote_style: Some('"'),
        },
        on_name: sql::normalize::unresolve(view_name),
        key_parts: keys
            .iter()
            .map(|i| match view_desc.get_unambiguous_name(*i) {
                Some(n) => Expr::Identifier(Ident {
                    value: n.to_string(),
                    quote_style: Some('"'),
                }),
                _ => Expr::Value(Value::Number((i + 1).to_string())),
            })
            .collect(),
        if_not_exists: false,
    }
    .to_string()
}
