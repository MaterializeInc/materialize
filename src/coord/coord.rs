// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Coordination of installed views, available timestamps, and compacted timestamps.
//!
//! The command coordinator maintains a view of the installed views, and for each tracks
//! the frontier of available times (`upper`) and the frontier of compacted times (`since`).
//! The upper frontier describes times that may not return immediately, as any timestamps in
//! advance of the frontier are still open. The since frontier constrains those times for
//! which the maintained view will be correct, as any timestamps in advance of the frontier
//! must accumulate to the same value as would an un-compacted trace.

// Clone on copy permitted for timestamps, which happen to be Copy at the moment, but which
// may become non-copy in the future.
#![allow(clippy::clone_on_copy)]

use std::collections::{HashMap, HashSet};
use std::fs;
use std::iter;
use std::path::Path;
use std::str::FromStr;

use failure::bail;
use futures::executor::block_on;
use futures::future::FutureExt;
use futures::future::{self, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{self, StreamExt, TryStreamExt};
use timely::progress::frontier::{Antichain, AntichainRef, MutableAntichain};
use timely::progress::ChangeBatch;

use catalog::{Catalog, CatalogItem, QualName};
use dataflow::logging::materialized::MaterializedEvent;
use dataflow::{SequencedCommand, WorkerFeedback, WorkerFeedbackWithMeta};
use dataflow_types::logging::LoggingConfig;
use dataflow_types::{
    DataflowDesc, IndexDesc, PeekResponse, PeekWhen, Sink, SinkConnector, TailSinkConnector,
    Timestamp, Update, View,
};
use expr::{EvalEnv, GlobalId, Id, IdHumanizer, RelationExpr, ScalarExpr};
use ore::{collections::CollectionExt, future::MaybeFuture};
use repr::{ColumnName, Datum, RelationDesc, Row};
use sql::{MutationKind, ObjectType, Plan, Session};
use sql::{Params, PreparedStatement};

use crate::{Command, ExecuteResponse, Response};
use futures::Stream;

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
    pub bootstrap_sql: String,
    pub data_directory: Option<&'a Path>,
    pub executor: &'a tokio::runtime::Handle,
}

/// Glues the external world to the Timely workers.
pub struct Coordinator<C>
where
    C: comm::Connection,
{
    switchboard: comm::Switchboard<C>,
    broadcast_tx: comm::broadcast::Sender<SequencedCommand>,
    num_timely_workers: usize,
    optimizer: expr::transform::Optimizer,
    catalog: Catalog,
    symbiosis: Option<symbiosis::Postgres>,
    views: HashMap<GlobalId, ViewState>,
    /// Each source name maps to a source.
    sources: HashMap<GlobalId, dataflow_types::Source>,
    /// Maps (view id, keys index is arranged on) -> (how many aliases it has, id of the first alias)
    indexes: HashMap<IndexDesc, (usize, GlobalId)>,
    /// Maps (id corresponding to an index name) -> (view id, keys index is arranged on)
    index_aliases: HashMap<GlobalId, IndexDesc>,
    since_updates: Vec<(GlobalId, Vec<Timestamp>)>,
    /// For each connection running a TAIL command, the name of the dataflow
    /// that is servicing the TAIL. A connection can only run one TAIL at a
    /// time.
    active_tails: HashMap<u32, GlobalId>,
    local_input_time: Timestamp,
    log: bool,
    executor: Option<tokio::runtime::Handle>,
}

impl<C> Coordinator<C>
where
    C: comm::Connection,
{
    pub fn new(config: Config<C>) -> Result<Self, failure::Error> {
        let broadcast_tx = config.switchboard.broadcast_tx(dataflow::BroadcastToken);

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

        let catalog_path = catalog_path.as_deref();
        let catalog = if let Some(logging_config) = config.logging {
            Catalog::open(
                catalog_path,
                logging_config
                    .active_logs()
                    .iter()
                    .map(|log| {
                        (
                            log.id(),
                            QualName::from_str(log.name()).expect("invalid logging name"),
                            CatalogItem::View(View {
                                raw_sql: "<system log>".to_string(),
                                // Dummy placeholder
                                relation_expr: RelationExpr::constant(
                                    vec![vec![]],
                                    log.schema().typ().clone(),
                                ),
                                eval_env: EvalEnv::default(),
                                desc: log.schema(),
                            }),
                        )
                    })
                    .chain(logging_config.active_logs().iter().map(|log| {
                        (
                            log.index_id(),
                            QualName::from_str(log.name())
                                .expect("invalid logging name")
                                .with_trailing_string("_PRIMARY_IDX"),
                            CatalogItem::Index(dataflow_types::Index::new_from_cols(
                                log.id(),
                                log.index_by(),
                                &log.schema(),
                            )),
                        )
                    })),
            )?
        } else {
            Catalog::open(catalog_path, iter::empty())?
        };

        let mut coord = Self {
            switchboard: config.switchboard,
            broadcast_tx,
            num_timely_workers: config.num_timely_workers,
            optimizer: Default::default(),
            catalog,
            symbiosis,
            views: HashMap::new(),
            sources: HashMap::new(),
            indexes: HashMap::new(),
            index_aliases: HashMap::new(),
            since_updates: Vec::new(),
            active_tails: HashMap::new(),
            local_input_time: 1,
            log: config.logging.is_some(),
            executor: Some(config.executor.clone()),
        };

        let executor = config.executor;
        let bootstrap_sql = config.bootstrap_sql;
        let logging = config.logging;
        executor.enter(move || {
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
                    CatalogItem::Source(source) => {
                        coord.sources.insert(id, source);
                    }
                    CatalogItem::View(view) => {
                        coord.insert_view(id, view, None);
                    }
                    CatalogItem::Sink(sink) => {
                        coord.create_sink_dataflow(name.to_string(), id, sink);
                    }
                    CatalogItem::Index(index) => match id {
                        GlobalId::User(_) => {
                            coord.create_index_dataflow(name.to_string(), id, index)
                        }
                        GlobalId::System(_) => coord.add_index_to_view(id, index.desc, Some(1_000)),
                    },
                }
            }

            if coord.catalog.bootstrapped() {
                // Per https://github.com/MaterializeInc/materialize/blob/5d85615ba8608f4f6d7a8a6a676c19bb2b37db55/src/pgwire/lib.rs#L52,
                // the first connection ID used is 1. As long as that remains the case,
                // 0 is safe to use here.
                let conn_id = 0;
                let params = Params {
                    datums: Row::pack(&[]),
                    types: vec![],
                };
                let mut session = sql::Session::default();
                // TODO(benesch): these bootstrap statements should be run in a
                // single transaction, so that we don't leave the catalog in a
                // partially-bootstrapped state if one fails.
                for stmt in sql::parse(bootstrap_sql)? {
                    block_on(coord.handle_statement(&session, stmt, &params))
                        .and_then(|plan| coord.sequence_plan(&mut session, plan, conn_id))?;
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
                let feedback_rx = self.enable_feedback();
                let streams: Vec<Box<dyn Stream<Item = Result<Message, comm::Error>> + Unpin>> = vec![
                    Box::new(
                        cmd_rx
                            .map(Message::Command)
                            .chain(stream::once(future::ready(Message::Shutdown)))
                            .map(Ok),
                    ),
                    Box::new(feedback_rx.map_ok(Message::Worker)),
                ];

                let mut messages = stream::select_all(streams);
                while let Some(msg) = block_on(messages.next()) {
                    match msg.expect("coordinator message receiver failed") {
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
            })
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
        Ok(match plan {
            Plan::CreateTable { name, desc } => {
                let view = View {
                    raw_sql: "<created by CREATE TABLE>".to_string(),
                    relation_expr: RelationExpr::constant(vec![vec![]], desc.typ().clone()),
                    eval_env: EvalEnv::default(),
                    desc,
                };
                let view_id = self.register_view(&name, &view)?;
                self.insert_view(view_id, view.clone(), Some(true));
                let index_name = name.with_trailing_string("_PRIMARY_IDX");
                let index = view.auto_generate_primary_idx(view_id);
                let index_id = self.register_index(&index_name, &index)?;
                broadcast(
                    &mut self.broadcast_tx,
                    SequencedCommand::CreateLocalInput {
                        name: name.to_string(),
                        index_id,
                        index: index.clone(),
                        advance_to: self.local_input_time,
                    },
                );
                self.add_index_to_view(index_id, index.desc, None);
                ExecuteResponse::CreatedTable
            }

            Plan::CreateSource(name, source) => {
                let source_id = self.register_source(&name, &source)?;
                self.sources.insert(source_id, source);
                ExecuteResponse::CreatedSource
            }

            Plan::CreateSources(mut sources) => {
                sources.retain(|(name, _)| self.catalog.get(name).is_err());
                for (name, source) in sources.iter() {
                    let source_id = self.register_source(&name, source)?;
                    self.sources.insert(source_id, source.clone());
                }
                send_immediate_rows(
                    sources
                        .iter()
                        .map(|s| Row::pack(&[Datum::String(&s.0.to_string())]))
                        .collect(),
                )
            }

            Plan::CreateSink(name, sink) => {
                let id = self
                    .catalog
                    .insert(name.clone(), CatalogItem::Sink(sink.clone()))?;
                self.report_catalog_update(
                    id,
                    self.catalog.humanize_id(expr::Id::Global(id)).unwrap(),
                    true,
                );
                self.create_sink_dataflow(name.to_string(), id, sink);
                ExecuteResponse::CreatedSink
            }

            Plan::CreateView(name, view) => {
                let id = self.register_view(&name, &view)?;
                self.create_materialized_view_dataflow(name.to_string(), id, view, None)?;
                ExecuteResponse::CreatedView
            }

            Plan::CreateIndex(name, index) => {
                let id = self.register_index(&name, &index)?;
                self.create_index_dataflow(name.to_string(), id, index);
                ExecuteResponse::CreatedIndex
            }

            Plan::DropItems(ids, item_type) => {
                let mut sources_to_drop: Vec<GlobalId> = Vec::new();
                let mut views_to_drop: Vec<GlobalId> = Vec::new();
                let mut sinks_to_drop: Vec<GlobalId> = Vec::new();
                let mut indexes_to_drop: Vec<GlobalId> = Vec::new();
                // Sort ids to be dropped ~before~ removing them from the
                // Coordinator's Catalog below.
                for id in &ids {
                    match self.catalog.get_by_id(id).item() {
                        CatalogItem::Source(_s) => sources_to_drop.push(*id),
                        CatalogItem::View(_v) => views_to_drop.push(*id),
                        CatalogItem::Sink(_s) => sinks_to_drop.push(*id),
                        CatalogItem::Index(_i) => indexes_to_drop.push(*id),
                    }
                }

                for id in &ids {
                    self.report_catalog_update(
                        *id,
                        self.catalog.humanize_id(expr::Id::Global(*id)).unwrap(),
                        false,
                    );
                    self.catalog.remove(*id);
                }

                if !sources_to_drop.is_empty() {
                    broadcast(
                        &mut self.broadcast_tx,
                        SequencedCommand::DropSources(sources_to_drop),
                    );
                }

                if !views_to_drop.is_empty() {
                    self.drop_views(views_to_drop);
                }

                if !sinks_to_drop.is_empty() {
                    broadcast(
                        &mut self.broadcast_tx,
                        SequencedCommand::DropSinks(sinks_to_drop),
                    );
                }

                if !indexes_to_drop.is_empty() {
                    self.drop_indexes(indexes_to_drop, item_type != ObjectType::Index);
                }

                match item_type {
                    ObjectType::Source => ExecuteResponse::DroppedSource,
                    ObjectType::View => ExecuteResponse::DroppedView,
                    ObjectType::Table => ExecuteResponse::DroppedTable,
                    ObjectType::Sink => ExecuteResponse::DroppedSink,
                    ObjectType::Index => ExecuteResponse::DroppedIndex,
                }
            }

            Plan::EmptyQuery => ExecuteResponse::EmptyQuery,

            Plan::SetVariable { name, value } => {
                session.set(&name, &value)?;
                ExecuteResponse::SetVariable { name }
            }

            Plan::StartTransaction => {
                session.start_transaction();
                ExecuteResponse::StartTransaction
            }

            Plan::Commit => {
                session.end_transaction();
                ExecuteResponse::Commit
            }

            Plan::Rollback => {
                session.end_transaction();
                ExecuteResponse::Rollback
            }

            Plan::Peek {
                mut source,
                when,
                finishing,
                mut eval_env,
                materialize,
            } => {
                let timestamp = self.determine_timestamp(&source, when)?;
                eval_env.wall_time = Some(chrono::Utc::now());
                eval_env.logical_time = Some(timestamp);
                // TODO (wangandi): what do we do about this line when we start passing indexes
                // to the optimizer?
                // Related: Is there anything that optimizes to a constant expression that originally
                // contains a global get? Is there anything not containing a global get that cannot be optimized to
                // a constant expression?
                self.optimizer.optimize(&mut source, &eval_env);

                // If this optimizes to a constant expression, we can immediately return the result.
                if let RelationExpr::Constant { rows, typ: _ } = source {
                    let mut results = Vec::new();
                    for (row, count) in rows {
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
                    send_immediate_rows(results)
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

                    let (project, filter) = Self::plan_peek(&mut source);

                    let (fast_path, view_id) = if let RelationExpr::Get {
                        id: Id::Global(id),
                        typ: _,
                    } = source
                    {
                        if self.upper_of(&id).is_some() {
                            (true, id)
                        } else if materialize {
                            (false, self.catalog.allocate_id())
                        } else {
                            bail!(
                                "{} is not materialized",
                                self.catalog.humanize_id(expr::Id::Global(id)).unwrap()
                            )
                        }
                    } else {
                        (false, self.catalog.allocate_id())
                    };

                    if !fast_path {
                        // Slow path. We need to perform some computation, so build
                        // a new transient dataflow that will be dropped after the
                        // peek completes.
                        let typ = source.typ();
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
                            typ,
                            iter::repeat::<Option<ColumnName>>(None).take(ncols),
                        );
                        let view = View {
                            raw_sql: "<none>".into(),
                            relation_expr: source,
                            desc,
                            eval_env: eval_env.clone(),
                        };
                        self.create_materialized_view_dataflow(
                            format!("temp-view-{}", view_id),
                            view_id,
                            view,
                            Some(vec![timestamp.clone()]),
                        )?;
                    }

                    broadcast(
                        &mut self.broadcast_tx,
                        SequencedCommand::Peek {
                            id: view_id,
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
                        self.drop_views(vec![view_id]);
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

                    ExecuteResponse::SendRows(Box::pin(rows_rx))
                }
            }

            Plan::Tail(source) => {
                let source_id = source.id();
                if !self.views.contains_key(&source_id)
                    || self.views[&source_id].materialization.is_none()
                {
                    bail!("Cannot tail a view that has not been materialized.");
                }
                let sink_name = format!(
                    "tail-source-{}",
                    self.catalog
                        .humanize_id(Id::Global(source_id))
                        .expect("Source id is known to exist in catalog")
                );
                let sink_id = self.catalog.allocate_id();
                self.active_tails.insert(conn_id, sink_id);
                let (tx, rx) = self.switchboard.mpsc_limited(self.num_timely_workers);
                let since = self
                    .upper_of(&source_id)
                    .expect("name missing at coordinator")
                    .get(0)
                    .copied()
                    .unwrap_or(Timestamp::max_value());
                let sink = Sink {
                    from: (source_id, source.desc()?.clone()),
                    connector: SinkConnector::Tail(TailSinkConnector { tx, since }),
                };
                self.create_sink_dataflow(sink_name, sink_id, sink);
                ExecuteResponse::Tailing { rx }
            }

            Plan::SendRows(rows) => send_immediate_rows(rows),

            Plan::ExplainPlan(mut relation_expr, mut eval_env) => {
                eval_env.wall_time = Some(chrono::Utc::now());
                self.optimizer.optimize(&mut relation_expr, &eval_env);
                let pretty = relation_expr.pretty_humanized(&self.catalog);
                let rows = vec![Row::pack(&[Datum::from(&*pretty)])];
                send_immediate_rows(rows)
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

                match kind {
                    MutationKind::Delete => ExecuteResponse::Deleted(affected_rows),
                    MutationKind::Insert => ExecuteResponse::Inserted(affected_rows),
                    MutationKind::Update => ExecuteResponse::Updated(affected_rows),
                }
            }
        })
    }

    /// Register sources as described by `sources`.
    ///
    /// This method installs descriptions of each source in `sources` in the
    /// coordinator, so that they can be recovered when used by name in views.
    fn register_source(
        &mut self,
        name: &QualName,
        source: &dataflow_types::Source,
    ) -> Result<GlobalId, failure::Error> {
        let id = self
            .catalog
            .insert(name.clone(), CatalogItem::Source(source.clone()))?;
        self.report_catalog_update(
            id,
            self.catalog.humanize_id(expr::Id::Global(id)).unwrap(),
            true,
        );
        Ok(id)
    }

    fn register_view(
        &mut self,
        name: &QualName,
        view: &dataflow_types::View,
    ) -> Result<GlobalId, failure::Error> {
        let id = self
            .catalog
            .insert(name.clone(), CatalogItem::View(view.clone()))?;
        self.report_catalog_update(
            id,
            self.catalog.humanize_id(expr::Id::Global(id)).unwrap(),
            true,
        );
        Ok(id)
    }

    fn register_index(
        &mut self,
        name: &QualName,
        index: &dataflow_types::Index,
    ) -> Result<GlobalId, failure::Error> {
        let id = self
            .catalog
            .insert(name.clone(), CatalogItem::Index(index.clone()))?;
        self.report_catalog_update(
            id,
            self.catalog.humanize_id(expr::Id::Global(id)).unwrap(),
            true,
        );
        Ok(id)
    }

    fn import_source_or_view(&mut self, id: &GlobalId, dataflow: &mut DataflowDesc) {
        if dataflow.objects_to_build.iter().any(|bd| &bd.id == id)
            || dataflow.source_imports.iter().any(|(i, _)| i == id)
        {
            return;
        }
        if let Some(source) = self.sources.get(id) {
            dataflow.add_source_import(*id, source.clone());
        } else {
            let view_item = self.catalog.get_by_id(id).item().clone();
            match view_item {
                CatalogItem::View(view) => {
                    if let Some(materialization_state) = &self.views[id].materialization {
                        let keys = materialization_state.get_primary_key();
                        let index_desc = IndexDesc {
                            on_id: *id,
                            keys: keys.to_vec(),
                        };
                        dataflow.add_index_import(
                            self.indexes[&index_desc].1,
                            index_desc,
                            view.desc.typ().clone(),
                            *id,
                        );
                    } else {
                        self.build_view_collection(id, &view, dataflow);
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    fn build_view_collection(
        &mut self,
        view_id: &GlobalId,
        view: &dataflow_types::View,
        dataflow: &mut DataflowDesc,
    ) {
        let mut view_to_build = view.clone();
        // Collect names of sources used
        view_to_build.relation_expr.visit(&mut |e| {
            if let RelationExpr::Get {
                id: Id::Global(id),
                typ: _,
            } = e
            {
                self.import_source_or_view(id, dataflow);
                dataflow.add_dependency(*view_id, *id)
            }
        });
        self.optimizer
            .optimize(&mut view_to_build.relation_expr, &view_to_build.eval_env);
        // TODO (wangandi): Add indexes required by the view according to the optimizer
        // to dataflow.dependent_indexes
        dataflow.add_view_to_build(*view_id, view_to_build);
    }

    fn build_arrangement(
        &mut self,
        id: &GlobalId,
        index: dataflow_types::Index,
        mut dataflow: DataflowDesc,
    ) {
        self.import_source_or_view(&index.desc.on_id, &mut dataflow);
        dataflow.add_index_to_build(*id, index.clone());
        dataflow.add_index_export(*id, index.desc.clone(), index.relation_type.clone());
        // TODO: should we still support creating multiple dataflows with a single command,
        // Or should it all be compacted into a single DataflowDesc with multiple exports?
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::CreateDataflows(vec![dataflow]),
        );
        self.add_index_to_view(*id, index.desc, None);
    }

    fn create_materialized_view_dataflow(
        &mut self,
        view_name: String,
        id: GlobalId,
        view: dataflow_types::View,
        as_of: Option<Vec<Timestamp>>,
    ) -> Result<(), failure::Error> {
        self.insert_view(id, view.clone(), None);
        let mut dataflow = DataflowDesc::new(view_name.clone());
        self.build_view_collection(&id, &view, &mut dataflow);
        let index_name = QualName::from_str(&view_name)?.with_trailing_string("_PRIMARY_IDX");
        let index = view.auto_generate_primary_idx(id);
        let index_id = if as_of.is_some() {
            self.catalog.allocate_id()
        } else {
            self.register_index(&index_name, &index)?
        };
        dataflow.as_of(as_of);
        self.build_arrangement(&index_id, index, dataflow);
        Ok(())
    }

    fn create_index_dataflow(&mut self, name: String, id: GlobalId, index: dataflow_types::Index) {
        self.index_aliases.insert(id, index.desc.clone());

        if let Some((count, _id)) = self.indexes.get_mut(&index.desc) {
            // just increment the count. no need to build a duplicate index
            *count += 1;
            return;
        }

        let dataflow = DataflowDesc::new(name);
        self.build_arrangement(&id, index, dataflow);
    }

    fn create_sink_dataflow(&mut self, name: String, id: GlobalId, sink: dataflow_types::Sink) {
        let mut dataflow = DataflowDesc::new(name);
        self.import_source_or_view(&sink.from.0, &mut dataflow);
        dataflow.add_sink_export(id, sink);
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::CreateDataflows(vec![dataflow]),
        );
    }

    pub fn drop_views(&mut self, views_names: Vec<GlobalId>) {
        let mut index_names = Vec::new();
        for name in views_names.iter() {
            index_names.push(self.remove_view(name));
        }
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::DropViews(views_names, index_names),
        )
    }

    pub fn drop_sinks(&mut self, dataflow_names: Vec<GlobalId>) {
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::DropSinks(dataflow_names),
        )
    }

    pub fn drop_indexes(&mut self, dataflow_names: Vec<GlobalId>, cascaded: bool) {
        let mut trace_keys = Vec::new();
        for name in dataflow_names {
            if let Some(trace_key) = self.index_aliases.remove(&name) {
                if cascaded {
                    // the underlying indexes will be removed when the dependent
                    // view is removed. No need to signal the server
                    self.indexes.remove(&trace_key);
                } else {
                    let (count, _id) = self.indexes.get_mut(&trace_key).unwrap();
                    if count == &1 {
                        self.indexes.remove(&trace_key);
                        trace_keys.push(trace_key);
                    } else {
                        *count -= 1;
                    }
                }
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

        // First determine the candidate timestamp, which is either the explicitly requested
        // timestamp, or the latest timestamp known to be immediately available.
        let timestamp = match when {
            // Explicitly requested timestamps should be respected.
            PeekWhen::AtTimestamp(timestamp) => timestamp,

            // These two strategies vary in terms of which traces drive the
            // timestamp determination process: either the trace itself or the
            // original sources on which they depend.
            PeekWhen::EarliestSource | PeekWhen::Immediately => {
                // Collect global identifiers in `source`.
                let mut sources = HashSet::new();
                let mut reached = HashSet::new();

                match when {
                    PeekWhen::EarliestSource => {
                        for id in uses_ids.iter() {
                            self.sources_frontier(*id, &mut sources, &mut reached);
                        }
                    }
                    PeekWhen::Immediately => {
                        for id in uses_ids.iter().cloned() {
                            sources.insert(id);
                        }
                    }
                    _ => unreachable!(),
                }

                // Form lower bound on available times.
                let mut upper = Antichain::new();
                for id in sources {
                    if let Some(view_upper) = self.upper_of(&id) {
                        // To track the meet of `upper` we just extend with the upper frontier.
                        upper.extend(view_upper.iter().cloned());
                    } else {
                        bail!(
                            "{} is not materialized",
                            self.catalog.humanize_id(expr::Id::Global(id)).unwrap()
                        )
                    }
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

    /// Collects frontiers from the earliest views.
    ///
    /// This method recursively traverses views and discovers other views on which
    /// they depend, collecting the frontiers of views that depend directly on sources.
    /// The `reached` input allows us to deduplicate views, and avoid e.g. recursion.
    fn sources_frontier(
        &self,
        id: GlobalId,
        sources: &mut HashSet<GlobalId>,
        reached: &mut HashSet<GlobalId>,
    ) {
        reached.insert(id);
        if let Some(view) = self.views.get(&id) {
            if view.depends_on_source {
                sources.insert(id);
            } else {
                for id in view.uses.iter() {
                    if !reached.contains(id) {
                        self.sources_frontier(*id, sources, reached);
                    }
                }
            }
        }
    }

    /// Updates the upper frontier of a named view.
    pub fn update_upper(&mut self, name: &GlobalId, mut changes: ChangeBatch<Timestamp>) {
        if let Some(entry) = self.views.get_mut(name) {
            if let Some(materialization_state) = entry.materialization.as_mut() {
                let changes: Vec<_> = materialization_state
                    .upper
                    .update_iter(changes.drain())
                    .collect();
                if !changes.is_empty() {
                    if self.log {
                        for (time, change) in changes {
                            // Rather than use the view's GlobalId, we should use that of its representatives.
                            for keys in materialization_state.primary_idx_keys.iter() {
                                // Fetch the representative global id for each of these keys.
                                let index_desc = IndexDesc {
                                    on_id: *name,
                                    keys: keys.to_vec(),
                                };
                                let representative_id = self.indexes[&index_desc].1;
                                broadcast(
                                    &mut self.broadcast_tx,
                                    SequencedCommand::AppendLog(MaterializedEvent::Frontier(
                                        representative_id,
                                        time,
                                        change,
                                    )),
                                );
                            }
                        }
                    }

                    // Advance the compaction frontier to trail the new frontier.
                    // If the compaction latency is `None` compaction messages are
                    // not emitted, and the trace should be broadly useable.
                    // TODO: If the frontier advances surprisingly quickly, e.g. in
                    // the case of a constant collection, this compaction is actively
                    // harmful. We should reconsider compaction policy with an eye
                    // towards minimizing unexpected screw-ups.
                    if let Some(compaction_latency_ms) = materialization_state.compaction_latency_ms
                    {
                        let mut since = Antichain::new();
                        for time in materialization_state.upper.frontier().iter() {
                            since.insert(time.saturating_sub(compaction_latency_ms));
                        }
                        self.since_updates
                            .push((name.clone(), since.elements().to_vec()));
                    }
                }
            }
        }
    }

    /// The upper frontier of a maintained view, if it exists.
    fn upper_of(&self, name: &GlobalId) -> Option<AntichainRef<Timestamp>> {
        if let Some(view_state) = self.views.get(name).as_mut() {
            view_state
                .materialization
                .as_ref()
                .map(|m| m.upper.frontier())
        } else {
            None
        }
    }

    /// Updates the since frontier of a named view.
    ///
    /// This frontier tracks compaction frontier, and represents a lower bound on times for
    /// which the associated trace is certain to produce valid results. For times greater
    /// or equal to some element of the since frontier the accumulation will be correct,
    /// and for other times no such guarantee holds.
    #[allow(dead_code)]
    fn update_since(&mut self, name: &GlobalId, since: &[Timestamp]) {
        if let Some(entry) = self.views.get_mut(name) {
            if let Some(materialization_state) = entry.materialization.as_mut() {
                materialization_state.since.clear();
                materialization_state.since.extend(since.iter().cloned());
            }
        }
    }

    /// The since frontier of a maintained view, if it exists.
    #[allow(dead_code)]
    fn since_of(&self, name: &GlobalId) -> Option<&Antichain<Timestamp>> {
        if let Some(view_state) = self.views.get(name) {
            view_state.materialization.as_ref().map(|m| &m.since)
        } else {
            None
        }
    }

    /// Inserts a view into the coordinator.
    ///
    /// Initializes managed state and logs the insertion (and removal of any existing view).
    fn insert_view(
        &mut self,
        view_id: GlobalId,
        view: dataflow_types::View,
        known_source_contain: Option<bool>,
    ) {
        let contains_sources = if let Some(contains_sources) = known_source_contain {
            contains_sources
        } else {
            let mut contains_sources = false;
            view.relation_expr.visit(&mut |e| {
                // Some `Get` expressions are for let bindings, and should not be loaded.
                // We might want explicitly enumerate assets to import.
                if let RelationExpr::Get {
                    id: Id::Global(id),
                    typ: _,
                } = e
                {
                    contains_sources |= match self.catalog.get_by_id(id).item() {
                        CatalogItem::Source(_source) => true,
                        _ => false,
                    };
                }
            });
            contains_sources
        };
        self.remove_view(&view_id);
        let mut viewstate = ViewState::from_view(&view);
        viewstate.depends_on_source = contains_sources;
        self.views.insert(view_id, viewstate);
    }

    /// Add an index to a view in the coordinator.
    fn add_index_to_view(&mut self, id: GlobalId, desc: IndexDesc, latency_ms: Option<Timestamp>) {
        if let Some(viewstate) = self.views.get_mut(&desc.on_id) {
            match viewstate.materialization.as_mut() {
                Some(materialization_state) => {
                    materialization_state.add_index(&desc.keys);
                }
                None => {
                    let mut materialization_state =
                        MaterializationState::new(self.num_timely_workers, &desc.keys);
                    if latency_ms.is_some() {
                        materialization_state.set_compaction_latency(latency_ms);
                    }
                    viewstate.materialization = Some(materialization_state);
                }
            }
            if self.log {
                if let Some(materialization_state) = &viewstate.materialization {
                    for time in materialization_state.upper.frontier().iter() {
                        broadcast(
                            &mut self.broadcast_tx,
                            SequencedCommand::AppendLog(MaterializedEvent::Frontier(
                                id,
                                time.clone(),
                                1,
                            )),
                        );
                    }
                } else {
                    unreachable!()
                }
            }
            self.index_aliases.insert(id, desc.clone());
            self.indexes.insert(desc, (1, id));
        } else {
            unreachable!()
        };
    }

    /// Removes a view from the coordinator.
    ///
    /// Removes the managed state and logs the removal.
    fn remove_view(&mut self, name: &GlobalId) -> Vec<GlobalId> {
        let mut dropped_index_ids = Vec::new();
        if let Some(view_state) = self.views.remove(name) {
            if self.log {
                if let Some(materialization_state) = view_state.materialization {
                    for time in materialization_state.upper.frontier().iter() {
                        for keys in materialization_state.primary_idx_keys.iter() {
                            // Fetch the representative global id for each of these keys.
                            let index_desc = IndexDesc {
                                on_id: *name,
                                keys: keys.to_vec(),
                            };
                            let representative_id = self.indexes[&index_desc].1;
                            broadcast(
                                &mut self.broadcast_tx,
                                SequencedCommand::AppendLog(MaterializedEvent::Frontier(
                                    representative_id,
                                    time.clone(),
                                    -1,
                                )),
                            );
                        }
                    }
                    for key in materialization_state.get_all_idx_keys() {
                        dropped_index_ids.push(
                            self.indexes[&IndexDesc {
                                on_id: *name,
                                keys: key.to_vec(),
                            }]
                                .1,
                        );
                    }
                }
            }
        }
        dropped_index_ids
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
                    block_on(postgres.execute(&self.catalog, &stmt))
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

pub struct MaterializationState {
    // TODO(andiwang): Change all `primary_idx_keys` to a single Vec<ScalarExpr> so that only one
    // primary index is allowed?
    /// Currently all indexes are primary indexes
    primary_idx_keys: Vec<Vec<ScalarExpr>>,
    /// No secondary indexes exist yet
    // secondary_idxes: Vec<Vec<ScalarExpr>>,
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

impl MaterializationState {
    /// Creates an empty view state from a number of workers.
    pub fn new(workers: usize, primary_idx: &[ScalarExpr]) -> Self {
        let mut upper = MutableAntichain::new();
        upper.update_iter(Some((0, workers as i64)));
        Self {
            primary_idx_keys: vec![primary_idx.to_owned()],
            //secondary_idxes: Vec::new(),
            upper,
            since: Antichain::from_elem(0),
            compaction_latency_ms: Some(60_000),
        }
    }

    pub fn add_index(&mut self, primary_idx: &[ScalarExpr]) {
        self.primary_idx_keys.push(primary_idx.to_owned());
    }

    pub fn get_primary_key(&self) -> &Vec<ScalarExpr> {
        &self.primary_idx_keys[0]
    }

    pub fn get_all_idx_keys(&self) -> &Vec<Vec<ScalarExpr>> {
        &self.primary_idx_keys
    }

    /// Sets the latency behind the collection frontier at which compaction occurs.
    pub fn set_compaction_latency(&mut self, latency_ms: Option<Timestamp>) {
        self.compaction_latency_ms = latency_ms;
    }
}

/// Per-view state.
pub struct ViewState {
    /// Names of views on which this view depends.
    uses: Vec<GlobalId>,
    /// True if the dataflow defining the view may depend on a source, which
    /// leads us to include the frontier of the view in timestamp selection,
    /// as the view cannot be expected to advance its frontier simply because
    /// its input views advance.
    depends_on_source: bool,
    // TODO(andiwang): make views not necessarily materialized
    /// None if not materialized
    materialization: Option<MaterializationState>,
}

impl Default for ViewState {
    fn default() -> Self {
        ViewState {
            uses: Vec::new(),
            depends_on_source: true,
            materialization: None,
        }
    }
}

impl ViewState {
    /// Creates view state from a view, and number of workers.
    pub fn from_view(view: &View) -> Self {
        let mut view_state = Self::default();
        let mut out = Vec::new();
        view.relation_expr.global_uses(&mut out);
        out.sort();
        out.dedup();
        view_state.uses = out;
        view_state
    }
}
