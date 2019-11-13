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
use std::convert::TryFrom;
use std::iter;
use std::path::Path;
use std::thread::{self, JoinHandle};

use failure::bail;
use futures::{sink, stream, Future, Sink as FuturesSink, Stream};
use timely::progress::frontier::{Antichain, AntichainRef, MutableAntichain};
use timely::progress::ChangeBatch;
use uuid::Uuid;

use dataflow::logging::materialized::MaterializedEvent;
use dataflow::{SequencedCommand, WorkerFeedback, WorkerFeedbackWithMeta};
use dataflow_types::logging::LoggingConfig;
use dataflow_types::{
    compare_columns, DataflowDesc, PeekResponse, PeekWhen, Sink, SinkConnector, Source,
    SourceConnector, TailSinkConnector, Timestamp, Update, View,
};
use expr::RelationExpr;
use ore::collections::CollectionExt;
use ore::future::FutureExt;
use repr::{Datum, RelationDesc, Row, RowPacker, RowUnpacker};
use sql::store::{Catalog, CatalogItem};
use sql::PreparedStatement;
use sql::{MutationKind, ObjectType, Plan, Session};
use symbiosis::Postgres;

use crate::{Command, ExecuteResponse, Response};

enum Message {
    Command(Command),
    Worker(WorkerFeedbackWithMeta),
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
    pub cmd_rx: futures::sync::mpsc::UnboundedReceiver<Command>,
}

pub fn serve<'a, C>(config: Config<'a, C>) -> Result<JoinHandle<()>, failure::Error>
where
    C: comm::Connection,
{
    let mut coord = Coordinator::new(
        config.switchboard.clone(),
        config.num_timely_workers,
        config.logging,
        config.symbiosis_url.is_some(),
    );

    let mut catalog = sql::store::Catalog::new(config.logging);

    let mut postgres = if let Some(symbiosis_url) = config.symbiosis_url {
        Some(symbiosis::Postgres::open_and_erase(symbiosis_url)?)
    } else {
        None
    };

    {
        // Per https://github.com/MaterializeInc/materialize/blob/5d85615ba8608f4f6d7a8a6a676c19bb2b37db55/src/pgwire/lib.rs#L52,
        // the first connection ID used is 1. As long as that remains the case,
        // 0 is safe to use here.
        let conn_id = 0;
        let mut session = sql::Session::default();
        for stmt in sql::parse(config.bootstrap_sql)? {
            handle_statement(
                &mut coord,
                postgres.as_mut(),
                &mut catalog,
                &mut session,
                stmt,
                None,
                conn_id,
            )?;
        }
    }

    let feedback_rx = coord.enable_feedback();
    // NOTE: returning an error from this point on (i.e., after calling
    // `coord.enable_feedback()`) will be fatal, as dropping `feedback_rx` will
    // cause the worker threads to panic.
    let messages = config
        .cmd_rx
        .map(Message::Command)
        .map_err(|()| unreachable!())
        .chain(stream::once(Ok(Message::Shutdown)))
        .select(feedback_rx.map(Message::Worker));

    Ok(thread::spawn(move || {
        let mut messages = messages.wait();
        for msg in messages.by_ref() {
            match msg.expect("coordinator message receiver failed") {
                Message::Command(Command::Execute {
                    portal_name,
                    mut session,
                    conn_id,
                    tx,
                }) => {
                    let result = handle_execute(
                        &mut coord,
                        postgres.as_mut(),
                        &mut catalog,
                        &mut session,
                        portal_name,
                        conn_id,
                    );
                    let _ = tx.send(Response { result, session });
                }

                Message::Command(Command::Parse {
                    name,
                    sql,
                    mut session,
                    tx,
                }) => {
                    let result = handle_parse(postgres.as_mut(), &catalog, &mut session, name, sql);
                    let _ = tx.send(Response { result, session });
                }

                Message::Command(Command::CancelRequest { conn_id }) => {
                    coord.sequence_cancel(conn_id);
                }

                Message::Shutdown => {
                    coord.shutdown();
                    break;
                }

                Message::Worker(WorkerFeedbackWithMeta {
                    worker_id: _,
                    message: WorkerFeedback::FrontierUppers(updates),
                }) => {
                    for (name, changes) in updates {
                        coord.update_upper(&name, changes);
                    }
                    coord.maintenance();
                }
            }
        }

        // Cleanly drain any pending messages from the worker before shutting
        // down.
        for msg in messages {
            match msg.expect("coordinator message receiver failed") {
                Message::Command(_) | Message::Shutdown => unreachable!(),
                Message::Worker(_) => (),
            }
        }
    }))
}

fn apply_plan(
    catalog: &mut Catalog,
    session: &mut Session,
    plan: &Plan,
) -> Result<(), failure::Error> {
    match plan {
        Plan::CreateView(view) => catalog.insert(CatalogItem::View(view.clone()))?,
        Plan::CreateTable { name, desc } => catalog.insert(CatalogItem::Source(Source {
            name: name.clone(),
            connector: SourceConnector::Local,
            desc: desc.clone(),
        }))?,
        Plan::CreateSource(source) => catalog.insert(CatalogItem::Source(source.clone()))?,
        Plan::CreateSources(sources) => {
            for source in sources {
                catalog.insert(CatalogItem::Source(source.clone()))?
            }
        }
        Plan::CreateSink(sink) => catalog.insert(CatalogItem::Sink(sink.clone()))?,
        Plan::CreateIndex(index) => catalog.insert(CatalogItem::Index(index.clone()))?,
        Plan::DropItems(names, _item_type) => {
            for name in names {
                catalog.remove(name);
            }
        }
        Plan::SetVariable { name, value } => session.set(name, value)?,
        Plan::StartTransaction => session.start_transaction(),
        Plan::Commit | Plan::Rollback => session.end_transaction(),
        _ => (),
    }
    Ok(())
}

fn handle_statement<C>(
    coord: &mut Coordinator<C>,
    mut postgres: Option<&mut Postgres>,
    catalog: &mut Catalog,
    session: &mut Session,
    stmt: sql::Statement,
    portal_name: Option<String>,
    conn_id: u32,
) -> Result<ExecuteResponse, failure::Error>
where
    C: comm::Connection,
{
    sql::plan(catalog, session, stmt.clone(), portal_name)
        .or_else(|err| {
            // Executing the query failed. If we're running in symbiosis with
            // Postgres, see if Postgres can handle it.
            match postgres {
                Some(ref mut postgres) if postgres.can_handle(&stmt) => postgres.execute(&stmt),
                _ => Err(err),
            }
        })
        .and_then(|plan| {
            apply_plan(catalog, session, &plan)?;
            Ok(plan)
        })
        .map(|plan| coord.sequence_plan(plan, conn_id))
}

fn handle_execute<C>(
    coord: &mut Coordinator<C>,
    postgres: Option<&mut Postgres>,
    catalog: &mut Catalog,
    session: &mut Session,
    portal_name: String,
    conn_id: u32,
) -> Result<ExecuteResponse, failure::Error>
where
    C: comm::Connection,
{
    let portal = session
        .get_portal(&portal_name)
        .ok_or_else(|| failure::format_err!("portal does not exist {:?}", portal_name))?;
    let prepared = session
        .get_prepared_statement(&portal.statement_name)
        .ok_or_else(|| {
            failure::format_err!(
                "statement for portal does not exist portal={:?} statement={:?}",
                portal_name,
                portal.statement_name
            )
        })?;
    match prepared.sql() {
        Some(stmt) => {
            let stmt = stmt.clone();
            handle_statement(
                coord,
                postgres,
                catalog,
                session,
                stmt,
                Some(portal_name),
                conn_id,
            )
        }
        None => Ok(ExecuteResponse::EmptyQuery),
    }
}

fn handle_parse(
    postgres: Option<&mut Postgres>,
    catalog: &Catalog,
    session: &mut Session,
    name: String,
    sql: String,
) -> Result<(), failure::Error> {
    let stmts = sql::parse(sql)?;
    let (stmt, desc, param_types) = match stmts.len() {
        0 => (None, None, vec![]),
        1 => {
            let stmt = stmts.into_element();
            let (desc, param_types) = match sql::describe(catalog, stmt.clone()) {
                Ok((desc, param_types)) => (desc, param_types),
                // Describing the query failed. If we're running in symbiosis with
                // Postgres, see if Postgres can handle it. Note that Postgres
                // only handles commands that do not return rows, so the
                // `RelationDesc` is always `None`.
                Err(err) => match postgres {
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

/// Glues the external world to the Timely workers.
pub struct Coordinator<C>
where
    C: comm::Connection,
{
    switchboard: comm::Switchboard<C>,
    broadcast_tx: sink::Wait<comm::broadcast::Sender<SequencedCommand>>,
    num_timely_workers: u64,
    optimizer: expr::transform::Optimizer,
    views: HashMap<String, ViewState>,
    /// Each source name maps to a source and re-written name (for auto-created views).
    sources: HashMap<String, (dataflow_types::Source, Option<String>)>,
    /// Maps user-defined indexes by collection + key and how many aliases it has
    indexes: HashMap<(String, Vec<usize>), usize>,
    /// Maps user-defined index names to their collection + key
    index_aliases: HashMap<String, (String, Vec<usize>)>,
    since_updates: Vec<(String, Vec<Timestamp>)>,
    /// For each connection running a TAIL command, the name of the dataflow
    /// that is servicing the TAIL. A connection can only run one TAIL at a
    /// time.
    active_tails: HashMap<u32, String>,
    local_input_time: Timestamp,
    log: bool,
    symbiosis: bool,
}

impl<C> Coordinator<C>
where
    C: comm::Connection,
{
    pub fn new(
        switchboard: comm::Switchboard<C>,
        num_timely_workers: usize,
        logging_config: Option<&LoggingConfig>,
        symbiosis: bool,
    ) -> Self {
        let broadcast_tx = switchboard.broadcast_tx(dataflow::BroadcastToken).wait();

        let mut coordinator = Self {
            switchboard,
            broadcast_tx,
            num_timely_workers: u64::try_from(num_timely_workers).unwrap(),
            optimizer: Default::default(),
            views: HashMap::new(),
            sources: HashMap::new(),
            indexes: HashMap::new(),
            index_aliases: HashMap::new(),
            since_updates: Vec::new(),
            active_tails: HashMap::new(),
            local_input_time: 1,
            log: logging_config.is_some(),
            symbiosis,
        };

        if let Some(logging_config) = logging_config {
            for log in logging_config.active_logs().iter() {
                // Insert with 1 second compaction latency.
                coordinator.insert_source(log.name(), 1_000);
            }
        }

        coordinator
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

    pub fn sequence_plan(&mut self, plan: Plan, conn_id: u32) -> ExecuteResponse {
        match plan {
            Plan::CreateTable { name, desc } => {
                self.create_sources(vec![Source {
                    name,
                    connector: SourceConnector::Local,
                    desc,
                }]);
                ExecuteResponse::CreatedTable
            }

            Plan::CreateSource(source) => {
                let sources = vec![source];
                self.create_sources(sources);
                ExecuteResponse::CreatedSource
            }

            Plan::CreateSources(sources) => {
                self.create_sources(sources.clone());
                send_immediate_rows(
                    sources
                        .iter()
                        .map(|s| Row::pack(&[Datum::String(&s.name)]))
                        .collect(),
                )
            }

            Plan::CreateSink(sink) => {
                self.create_dataflows(vec![DataflowDesc::from(sink)]);
                ExecuteResponse::CreatedSink
            }

            Plan::CreateView(view) => {
                self.create_dataflows(vec![DataflowDesc::from(view)]);
                ExecuteResponse::CreatedView
            }

            Plan::CreateIndex(index) => {
                self.create_index(index);
                ExecuteResponse::CreatedIndex
            }

            Plan::DropItems(names, item_type) => {
                let mut sources_to_drop = Vec::new();
                let mut views_to_drop = Vec::new();
                let mut indexes_to_drop = Vec::new();
                for name in names {
                    // TODO: Test if a name was installed and error if not?
                    if let Some((_source, new_name)) = self.sources.remove(&name) {
                        sources_to_drop.push(name.clone());
                        if let Some(name) = new_name {
                            views_to_drop.push(name);
                        } else {
                            panic!("Attempting to drop an uninstalled source: {}", name);
                        }
                    } else if self.views.contains_key(&name) {
                        views_to_drop.push(name);
                    } else {
                        indexes_to_drop.push(name);
                    }
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

                if !indexes_to_drop.is_empty() {
                    self.drop_indexes(indexes_to_drop, item_type != ObjectType::Index);
                }

                match item_type {
                    ObjectType::Source => ExecuteResponse::DroppedSource,
                    ObjectType::View => ExecuteResponse::DroppedView,
                    ObjectType::Table => ExecuteResponse::DroppedTable,
                    ObjectType::Index => ExecuteResponse::DroppedIndex,
                    ObjectType::Sink => unreachable!(),
                }
            }

            Plan::EmptyQuery => ExecuteResponse::EmptyQuery,

            Plan::SetVariable { name, .. } => ExecuteResponse::SetVariable { name },

            Plan::StartTransaction => ExecuteResponse::StartTransaction,
            Plan::Commit => ExecuteResponse::Commit,
            Plan::Rollback => ExecuteResponse::Rollback,

            Plan::Peek {
                mut source,
                when,
                finishing,
            } => {
                // Peeks describe a source of data and a timestamp at which to view its contents.
                //
                // We need to determine both an appropriate timestamp from the description, and
                // also to ensure that there is a view in place to query, if the source of data
                // for the peek is not a base relation.

                self.optimizer.optimize(&mut source);

                let (rows_tx, rows_rx) = self.switchboard.mpsc_limited(self.num_timely_workers);

                // Choose a timestamp for all workers to use in the peek.
                // We minimize over all participating views, to ensure that the query will not
                // need to block on the arrival of further input data.
                let timestamp = self.determine_timestamp(&source, when);

                let (project, filter) = Self::plan_peek(&mut source);

                // Create a transient view if the peek is not of a base relation.
                if let RelationExpr::Get { name, typ: _ } = source {
                    // If `name` is a source, we'll need to rename it.
                    let mut name = name.clone();
                    if let Some((_source, rename)) = self.sources.get(&name) {
                        if let Some(rename) = rename {
                            name = rename.clone();
                        }
                    }

                    // Fast path. We can just look at the existing dataflow directly.
                    broadcast(
                        &mut self.broadcast_tx,
                        SequencedCommand::Peek {
                            name,
                            conn_id,
                            tx: rows_tx,
                            timestamp,
                            finishing: finishing.clone(),
                            project,
                            filter,
                        },
                    );
                } else {
                    // Slow path. We need to perform some computation, so build
                    // a new transient dataflow that will be dropped after the
                    // peek completes.
                    let name = format!("<peek_{}>", Uuid::new_v4());
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
                    let desc =
                        RelationDesc::new(typ, iter::repeat::<Option<String>>(None).take(ncols));
                    let dataflow = DataflowDesc::from(View {
                        name: name.clone(),
                        raw_sql: "<none>".into(),
                        relation_expr: source,
                        desc,
                    })
                    .as_of(Some(vec![timestamp.clone()]));

                    self.create_dataflows(vec![dataflow]);
                    broadcast(
                        &mut self.broadcast_tx,
                        SequencedCommand::Peek {
                            name: name.clone(),
                            conn_id,
                            tx: rows_tx,
                            timestamp,
                            finishing: finishing.clone(),
                            project: None,
                            filter: Vec::new(),
                        },
                    );
                    broadcast(
                        &mut self.broadcast_tx,
                        SequencedCommand::DropViews(vec![name]),
                    );
                }

                let rows_rx = rows_rx
                    .fold(PeekResponse::Rows(vec![]), |memo, resp| {
                        match (memo, resp) {
                            (PeekResponse::Rows(mut memo), PeekResponse::Rows(rows)) => {
                                memo.extend(rows);
                                let out: Result<_, bincode::Error> = Ok(PeekResponse::Rows(memo));
                                out
                            }
                            _ => Ok(PeekResponse::Canceled),
                        }
                    })
                    .map(move |mut resp| {
                        if let PeekResponse::Rows(rows) = &mut resp {
                            let mut left_unpacker = RowUnpacker::new();
                            let mut right_unpacker = RowUnpacker::new();
                            let mut sort_by = |left: &Row, right: &Row| {
                                compare_columns(
                                    &finishing.order_by,
                                    &left_unpacker.unpack(left),
                                    &right_unpacker.unpack(right),
                                )
                            };
                            let offset = finishing.offset;
                            if offset > rows.len() {
                                *rows = Vec::new();
                            } else {
                                if let Some(limit) = finishing.limit {
                                    let offset_plus_limit = offset + limit;
                                    if rows.len() > offset_plus_limit {
                                        pdqselect::select_by(rows, offset_plus_limit, &mut sort_by);
                                        rows.truncate(offset_plus_limit);
                                    }
                                }
                                if offset > 0 {
                                    pdqselect::select_by(rows, offset, &mut sort_by);
                                    rows.drain(..offset);
                                }
                                rows.sort_by(&mut sort_by);
                                let mut unpacker = RowUnpacker::new();
                                let mut packer = RowPacker::new();
                                for row in rows {
                                    let datums = unpacker.unpack(&*row);
                                    let new_row =
                                        packer.pack(finishing.project.iter().map(|i| datums[*i]));
                                    drop(datums);
                                    *row = new_row;
                                }
                            }
                        }
                        resp
                    })
                    .from_err()
                    .boxed();

                ExecuteResponse::SendRows(rows_rx)
            }

            Plan::Tail(source) => {
                let mut source_name = source.name().to_string();
                if let Some((_source, rename)) = self.sources.get(&source_name) {
                    if let Some(rename) = rename {
                        source_name = rename.clone();
                    }
                }

                let name = format!("<tail_{}>", Uuid::new_v4());
                self.active_tails.insert(conn_id, name.clone());
                let (tx, rx) = self.switchboard.mpsc_limited(self.num_timely_workers);
                let since = self
                    .upper_of(&source_name)
                    .expect("name missing at coordinator")
                    .get(0)
                    .copied()
                    .unwrap_or(Timestamp::max_value());
                broadcast(
                    &mut self.broadcast_tx,
                    SequencedCommand::CreateDataflows(vec![DataflowDesc::from(Sink {
                        name,
                        from: (source_name, source.desc().clone()),
                        connector: SinkConnector::Tail(TailSinkConnector { tx, since }),
                    })]),
                );

                ExecuteResponse::Tailing { rx }
            }

            Plan::SendRows(rows) => send_immediate_rows(rows),

            Plan::ExplainPlan(mut relation_expr) => {
                self.optimizer.optimize(&mut relation_expr);
                let pretty = relation_expr.pretty();
                let rows = vec![Row::pack(vec![Datum::from(&*pretty)])];
                send_immediate_rows(rows)
            }

            Plan::SendDiffs {
                name,
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

                broadcast(
                    &mut self.broadcast_tx,
                    SequencedCommand::Insert { name, updates },
                );

                self.local_input_time += 1;

                let local_inputs = self
                    .sources
                    .iter()
                    .filter_map(|(name, (src, _view))| match src.connector {
                        SourceConnector::Local => Some(name.as_str()),
                        _ => None,
                    });

                for name in local_inputs {
                    broadcast(
                        &mut self.broadcast_tx,
                        SequencedCommand::AdvanceTime {
                            name: name.into(),
                            to: self.local_input_time,
                        },
                    );
                }

                match kind {
                    MutationKind::Delete => ExecuteResponse::Deleted(affected_rows),
                    MutationKind::Insert => ExecuteResponse::Inserted(affected_rows),
                    MutationKind::Update => ExecuteResponse::Updated(affected_rows),
                }
            }
        }
    }

    /// Create sources as described by `sources`.
    ///
    /// This method installs descriptions of each source in `sources` in the
    /// coordinator, so that they can be recovered when used by name in views.
    /// The method also creates a view for each source which mirrors the contents,
    /// using the same name.
    fn create_sources(&mut self, sources: Vec<dataflow_types::Source>) {
        for source in sources.iter() {
            // Effecting policy, we mirror all sources with a view.
            let name = create_internal_source_name(&source.name);
            self.sources
                .insert(source.name.clone(), (source.clone(), Some(name)));
        }
        let mut dataflows = Vec::new();
        for source in sources.iter() {
            if let Some(rename) = &self.sources.get(&source.name).unwrap().1 {
                dataflows.push(
                    DataflowDesc::new()
                        .add_view(View {
                            name: rename.clone(),
                            relation_expr: RelationExpr::Get {
                                name: source.name.clone(),
                                typ: source.desc.typ().clone(),
                            },
                            raw_sql: "<created by CREATE SOURCE>".to_string(),
                            desc: source.desc.clone(),
                        })
                        .add_source(source.clone()),
                );
            }
        }
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::CreateDataflows(dataflows.clone()),
        );
        for dataflow in dataflows.iter() {
            self.insert_views(dataflow);
        }
        for source in sources {
            if let SourceConnector::Local = source.connector {
                broadcast(
                    &mut self.broadcast_tx,
                    SequencedCommand::AdvanceTime {
                        name: source.name,
                        to: self.local_input_time,
                    },
                );
            }
        }
    }

    fn create_index(&mut self, mut idx: dataflow_types::Index) {
        // Rewrite the source used.
        if let Some((_source, new_name)) = self.sources.get(&idx.on_name) {
            // If the source has a corresponding view, use its name instead.
            if let Some(new_name) = new_name {
                idx.on_name = new_name.to_string();
            } else {
                panic!("create_index called on bare source");
            }
        }
        let trace_key = (idx.on_name.clone(), idx.keys.clone());
        self.index_aliases
            .insert(idx.name.clone(), trace_key.clone());

        if let Some(count) = self.indexes.get_mut(&trace_key) {
            // just increment the count. no need to build a duplicate index
            *count += 1;
            return;
        }
        self.indexes.insert(trace_key, 1);

        let dataflows = vec![DataflowDesc::from(idx)];

        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::CreateDataflows(dataflows),
        );
    }

    pub fn create_dataflows(&mut self, mut dataflows: Vec<DataflowDesc>) {
        for dataflow in dataflows.iter_mut() {
            // Check for sources before optimization, to provide a consistent
            // dependency experience. Perhaps change the order if we are ok
            // with that.
            let mut sources = Vec::new();
            for view in dataflow.views.iter_mut() {
                // Collect names of sources used, rewrite them...
                view.relation_expr.visit_mut(&mut |e| {
                    if let RelationExpr::Get { name, typ: _ } = e {
                        if let Some((_source, new_name)) = self.sources.get(name) {
                            // If the source has a corresponding view, use its name instead.
                            if let Some(new_name) = new_name {
                                *name = new_name.clone();
                            } else {
                                sources.push(name.to_string());
                            }
                        }
                    }
                })
            }
            sources.sort();
            sources.dedup();
            for name in sources {
                dataflow
                    .sources
                    .push(self.sources.get(&name).unwrap().0.clone());
            }

            // Now optimize the query expression.
            for view in dataflow.views.iter_mut() {
                self.optimizer.optimize(&mut view.relation_expr);
            }
        }
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::CreateDataflows(dataflows.clone()),
        );
        for dataflow in dataflows.iter() {
            self.insert_views(dataflow);
        }
    }

    pub fn drop_views(&mut self, dataflow_names: Vec<String>) {
        for name in dataflow_names.iter() {
            self.remove_view(name);
        }
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::DropViews(dataflow_names),
        )
    }

    pub fn drop_sinks(&mut self, dataflow_names: Vec<String>) {
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::DropSinks(dataflow_names),
        )
    }

    pub fn drop_indexes(&mut self, dataflow_names: Vec<String>, cascaded: bool) {
        let mut trace_keys = Vec::new();
        for name in dataflow_names {
            if let Some(trace_key) = self.index_aliases.remove(&name) {
                if cascaded {
                    // the underlying indexes will be removed when the dependent
                    // view is removed. No need to signal the server
                    self.indexes.remove(&trace_key);
                } else {
                    let count = self.indexes.get_mut(&trace_key).unwrap();
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

    /// Perform maintenance work associated with the coordinator.
    ///
    /// Primarily, this involves sequencing compaction commands, which should be
    /// issued whenever available.
    pub fn maintenance(&mut self) {
        // Take this opportunity to drain `since_update` commands.
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
    fn determine_timestamp(&mut self, source: &RelationExpr, when: PeekWhen) -> Timestamp {
        if self.symbiosis {
            // In symbiosis mode, we enforce serializability by forcing all
            // PEEKs to peek at the latest input time.
            return self.local_input_time - 1;
        }

        match when {
            // Explicitly requested timestamps should be respected.
            PeekWhen::AtTimestamp(timestamp) => timestamp,

            // We should produce the minimum accepted time among inputs sources that
            // `source` depends on transitively, ignoring the accepted times of
            // intermediate views.
            PeekWhen::EarliestSource | PeekWhen::Immediately => {
                // Collect unbound names in `source`.
                let mut names = Vec::new();
                source.unbound_uses(&mut names);
                names.sort();
                names.dedup();

                // Form lower bound on available times.
                let mut bound = Antichain::new();
                match when {
                    PeekWhen::EarliestSource => {
                        for name in names {
                            let mut reached = HashSet::new();
                            self.sources_frontier(name, &mut bound, &mut reached);
                        }
                    }
                    PeekWhen::Immediately => {
                        for mut name in names {
                            if let Some((_source, rename)) = &self.sources.get(name) {
                                if let Some(rename) = rename {
                                    name = &rename;
                                }
                            }
                            bound.extend(
                                self.upper_of(name)
                                    .expect("Upper bound missing")
                                    .iter()
                                    .cloned(),
                            );
                        }
                    }
                    _ => unreachable!(),
                }

                // Pick the first time strictly less than `bound` to ensure that the
                // peek can respond without further input advances.
                // TODO : the subtraction saturates to not wrap zero around, but if
                // we get this far with a zero we are at risk of a peek that may not
                // immediately return.
                if let Some(bound) = bound.elements().get(0) {
                    bound.saturating_sub(1)
                } else {
                    Timestamp::max_value()
                }
            }
        }
    }

    /// Collects frontiers from the earliest views.
    ///
    /// This method recursively traverses views and discovers other views on which
    /// they depend, collecting the frontiers of views that depend directly on sources.
    /// The `reached` input allows us to deduplicate views, and avoid e.g. recursion.
    fn sources_frontier(
        &self,
        name: &str,
        bound: &mut Antichain<Timestamp>,
        reached: &mut HashSet<String>,
    ) {
        if let Some((_source, rename)) = self.sources.get(name) {
            if let Some(rename) = rename {
                self.sources_frontier(rename, bound, reached);
            } else {
                panic!("sources_frontier called on bare source");
            }
        } else {
            reached.insert(name.to_string());
            if let Some(view) = self.views.get(name) {
                if view.depends_on_source {
                    // Views that depend on a source should propose their own frontiers,
                    // which means we need not investigate their input frontiers (which
                    // should be at least as far along).
                    let upper = self.upper_of(name).expect("Name missing at coordinator");
                    bound.extend(upper.iter().cloned());
                } else {
                    for name in view.uses.iter() {
                        if !reached.contains(name) {
                            self.sources_frontier(name, bound, reached);
                        }
                    }
                }
            }
        }
    }

    /// Updates the upper frontier of a named view.
    pub fn update_upper(&mut self, name: &str, mut changes: ChangeBatch<Timestamp>) {
        if let Some(entry) = self.views.get_mut(name) {
            let changes: Vec<_> = entry.upper.update_iter(changes.drain()).collect();
            if !changes.is_empty() {
                if self.log {
                    for (time, change) in changes {
                        broadcast(
                            &mut self.broadcast_tx,
                            SequencedCommand::AppendLog(MaterializedEvent::Frontier(
                                name.to_string(),
                                time,
                                change,
                            )),
                        );
                    }
                }

                // Advance the compaction frontier to trail the new frontier.
                // This logic assumes one-dimensional `Timestamp` time.
                let mut since = Antichain::new();
                for time in entry.upper.frontier().iter() {
                    since.insert(time.saturating_sub(entry.compaction_latency_ms));
                }
                self.since_updates
                    .push((name.to_string(), since.elements().to_vec()));
            }
        }
    }

    /// The upper frontier of a maintained view, if it exists.
    fn upper_of(&self, name: &str) -> Option<AntichainRef<Timestamp>> {
        self.views.get(name).map(|v| v.upper.frontier())
    }

    /// Updates the since frontier of a named view.
    ///
    /// This frontier tracks compaction frontier, and represents a lower bound on times for
    /// which the associated trace is certain to produce valid results. For times greater
    /// or equal to some element of the since frontier the accumulation will be correct,
    /// and for other times no such guarantee holds.
    #[allow(dead_code)]
    fn update_since(&mut self, name: &str, since: &[Timestamp]) {
        if let Some(entry) = self.views.get_mut(name) {
            entry.since.clear();
            entry.since.extend(since.iter().cloned());
        }
    }

    /// The since frontier of a maintained view, if it exists.
    #[allow(dead_code)]
    fn since_of(&self, name: &str) -> Option<&Antichain<Timestamp>> {
        self.views.get(name).map(|v| &v.since)
    }

    /// Inserts a view into the coordinator.
    ///
    /// Initializes managed state and logs the insertion (and removal of any existing view).
    fn insert_views(&mut self, dataflow: &DataflowDesc) {
        let contains_sources = !dataflow.sources.is_empty();
        for view in dataflow.views.iter() {
            self.remove_view(&view.name);
            let mut viewstate = ViewState::from_view(view, self.num_timely_workers);
            viewstate.depends_on_source = contains_sources;
            if self.log {
                for time in viewstate.upper.frontier().iter() {
                    broadcast(
                        &mut self.broadcast_tx,
                        SequencedCommand::AppendLog(MaterializedEvent::Frontier(
                            view.name.clone(),
                            time.clone(),
                            1,
                        )),
                    );
                }
            }
            self.views.insert(view.name.clone(), viewstate);
        }
    }

    /// Inserts a source into the coordinator.
    ///
    /// Unlike `insert_view`, this method can be called without a dataflow argument.
    /// This is most commonly used for internal sources such as logging.
    fn insert_source(&mut self, name: &str, compaction_ms: Timestamp) {
        self.remove_view(name);
        let mut viewstate = ViewState::new(self.num_timely_workers);
        if self.log {
            for time in viewstate.upper.frontier().iter() {
                broadcast(
                    &mut self.broadcast_tx,
                    SequencedCommand::AppendLog(MaterializedEvent::Frontier(
                        name.to_string(),
                        time.clone(),
                        1,
                    )),
                );
            }
        }
        viewstate.set_compaction_latency(compaction_ms);
        self.views.insert(name.to_string(), viewstate);
    }

    /// Removes a view from the coordinator.
    ///
    /// Removes the managed state and logs the removal.
    fn remove_view(&mut self, name: &str) {
        if let Some(state) = self.views.remove(name) {
            if self.log {
                for time in state.upper.frontier().iter() {
                    broadcast(
                        &mut self.broadcast_tx,
                        SequencedCommand::AppendLog(MaterializedEvent::Frontier(
                            name.to_string(),
                            time.clone(),
                            -1,
                        )),
                    );
                }
            }
        }
    }
}

fn broadcast(
    tx: &mut sink::Wait<comm::broadcast::Sender<SequencedCommand>>,
    cmd: SequencedCommand,
) {
    // TODO(benesch): avoid flushing after every send. This will require
    // something smarter than sink::Wait, which won't flush the sink if a send
    // gets stuck.
    tx.send(cmd).unwrap();
    tx.flush().unwrap();
}

/// Constructs an [`ExecuteResponse`] that that will send some rows to the
/// client immediately, as opposed to asking the dataflow layer to send along
/// the rows after some computation.
fn send_immediate_rows(rows: Vec<Row>) -> ExecuteResponse {
    let (tx, rx) = futures::sync::oneshot::channel();
    tx.send(PeekResponse::Rows(rows)).unwrap();
    ExecuteResponse::SendRows(Box::new(rx.from_err()))
}

/// Per-view state.
pub struct ViewState {
    /// Names of views on which this view depends.
    uses: Vec<String>,
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
    compaction_latency_ms: Timestamp,
    /// True if the dataflow defining the view may depend on a source, which
    /// leads us to include the frontier of the view in timestamp selection,
    /// as the view cannot be expected to advance its frontier simply because
    /// its input views advance.
    depends_on_source: bool,
}

impl ViewState {
    /// Creates an empty view state from a number of workers.
    pub fn new(workers: u64) -> Self {
        let mut upper = MutableAntichain::new();
        upper.update_iter(Some((0, workers as i64)));
        Self {
            uses: Vec::new(),
            upper,
            since: Antichain::from_elem(0),
            compaction_latency_ms: 60_000,
            depends_on_source: true,
        }
    }

    /// Creates view state from a view, and number of workers.
    pub fn from_view(view: &View, workers: u64) -> Self {
        let mut view_state = Self::new(workers);
        let mut out = Vec::new();
        view.relation_expr.unbound_uses(&mut out);
        out.sort();
        out.dedup();
        view_state.uses = out.iter().map(|x| x.to_string()).collect();
        view_state
    }

    /// Sets the latency behind the collection frontier at which compaction occurs.
    pub fn set_compaction_latency(&mut self, latency_ms: Timestamp) {
        self.compaction_latency_ms = latency_ms;
    }
}

/// Creates an internal name for sources that are unlikely to clash with view names.
fn create_internal_source_name(name: &str) -> String {
    format!("{}-VIEW", name)
}
