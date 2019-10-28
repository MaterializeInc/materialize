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

use timely::progress::frontier::Antichain;

use futures::{sink, Future, Sink as FuturesSink, Stream};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use uuid::Uuid;

use crate::QueryExecuteResponse;
use dataflow::logging::materialized::MaterializedEvent;
use dataflow::{SequencedCommand, WorkerFeedbackWithMeta};
use dataflow_types::logging::LoggingConfig;
use dataflow_types::{
    compare_columns, DataflowDesc, PeekResponse, PeekWhen, RowSetFinishing, Sink, SinkConnector,
    Source, SourceConnector, TailSinkConnector, Timestamp, Update, View,
};
use expr::RelationExpr;
use ore::future::FutureExt;
use repr::{Datum, RelationDesc, ScalarType};
use sql::{MutationKind, ObjectType, Plan};

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

    pub fn sequence_plan(&mut self, plan: Plan, conn_id: u32) -> QueryExecuteResponse {
        match plan {
            Plan::CreateTable { name, desc } => {
                self.create_sources(vec![Source {
                    name,
                    connector: SourceConnector::Local,
                    desc,
                }]);
                QueryExecuteResponse::CreatedTable
            }

            Plan::CreateSource(source) => {
                let sources = vec![source];
                self.create_sources(sources);
                QueryExecuteResponse::CreatedSource
            }

            Plan::CreateSources(sources) => {
                self.create_sources(sources.clone());
                send_immediate_rows(
                    RelationDesc::empty().add_column("Topic", ScalarType::String),
                    sources
                        .iter()
                        .map(|s| vec![Datum::from(s.name.to_owned())])
                        .collect(),
                )
            }

            Plan::CreateSink(sink) => {
                self.create_dataflows(vec![DataflowDesc::from(sink)]);
                QueryExecuteResponse::CreatedSink
            }

            Plan::CreateView(view) => {
                self.create_dataflows(vec![DataflowDesc::from(view)]);
                QueryExecuteResponse::CreatedView
            }

            Plan::DropItems(names, item_type) => {
                let mut sources_to_drop = Vec::new();
                let mut views_to_drop = Vec::new();
                for name in names {
                    // TODO: Test if a name was installed and error if not?
                    if let Some((_source, new_name)) = self.sources.remove(&name) {
                        sources_to_drop.push(name.clone());
                        if let Some(name) = new_name {
                            views_to_drop.push(name);
                        } else {
                            panic!("Attempting to drop an uninstalled source: {}", name);
                        }
                    }
                    if self.views.contains_key(&name) {
                        views_to_drop.push(name);
                    }
                }
                if sources_to_drop.is_empty() {
                    broadcast(
                        &mut self.broadcast_tx,
                        SequencedCommand::DropSources(sources_to_drop),
                    )
                }
                self.drop_views(views_to_drop);
                match item_type {
                    ObjectType::Source => QueryExecuteResponse::DroppedSource,
                    ObjectType::View => QueryExecuteResponse::DroppedView,
                    ObjectType::Table => QueryExecuteResponse::DroppedTable,
                    ObjectType::Sink | ObjectType::Index => unreachable!(),
                }
            }

            Plan::EmptyQuery => QueryExecuteResponse::EmptyQuery,

            Plan::SetVariable { name, .. } => QueryExecuteResponse::SetVariable { name },

            Plan::Peek {
                mut source,
                desc,
                when,
                mut finishing,
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

                // For straight-forward dataflow pipelines, use finishing instructions instead of
                // dataflow operators.
                Self::maybe_use_finishing(&mut source, &mut finishing);

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
                        },
                    );
                } else {
                    // Slow path. We need to perform some computation, so build
                    // a new transient dataflow that will be dropped after the
                    // peek completes.
                    let name = format!("<peek_{}>", Uuid::new_v4());

                    let dataflow = DataflowDesc::from(View {
                        name: name.clone(),
                        raw_sql: "<none>".into(),
                        relation_expr: source,
                        desc: desc.clone(),
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
                            let sort_by = |left: &Vec<Datum>, right: &Vec<Datum>| {
                                compare_columns(&finishing.order_by, left, right)
                            };
                            let offset = finishing.offset;
                            if offset > rows.len() {
                                *rows = Vec::new();
                            } else {
                                if let Some(limit) = finishing.limit {
                                    let offset_plus_limit = offset + limit;
                                    if rows.len() > offset_plus_limit {
                                        pdqselect::select_by(rows, offset_plus_limit, sort_by);
                                        rows.truncate(offset_plus_limit);
                                    }
                                }
                                if offset > 0 {
                                    pdqselect::select_by(rows, offset, sort_by);
                                    rows.drain(..offset);
                                }
                                rows.sort_by(sort_by);
                                for row in rows {
                                    *row =
                                        finishing.project.iter().map(|i| row[*i].clone()).collect();
                                }
                            }
                        }
                        resp
                    })
                    .from_err()
                    .boxed();

                QueryExecuteResponse::SendRows { desc, rx: rows_rx }
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
                    .elements()
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

                QueryExecuteResponse::Tailing { rx }
            }

            Plan::SendRows { desc, rows } => send_immediate_rows(desc, rows),

            Plan::ExplainPlan {
                desc,
                mut relation_expr,
            } => {
                self.optimizer.optimize(&mut relation_expr);
                let rows = vec![vec![Datum::from(relation_expr.pretty())]];
                send_immediate_rows(desc, rows)
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
                    MutationKind::Delete => QueryExecuteResponse::Deleted(affected_rows),
                    MutationKind::Insert => QueryExecuteResponse::Inserted(affected_rows),
                    MutationKind::Update => QueryExecuteResponse::Updated(affected_rows),
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
                    DataflowDesc::new(None)
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

    /// Maybe use finishing instructions instead of relation expressions. This
    /// method may replace top-level dataflow operators in the given dataflow
    /// graph with semantically equivalent finishing instructions.
    ///
    /// This makes sense for short-lived queries that do not need to shuffle
    /// data between workers and is the case for basic
    /// `SELECT ... FROM ... WHERE ...` queries. They translate to
    /// straight-forward dataflow pipelines that start with a `Get` on a
    /// built-in view or a source and then process the resulting records with
    /// `Project`, `Map`, `Filter`, `Negate`, `Threshold`, and possibly `Union`.
    /// By executing finishing instructions instead of installing full dataflow
    /// graphs, such queries can avoid view generation altogether and execute
    /// faster. That is particularly valuable for introspective queries, e.g.,
    /// when performance monitoring. It is, however, exactly the wrong thing to
    /// do when a query needs to install a view, since `Project`, `Filter`, and
    /// `Threshold` may reduce the size of that view.
    fn maybe_use_finishing(expr: &mut RelationExpr, finishing: &mut RowSetFinishing) {
        // Check whether the relation expression is a simple pipeline.
        fn is_simple(expr: &RelationExpr) -> bool {
            match expr {
                RelationExpr::Filter { input, .. } => is_simple(input),
                RelationExpr::Get { .. } => true,
                _ => false,
            }
        }

        if !is_simple(expr) {
            return;
        }

        // Replace operators with finishing instructions.
        fn use_finishing(expr: &mut RelationExpr, finishing: &mut RowSetFinishing) {
            if let RelationExpr::Get { .. } = expr {
                return;
            }

            match expr.take_dangerous() {
                RelationExpr::Filter {
                    mut input,
                    mut predicates,
                } => {
                    use_finishing(&mut input, finishing);
                    *expr = *input;
                    finishing.filter.append(&mut predicates);
                }
                _ => unreachable!(),
            }
        }

        use_finishing(expr, finishing);
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
                            let upper = self
                                .upper_of(name)
                                .expect("Absent relation in view")
                                .elements()
                                .iter()
                                .cloned();
                            bound.extend(upper);
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
                    bound.extend(upper.elements().iter().cloned());
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
    pub fn update_upper(&mut self, name: &str, upper: &[Timestamp]) {
        if let Some(entry) = self.views.get_mut(name) {
            // We may be informed of non-changes; suppress them.
            if entry.upper.elements() != upper {
                // Log the change to frontiers.
                if self.log {
                    for time in upper.iter() {
                        broadcast(
                            &mut self.broadcast_tx,
                            SequencedCommand::AppendLog(MaterializedEvent::Frontier(
                                name.to_string(),
                                time.clone(),
                                1,
                            )),
                        );
                    }
                    for time in entry.upper.elements().iter() {
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

                entry.upper.clear();
                entry.upper.extend(upper.iter().cloned());

                let mut since = Antichain::new();
                for time in entry.upper.elements() {
                    since.insert(time.saturating_sub(entry.compaction_latency_ms));
                }
                self.since_updates
                    .push((name.to_string(), since.elements().to_vec()));
            }
        }
    }

    /// The upper frontier of a maintained view, if it exists.
    fn upper_of(&self, name: &str) -> Option<&Antichain<Timestamp>> {
        self.views.get(name).map(|v| &v.upper)
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
            let mut viewstate = ViewState::from(view);
            viewstate.depends_on_source = contains_sources;
            if self.log {
                for time in viewstate.upper.elements() {
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
        let mut viewstate = ViewState::default();
        if self.log {
            for time in viewstate.upper.elements() {
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
                for time in state.upper.elements() {
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

/// Constructs a [`QueryExecuteResponse`] that that will send some rows to the client
/// immediately, as opposed to asking the dataflow layer to send along the rows
/// after some computation.
fn send_immediate_rows(desc: RelationDesc, rows: Vec<Vec<Datum>>) -> QueryExecuteResponse {
    let (tx, rx) = futures::sync::oneshot::channel();
    tx.send(PeekResponse::Rows(rows)).unwrap();
    QueryExecuteResponse::SendRows {
        desc,
        rx: Box::new(rx.from_err()),
    }
}

/// Per-view state.
pub struct ViewState {
    /// Names of views on which this view depends.
    uses: Vec<String>,
    /// The most recent frontier for new data.
    /// All further changes will be in advance of this bound.
    upper: Antichain<Timestamp>,
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
    /// Sets the latency behind the collection frontier at which compaction occurs.
    pub fn set_compaction_latency(&mut self, latency_ms: Timestamp) {
        self.compaction_latency_ms = latency_ms;
    }
}

impl Default for ViewState {
    fn default() -> Self {
        Self {
            uses: Vec::new(),
            upper: Antichain::from_elem(0),
            since: Antichain::from_elem(0),
            compaction_latency_ms: 60_000,
            depends_on_source: true,
        }
    }
}

impl<'view> From<&'view View> for ViewState {
    fn from(view: &'view View) -> Self {
        let mut view_state = Self::default();
        let mut out = Vec::new();
        view.relation_expr.unbound_uses(&mut out);
        out.sort();
        out.dedup();
        view_state.uses = out.iter().map(|x| x.to_string()).collect();
        view_state
    }
}

/// Creates an internal name for sources that are unlikely to clash with view names.
fn create_internal_source_name(name: &str) -> String {
    format!("{}-VIEW", name)
}
