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
use std::collections::HashMap;
use std::convert::TryFrom;
use uuid::Uuid;

use crate::SqlResponse;
use dataflow::logging::materialized::MaterializedEvent;
use dataflow::{SequencedCommand, WorkerFeedbackWithMeta};
use dataflow_types::logging::LoggingConfig;
use dataflow_types::{
    compare_columns, DataflowDescription, PeekResponse, PeekWhen, RowSetFinishing, Sink,
    SinkConnector, TailSinkConnector, Timestamp, View,
};
use expr::RelationExpr;
use ore::future::FutureExt;
use repr::{Datum, RelationDesc, ScalarType};
use sql::Plan;

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
    since_updates: Vec<(String, Vec<Timestamp>)>,
    /// For each connection running a TAIL command, the name of the dataflow
    /// that is servicing the TAIL. A connection can only run one TAIL at a
    /// time.
    active_tails: HashMap<u32, String>,
    log: bool,
}

impl<C> Coordinator<C>
where
    C: comm::Connection,
{
    pub fn new(
        switchboard: comm::Switchboard<C>,
        num_timely_workers: usize,
        logging_config: Option<&LoggingConfig>,
    ) -> Self {
        let broadcast_tx = switchboard
            .broadcast_tx::<dataflow::BroadcastToken>()
            .wait();

        let mut coordinator = Self {
            switchboard,
            broadcast_tx,
            num_timely_workers: u64::try_from(num_timely_workers).unwrap(),
            optimizer: Default::default(),
            views: HashMap::new(),
            since_updates: Vec::new(),
            active_tails: HashMap::new(),
            log: logging_config.is_some(),
        };

        if let Some(logging_config) = logging_config {
            for log in logging_config.active_logs().iter() {
                // Insert with 1 second compaction latency.
                coordinator.insert_name(log.name(), 1_000);
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
            self.drop_dataflows(vec![name]);
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

    // TODO(benesch): the `ts_override` parameter exists only to support
    // sqllogictest and is kind of gross. See if we can get rid of it.
    pub fn sequence_plan(
        &mut self,
        plan: Plan,
        conn_id: u32,
        ts_override: Option<Timestamp>,
    ) -> SqlResponse {
        match plan {
            Plan::CreateSource(source) => {
                self.create_dataflows(vec![DataflowDescription::from(source)]);
                SqlResponse::CreatedSource
            }

            Plan::CreateSources(sources) => {
                self.create_dataflows(
                    sources
                        .iter()
                        .map(|s| DataflowDescription::from(s.clone()))
                        .collect(),
                );
                send_immediate_rows(
                    RelationDesc::empty().add_column("Topic", ScalarType::String),
                    sources
                        .iter()
                        .map(|s| vec![Datum::from(s.name.to_owned())])
                        .collect(),
                )
            }

            Plan::CreateSink(sink) => {
                self.create_dataflows(vec![DataflowDescription::from(sink)]);
                SqlResponse::CreatedSink
            }

            Plan::CreateView(view) => {
                self.create_dataflows(vec![DataflowDescription::from(view)]);
                SqlResponse::CreatedView
            }

            Plan::DropSources(names) => {
                self.drop_dataflows(names);
                // Though the plan specifies a number of sources to drop, multiple
                // sources can only be dropped via DROP SOURCE root CASCADE, so
                // the tagline is still singular, as in "DROP SOURCE".
                SqlResponse::DroppedSource
            }

            Plan::DropViews(names) => {
                // See note in `DropSources` about the conversion from plural to
                // singular.
                broadcast(&mut self.broadcast_tx, SequencedCommand::DropThings(names));
                SqlResponse::DroppedView
            }

            Plan::EmptyQuery => SqlResponse::EmptyQuery,

            Plan::SetVariable { .. } => SqlResponse::SetVariable,

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
                let timestamp =
                    ts_override.unwrap_or_else(|| self.determine_timestamp(&source, when));

                // For straight-forward dataflow pipelines, use finishing instructions instead of
                // dataflow operators.
                Coordinator::<C>::maybe_use_finishing(&mut source, &mut finishing);

                // Create a transient view if the peek is not of a base relation.
                if let RelationExpr::Get { name, typ: _ } = source {
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

                    let dataflow = DataflowDescription::from(View {
                        name: name.clone(),
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
                        SequencedCommand::DropThings(vec![name]),
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

                SqlResponse::SendRows { desc, rx: rows_rx }
            }

            Plan::Tail(source) => {
                let name = format!("<tail_{}>", Uuid::new_v4());
                self.active_tails.insert(conn_id, name.clone());
                let (tx, rx) = self.switchboard.mpsc_limited(self.num_timely_workers);
                broadcast(
                    &mut self.broadcast_tx,
                    SequencedCommand::CreateDataflows(vec![DataflowDescription::from(Sink {
                        name,
                        from: (source.name().to_owned(), source.desc().clone()),
                        connector: SinkConnector::Tail(TailSinkConnector { tx }),
                    })]),
                );
                SqlResponse::Tailing { rx }
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

            Plan::Parsed => SqlResponse::Parsed,
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

    pub fn create_dataflows(&mut self, mut dataflows: Vec<DataflowDescription>) {
        for dataflow in dataflows.iter_mut() {
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

    pub fn drop_dataflows(&mut self, dataflow_names: Vec<String>) {
        broadcast(
            &mut self.broadcast_tx,
            SequencedCommand::DropThings(dataflow_names),
        )
    }

    pub fn enable_feedback(&mut self) -> comm::mpsc::Receiver<WorkerFeedbackWithMeta> {
        let (tx, rx) = self.switchboard.mpsc();
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
                            self.sources_frontier(name, &mut bound);
                        }
                    }
                    PeekWhen::Immediately => {
                        for name in names {
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

    /// Introduces all frontier elements from sources (not views) into `bound`.
    ///
    /// This method traverses all views whose only inputs are other views (no
    /// sources) until it arrives at views that either depend on at least one
    /// source, or which have no other views as inputs. This latter case is a
    /// bit silly, but is how logging works for the moment (until we migrate
    /// to logging being a flavor of source).
    fn sources_frontier(&self, name: &str, bound: &mut Antichain<Timestamp>) {
        let no_inputs = self.views[name].uses.is_empty();
        let no_sources = self.views[name]
            .uses
            .iter()
            .all(|n| self.views.contains_key(n));
        if no_sources && !no_inputs {
            for name in self.views[name].uses.iter() {
                self.sources_frontier(name, bound);
            }
        } else {
            let upper = self.upper_of(name).expect("Name missing at coordinator");
            bound.extend(upper.elements().iter().cloned());
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
    fn insert_views(&mut self, dataflow: &DataflowDescription) {
        for view in dataflow.views.iter() {
            self.remove_view(&view.name);
            let viewstate = ViewState::from(view);
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
    fn insert_name(&mut self, name: &str, compaction_ms: Timestamp) {
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

/// Constructs a [`SqlResponse`] that that will send some rows to the client
/// immediately, as opposed to asking the dataflow layer to send along the rows
/// after some computation.
fn send_immediate_rows(desc: RelationDesc, rows: Vec<Vec<Datum>>) -> SqlResponse {
    let (tx, rx) = futures::sync::oneshot::channel();
    tx.send(PeekResponse::Rows(rows)).unwrap();
    SqlResponse::SendRows {
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
