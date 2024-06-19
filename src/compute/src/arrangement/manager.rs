// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Management of arrangements across dataflows.

use std::any::Any;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Instant;

use differential_dataflow::lattice::{antichain_join, Lattice};
use differential_dataflow::operators::arrange::{Arranged, ShutdownButton, TraceAgent};
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use differential_dataflow::trace::TraceReader;
use mz_repr::{Diff, GlobalId, Timestamp};
use timely::dataflow::operators::{probe, CapabilitySet, Probe};
use timely::dataflow::scopes::Child;
use timely::dataflow::Scope;
use timely::progress::frontier::{Antichain, AntichainRef};
use timely::progress::timestamp::Refines;
use timely::PartialOrder;

use crate::metrics::TraceMetrics;
use crate::render::context::MzArrangementImport;
use crate::typedefs::{ErrAgent, RowRowAgent};

/// A `TraceManager` stores maps from global identifiers to the primary arranged
/// representation of that collection.
pub struct TraceManager {
    pub(crate) traces: BTreeMap<GlobalId, TraceBundle>,
    metrics: TraceMetrics,
}

impl TraceManager {
    /// TODO(undocumented)
    pub fn new(metrics: TraceMetrics) -> Self {
        TraceManager {
            traces: BTreeMap::new(),
            metrics,
        }
    }

    /// performs maintenance work on the managed traces.
    ///
    /// In particular, this method enables the physical merging of batches, so that at most a logarithmic
    /// number of batches need to be maintained. Any new batches introduced after this method is called
    /// will not be physically merged until the method is called again. This is mostly due to limitations
    /// of differential dataflow, which requires users to perform this explicitly; if that changes we may
    /// be able to remove this code.
    pub fn maintenance(&mut self) {
        let start = Instant::now();
        self.metrics.maintenance_active_info.set(1);

        let mut antichain = Antichain::new();
        for bundle in self.traces.values_mut() {
            bundle.oks.read_upper(&mut antichain);
            bundle.oks.set_physical_compaction(antichain.borrow());
            bundle.errs.read_upper(&mut antichain);
            bundle.errs.set_physical_compaction(antichain.borrow());
        }

        let duration = start.elapsed().as_secs_f64();
        self.metrics.maintenance_seconds_total.inc_by(duration);
        self.metrics.maintenance_active_info.set(0);
    }

    /// Enables compaction of traces associated with the identifier.
    ///
    /// Compaction may not occur immediately, but once this method is called the
    /// associated traces may not accumulate to the correct quantities for times
    /// not in advance of `frontier`. Users should take care to only rely on
    /// accumulations at times in advance of `frontier`.
    pub fn allow_compaction(&mut self, id: GlobalId, frontier: AntichainRef<Timestamp>) {
        if let Some(bundle) = self.traces.get_mut(&id) {
            bundle.oks.set_logical_compaction(frontier);
            bundle.errs.set_logical_compaction(frontier);
        }
    }

    /// Returns a reference to the trace for `id`, should it exist.
    pub fn get(&self, id: &GlobalId) -> Option<&TraceBundle> {
        self.traces.get(id)
    }

    /// Returns a mutable reference to the trace for `id`, should it
    /// exist.
    pub fn get_mut(&mut self, id: &GlobalId) -> Option<&mut TraceBundle> {
        self.traces.get_mut(id)
    }

    /// Binds the arrangement for `id` to `trace`.
    pub fn set(&mut self, id: GlobalId, trace: TraceBundle) {
        self.traces.insert(id, trace);
    }

    /// Removes the trace for `id`.
    pub fn remove(&mut self, id: &GlobalId) -> Option<TraceBundle> {
        self.traces.remove(id)
    }
}

/// An abstraction of a trace handle.
///
/// This type exists as an `enum` to support potential experimentation with alternate
/// representation and layouts.
#[derive(Clone)]
pub enum SpecializedTraceHandle {
    RowRow(PaddedTrace<RowRowAgent<Timestamp, Diff>>),
}

impl From<RowRowAgent<Timestamp, Diff>> for SpecializedTraceHandle {
    fn from(trace: RowRowAgent<Timestamp, Diff>) -> Self {
        Self::RowRow(trace.into())
    }
}

impl SpecializedTraceHandle {
    /// Obtains the logical compaction frontier for the underlying trace handle.
    fn get_logical_compaction(&mut self) -> AntichainRef<Timestamp> {
        match self {
            SpecializedTraceHandle::RowRow(handle) => handle.get_logical_compaction(),
        }
    }

    /// Advances the logical compaction frontier for the underlying trace handle.
    pub fn set_logical_compaction(&mut self, frontier: AntichainRef<Timestamp>) {
        match self {
            SpecializedTraceHandle::RowRow(handle) => handle.set_logical_compaction(frontier),
        }
    }

    /// Advances the physical compaction frontier for the underlying trace handle.
    pub fn set_physical_compaction(&mut self, frontier: AntichainRef<Timestamp>) {
        match self {
            SpecializedTraceHandle::RowRow(handle) => handle.set_physical_compaction(frontier),
        }
    }

    /// Reads the upper frontier of the underlying trace handle.
    pub fn read_upper(&mut self, target: &mut Antichain<Timestamp>) {
        match self {
            SpecializedTraceHandle::RowRow(handle) => handle.read_upper(target),
        }
    }

    /// Maps the underlying trace handle to a `MzArrangementImport`,
    /// while readjusting times by `since` and `until`.
    pub fn import_frontier<'g, G, T>(
        &mut self,
        scope: &Child<'g, G, T>,
        name: &str,
        since: Antichain<Timestamp>,
        until: Antichain<Timestamp>,
        input_probe: probe::Handle<Timestamp>,
    ) -> (
        MzArrangementImport<Child<'g, G, T>, Timestamp>,
        ShutdownButton<CapabilitySet<Timestamp>>,
    )
    where
        G: Scope<Timestamp = Timestamp>,
        T: Lattice + Refines<G::Timestamp>,
    {
        match self {
            SpecializedTraceHandle::RowRow(handle) => {
                let (oks, oks_button) =
                    handle.import_frontier_core(&scope.parent, name, since, until);
                let oks = Arranged {
                    stream: oks.stream.probe_with(&input_probe),
                    trace: oks.trace,
                };
                (MzArrangementImport::RowRow(oks.enter(scope)), oks_button)
            }
        }
    }

    /// Turns this trace into a padded version that reports empty data for all times less than the
    /// trace's current logical compaction frontier.
    fn into_padded(self) -> Self {
        match self {
            Self::RowRow(trace) => Self::RowRow(trace.into_padded()),
        }
    }
}

/// Handle to a trace that can be padded.
///
/// A padded trace contains empty data for all times greater than or equal to its `padded_since`
/// and less than the logical compaction frontier of the inner `trace`.
///
/// This type is intentionally limited to only work with `mz_repr::Timestamp` times, because that
/// is all that's required by `TraceManager`. It can be made to be more generic, at the cost of
/// more complicated reasoning about the correct management of the involved frontiers.
#[derive(Clone)]
pub struct PaddedTrace<Tr>
where
    Tr: TraceReader<Time = Timestamp>,
{
    /// The wrapped trace.
    trace: Tr,
    /// The frontier from which the trace is padded, or `None` if it is not padded.
    ///
    /// Invariant: The contained frontier is less than the logical compaction frontier of `trace`.
    ///
    /// All methods of `PaddedTrace` are written to uphold this invariant. In particular,
    /// `set_logical_compaction_frontier`  sets the `padded_since` to `None` if the new compaction
    /// frontier is >= the previous compaction frontier of `trace`.
    padded_since: Option<Antichain<Timestamp>>,
}

impl<Tr> From<Tr> for PaddedTrace<Tr>
where
    Tr: TraceReader<Time = Timestamp>,
{
    fn from(trace: Tr) -> Self {
        Self {
            trace,
            padded_since: None,
        }
    }
}

impl<Tr> PaddedTrace<Tr>
where
    Tr: TraceReader<Time = Timestamp>,
{
    /// Turns this trace into a padded version that reports empty data for all times less than the
    /// trace's current logical compaction frontier.
    fn into_padded(mut self) -> Self {
        let trace_since = self.trace.get_logical_compaction();
        let minimum_frontier = Antichain::from_elem(Timestamp::MIN);
        if PartialOrder::less_than(&minimum_frontier.borrow(), &trace_since) {
            self.padded_since = Some(minimum_frontier);
        }
        self
    }
}

impl<Tr> TraceReader for PaddedTrace<Tr>
where
    Tr: TraceReader<Time = Timestamp>,
{
    type Key<'a> = Tr::Key<'a>;
    type Val<'a> = Tr::Val<'a>;
    type Time = Tr::Time;
    type TimeGat<'a> = Tr::TimeGat<'a>;
    type Diff = Tr::Diff;
    type DiffGat<'a> = Tr::DiffGat<'a>;
    type Batch = Tr::Batch;
    type Storage = Tr::Storage;
    type Cursor = Tr::Cursor;

    fn cursor_through(
        &mut self,
        upper: AntichainRef<Self::Time>,
    ) -> Option<(Self::Cursor, Self::Storage)> {
        self.trace.cursor_through(upper)
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Self::Time>) {
        let Some(padded_since) = &mut self.padded_since else {
            self.trace.set_logical_compaction(frontier);
            return;
        };

        // If a padded trace is compacted to some frontier less than the inner trace's compaction
        // frontier, advance the `padded_since`. Otherwise discard the padding and apply the
        // compaction to the inner trace instead.
        let trace_since = self.trace.get_logical_compaction();
        if PartialOrder::less_than(&frontier, &trace_since) {
            if PartialOrder::less_than(&padded_since.borrow(), &frontier) {
                *padded_since = frontier.to_owned();
            }
        } else {
            self.padded_since = None;
            self.trace.set_logical_compaction(frontier);
        }
    }

    fn get_logical_compaction(&mut self) -> AntichainRef<Self::Time> {
        match &self.padded_since {
            Some(since) => since.borrow(),
            None => self.trace.get_logical_compaction(),
        }
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<Self::Time>) {
        self.trace.set_physical_compaction(frontier);
    }

    fn get_physical_compaction(&mut self) -> AntichainRef<Self::Time> {
        self.trace.get_logical_compaction()
    }

    fn map_batches<F: FnMut(&Self::Batch)>(&self, f: F) {
        self.trace.map_batches(f)
    }
}

impl<Tr> PaddedTrace<TraceAgent<Tr>>
where
    Tr: TraceReader<Time = Timestamp> + 'static,
{
    /// Import a trace restricted to a specific time interval `[since, until)`.
    pub fn import_frontier_core<G>(
        &mut self,
        scope: &G,
        name: &str,
        since: Antichain<Tr::Time>,
        until: Antichain<Tr::Time>,
    ) -> (
        Arranged<G, TraceFrontier<TraceAgent<Tr>>>,
        ShutdownButton<CapabilitySet<Tr::Time>>,
    )
    where
        G: Scope<Timestamp = Tr::Time>,
    {
        self.trace.import_frontier_core(scope, name, since, until)
    }
}

/// Bundles together traces for the successful computations (`oks`), the
/// failed computations (`errs`), additional tokens that should share
/// the lifetime of the bundled traces (`to_drop`).
#[derive(Clone)]
pub struct TraceBundle {
    oks: SpecializedTraceHandle,
    errs: PaddedTrace<ErrAgent<Timestamp, Diff>>,
    to_drop: Option<Rc<dyn Any>>,
}

impl TraceBundle {
    /// Constructs a new trace bundle out of an `oks` trace and `errs` trace.
    pub fn new<O, E>(oks: O, errs: E) -> TraceBundle
    where
        O: Into<SpecializedTraceHandle>,
        E: Into<PaddedTrace<ErrAgent<Timestamp, Diff>>>,
    {
        TraceBundle {
            oks: oks.into(),
            errs: errs.into(),
            to_drop: None,
        }
    }

    /// Adds tokens to be dropped when the trace bundle is dropped.
    pub fn with_drop<T>(self, to_drop: T) -> TraceBundle
    where
        T: 'static,
    {
        TraceBundle {
            to_drop: Some(Rc::new(Box::new(to_drop))),
            ..self
        }
    }

    /// Returns a mutable reference to the `oks` trace.
    pub fn oks_mut(&mut self) -> &mut SpecializedTraceHandle {
        &mut self.oks
    }

    /// Returns a mutable reference to the `errs` trace.
    pub fn errs_mut(&mut self) -> &mut PaddedTrace<ErrAgent<Timestamp, Diff>> {
        &mut self.errs
    }

    /// Returns a reference to the `to_drop` tokens.
    pub fn to_drop(&self) -> &Option<Rc<dyn Any>> {
        &self.to_drop
    }

    /// Returns the frontier up to which the traces have been allowed to compact.
    pub fn compaction_frontier(&mut self) -> Antichain<Timestamp> {
        antichain_join(
            &self.oks.get_logical_compaction(),
            &self.errs.get_logical_compaction(),
        )
    }

    /// Turns this trace bundle into a padded version that reports empty data for all times less
    /// than the traces' current logical compaction frontier.
    ///
    /// Note that the padded bundle represents a different TVC than the original one, it is unsound
    /// to use it to "uncompact" an existing TVC. The only valid use of the padded bundle is to
    /// initializa a new TVC.
    pub fn into_padded(self) -> Self {
        Self {
            oks: self.oks.into_padded(),
            errs: self.errs.into_padded(),
            to_drop: self.to_drop,
        }
    }
}
