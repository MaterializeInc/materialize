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

use differential_dataflow::lattice::antichain_join;
use differential_dataflow::operators::arrange::{Arranged, ShutdownButton, TraceAgent};
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use mz_repr::{Diff, GlobalId, Timestamp};
use timely::PartialOrder;
use timely::dataflow::Scope;
use timely::dataflow::operators::CapabilitySet;
use timely::progress::Timestamp as _;
use timely::progress::frontier::{Antichain, AntichainRef};

use crate::metrics::WorkerMetrics;
use crate::shared_trace::{SharedErrsHandle, SharedOksHandle};
use crate::typedefs::{ErrAgent, RowRowAgent};

/// A `TraceManager` stores maps from global identifiers to the primary arranged
/// representation of that collection.
pub struct TraceManager {
    pub(crate) traces: BTreeMap<GlobalId, TraceBundle>,
    metrics: WorkerMetrics,
}

impl TraceManager {
    /// TODO(undocumented)
    pub fn new(metrics: WorkerMetrics) -> Self {
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
        self.metrics.arrangement_maintenance_active_info.set(1);

        let mut antichain = Antichain::new();
        for bundle in self.traces.values_mut() {
            bundle.oks.read_upper(&mut antichain);
            bundle.oks.set_physical_compaction(antichain.borrow());
            bundle.errs.read_upper(&mut antichain);
            bundle.errs.set_physical_compaction(antichain.borrow());
        }

        let duration = start.elapsed().as_secs_f64();
        self.metrics
            .arrangement_maintenance_seconds_total
            .inc_by(duration);
        self.metrics.arrangement_maintenance_active_info.set(0);
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
    Tr: TraceReader,
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
    padded_since: Option<Antichain<Tr::Time>>,
}

impl<Tr> From<Tr> for PaddedTrace<Tr>
where
    Tr: TraceReader,
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
    Tr: TraceReader,
{
    /// Turns this trace into a padded version that reports empty data for all times less than the
    /// trace's current logical compaction frontier.
    fn into_padded(mut self) -> Self {
        let trace_since = self.trace.get_logical_compaction();
        let minimum_frontier = Antichain::from_elem(Tr::Time::minimum());
        if PartialOrder::less_than(&minimum_frontier.borrow(), &trace_since) {
            self.padded_since = Some(minimum_frontier);
        }
        self
    }

    /// The wrapped trace, which does not report the padding.
    pub fn unpadded(&self) -> &Tr {
        &self.trace
    }
}

impl<Tr> TraceReader for PaddedTrace<Tr>
where
    Tr: TraceReader,
{
    type Time = Tr::Time;
    type Batch = Tr::Batch;

    fn batches_through(&mut self, upper: AntichainRef<Self::Time>) -> Option<Vec<Self::Batch>> {
        self.trace.batches_through(upper)
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

    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Self::Time> {
        match &self.padded_since {
            Some(since) => since.borrow(),
            None => self.trace.get_logical_compaction(),
        }
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<Self::Time>) {
        self.trace.set_physical_compaction(frontier);
    }

    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Self::Time> {
        self.trace.get_physical_compaction()
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
    pub fn import_frontier_core<'scope>(
        &mut self,
        scope: Scope<'scope, Tr::Time>,
        name: &str,
        since: Antichain<Tr::Time>,
        until: Antichain<Tr::Time>,
    ) -> (
        Arranged<'scope, TraceFrontier<TraceAgent<Tr>>>,
        ShutdownButton<CapabilitySet<Tr::Time>>,
    ) {
        self.trace.import_frontier_core(scope, name, since, until)
    }
}

/// Defines a trace of an index that is either maintained by this runtime or published by the
/// process's other compute runtime.
///
/// The two variants share their batch type, so cursors over either are the same and dispatch costs
/// one branch per trace call, not per record.
macro_rules! index_trace {
    ($(#[$attr:meta])* $name:ident, $local:ty, $shared:ty) => {
        $(#[$attr])*
        #[derive(Clone)]
        pub enum $name {
            /// A trace this runtime maintains.
            Local(PaddedTrace<$local>),
            /// A trace the other runtime publishes. Its logical compaction is this runtime's hold
            /// on the publication.
            Shared($shared),
        }

        impl TraceReader for $name {
            type Time = Timestamp;
            type Batch = <$local as TraceReader>::Batch;

            fn batches_through(
                &mut self,
                upper: AntichainRef<Timestamp>,
            ) -> Option<Vec<Self::Batch>> {
                match self {
                    $name::Local(trace) => trace.batches_through(upper),
                    $name::Shared(trace) => trace.batches_through(upper),
                }
            }

            fn set_logical_compaction(&mut self, frontier: AntichainRef<Timestamp>) {
                match self {
                    $name::Local(trace) => trace.set_logical_compaction(frontier),
                    $name::Shared(trace) => trace.set_logical_compaction(frontier),
                }
            }

            fn get_logical_compaction(&mut self) -> AntichainRef<'_, Timestamp> {
                match self {
                    $name::Local(trace) => trace.get_logical_compaction(),
                    $name::Shared(trace) => trace.get_logical_compaction(),
                }
            }

            fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Timestamp>) {
                match self {
                    $name::Local(trace) => trace.set_physical_compaction(frontier),
                    $name::Shared(trace) => trace.set_physical_compaction(frontier),
                }
            }

            fn get_physical_compaction(&mut self) -> AntichainRef<'_, Timestamp> {
                match self {
                    $name::Local(trace) => trace.get_physical_compaction(),
                    $name::Shared(trace) => trace.get_physical_compaction(),
                }
            }

            fn map_batches<F: FnMut(&Self::Batch)>(&self, f: F) {
                match self {
                    $name::Local(trace) => trace.map_batches(f),
                    $name::Shared(trace) => trace.map_batches(f),
                }
            }
        }

        impl $name {
            /// Imports the trace into `scope`, restricted to `[since, until)`.
            ///
            /// A local import comes with the button that shuts its operator down. A shared import
            /// reads through a reader minted for it, never through this handle: the import holds the
            /// publisher physically at the boundary it was seeded with, which this handle, a logical
            /// hold only, does not.
            pub fn import_frontier_core<'scope>(
                &mut self,
                scope: Scope<'scope, Timestamp>,
                name: &str,
                since: Antichain<Timestamp>,
                until: Antichain<Timestamp>,
            ) -> (
                Arranged<'scope, TraceFrontier<$name>>,
                Option<ShutdownButton<CapabilitySet<Timestamp>>>,
            ) {
                match self {
                    $name::Local(trace) => {
                        let (arranged, button) =
                            trace.import_frontier_core(scope, name, since.clone(), until.clone());
                        let trace = $name::Local(PaddedTrace::from(trace.unpadded().clone()));
                        let arranged = Arranged {
                            stream: arranged.stream,
                            trace: TraceFrontier::make_from(trace, since.borrow(), until.borrow()),
                        };
                        (arranged, Some(button))
                    }
                    $name::Shared(handle) => {
                        let reader = handle.fresh_reader();
                        let arranged =
                            reader.import_frontier_core(scope, name, since.clone(), until.clone());
                        let trace = $name::Shared(reader);
                        let arranged = Arranged {
                            stream: arranged.stream,
                            trace: TraceFrontier::make_from(trace, since.borrow(), until.borrow()),
                        };
                        (arranged, None)
                    }
                }
            }
        }
    };
}

index_trace!(
    /// The `oks` trace of an index.
    OksTrace,
    RowRowAgent<Timestamp, Diff>,
    SharedOksHandle
);
index_trace!(
    /// The `errs` trace of an index.
    ErrsTrace,
    ErrAgent<Timestamp, Diff>,
    SharedErrsHandle
);

/// Bundles together traces for the successful computations (`oks`), the
/// failed computations (`errs`), additional tokens that should share
/// the lifetime of the bundled traces (`to_drop`).
#[derive(Clone)]
pub struct TraceBundle {
    oks: OksTrace,
    errs: ErrsTrace,
    to_drop: Option<Rc<dyn Any>>,
}

impl TraceBundle {
    /// Constructs a new trace bundle out of an `oks` trace and `errs` trace this runtime maintains.
    pub fn new<O, E>(oks: O, errs: E) -> TraceBundle
    where
        O: Into<PaddedTrace<RowRowAgent<Timestamp, Diff>>>,
        E: Into<PaddedTrace<ErrAgent<Timestamp, Diff>>>,
    {
        TraceBundle {
            oks: OksTrace::Local(oks.into()),
            errs: ErrsTrace::Local(errs.into()),
            to_drop: None,
        }
    }

    /// Constructs a trace bundle over traces the process's other compute runtime publishes.
    pub fn shared(oks: SharedOksHandle, errs: SharedErrsHandle) -> TraceBundle {
        TraceBundle {
            oks: OksTrace::Shared(oks),
            errs: ErrsTrace::Shared(errs),
            to_drop: None,
        }
    }

    /// The traces this runtime maintains, if it maintains them.
    pub fn local(&self) -> Option<(&RowRowAgent<Timestamp, Diff>, &ErrAgent<Timestamp, Diff>)> {
        match (&self.oks, &self.errs) {
            (OksTrace::Local(oks), ErrsTrace::Local(errs)) => {
                Some((oks.unpadded(), errs.unpadded()))
            }
            _ => None,
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
    pub fn oks_mut(&mut self) -> &mut OksTrace {
        &mut self.oks
    }

    /// Returns a mutable reference to the `errs` trace.
    pub fn errs_mut(&mut self) -> &mut ErrsTrace {
        &mut self.errs
    }

    /// Returns mutable references to both traces.
    ///
    /// A reader of both, such as a peek that must rule out errors before it returns rows, cannot
    /// take them one at a time, because each of [`TraceBundle::oks_mut`] and
    /// [`TraceBundle::errs_mut`] borrows the whole bundle.
    pub fn oks_errs_mut(&mut self) -> (&mut OksTrace, &mut ErrsTrace) {
        (&mut self.oks, &mut self.errs)
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
    ///
    /// Panics on traces this runtime does not maintain, whose compaction its peer owns.
    pub fn into_padded(self) -> Self {
        let (OksTrace::Local(oks), ErrsTrace::Local(errs)) = (self.oks, self.errs) else {
            panic!("only a trace this runtime maintains can be padded");
        };
        Self {
            oks: OksTrace::Local(oks.into_padded()),
            errs: ErrsTrace::Local(errs.into_padded()),
            to_drop: self.to_drop,
        }
    }
}
