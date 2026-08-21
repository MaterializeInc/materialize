// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The traces an index peek reads, from an arrangement this runtime maintains or one the sharing
//! registry publishes.

use differential_dataflow::trace::TraceReader;
use mz_repr::{Diff, GlobalId, Timestamp};
use timely::progress::frontier::AntichainRef;

use crate::arrangement::manager::{PaddedTrace, TraceBundle};
use crate::compute_state::error_scan::ErrsHandle;
use crate::shared_trace::{SharedErrsHandle, SharedOksHandle};
use crate::sharing::ArrangementSharingRegistry;
use crate::typedefs::{ErrSpine, RowRowAgent, RowRowSpine};

/// Where an index peek finds the traces that answer it.
pub(super) enum IndexTraces {
    /// Traces this runtime maintains, pinned for the peek's life.
    Local(TraceBundle),
    /// An arrangement the sharing registry publishes, resolved on every attempt. A parked peek
    /// holds nothing of the arrangement, so an unpublished slot registers no hold at the minimum.
    Shared {
        registry: ArrangementSharingRegistry,
        worker_index: usize,
    },
}

impl IndexTraces {
    /// Handles on the traces of `id` for one attempt, or `None` while a shared index is
    /// unpublished.
    ///
    /// Both variants hand out owned handles so the scan can carry them off the worker. A local
    /// handle is a clone of the pinned one, which registers a hold the pinned one already keeps.
    pub(super) fn resolve(&mut self, id: GlobalId) -> Option<(PeekOks, PeekErrs)> {
        match self {
            IndexTraces::Local(bundle) => {
                let (oks, errs) = bundle.oks_errs_mut();
                Some((PeekOks::Local(oks.clone()), PeekErrs::Local(errs.clone())))
            }
            IndexTraces::Shared {
                registry,
                worker_index,
            } => registry
                .handles(&id, *worker_index)
                .map(|(oks, errs)| (PeekOks::Shared(oks), PeekErrs::Shared(errs))),
        }
    }
}

/// The ok trace an index peek reads.
pub(super) enum PeekOks {
    Local(PaddedTrace<RowRowAgent<Timestamp, Diff>>),
    Shared(SharedOksHandle),
}

/// The error trace an index peek reads.
pub(super) enum PeekErrs {
    Local(ErrsHandle),
    Shared(SharedErrsHandle),
}

/// Both variants read the same batch type, so the enum is a `TraceReader` by delegation.
macro_rules! delegate_trace_reader {
    ($ty:ident, $spine:ty) => {
        impl TraceReader for $ty {
            type Time = Timestamp;
            type Batch = <$spine as TraceReader>::Batch;

            fn set_logical_compaction(&mut self, frontier: AntichainRef<Timestamp>) {
                match self {
                    $ty::Local(trace) => trace.set_logical_compaction(frontier),
                    $ty::Shared(trace) => trace.set_logical_compaction(frontier),
                }
            }

            fn get_logical_compaction(&mut self) -> AntichainRef<'_, Timestamp> {
                match self {
                    $ty::Local(trace) => trace.get_logical_compaction(),
                    $ty::Shared(trace) => trace.get_logical_compaction(),
                }
            }

            fn set_physical_compaction(&mut self, frontier: AntichainRef<Timestamp>) {
                match self {
                    $ty::Local(trace) => trace.set_physical_compaction(frontier),
                    $ty::Shared(trace) => trace.set_physical_compaction(frontier),
                }
            }

            fn get_physical_compaction(&mut self) -> AntichainRef<'_, Timestamp> {
                match self {
                    $ty::Local(trace) => trace.get_physical_compaction(),
                    $ty::Shared(trace) => trace.get_physical_compaction(),
                }
            }

            fn map_batches<F: FnMut(&Self::Batch)>(&self, f: F) {
                match self {
                    $ty::Local(trace) => trace.map_batches(f),
                    $ty::Shared(trace) => trace.map_batches(f),
                }
            }

            fn batches_through(
                &mut self,
                upper: AntichainRef<Timestamp>,
            ) -> Option<Vec<Self::Batch>> {
                match self {
                    $ty::Local(trace) => trace.batches_through(upper),
                    $ty::Shared(trace) => trace.batches_through(upper),
                }
            }
        }
    };
}

delegate_trace_reader!(PeekOks, RowRowSpine<Timestamp, Diff>);
delegate_trace_reader!(PeekErrs, ErrSpine<Timestamp, Diff>);
