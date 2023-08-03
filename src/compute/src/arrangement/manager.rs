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
use differential_dataflow::trace::TraceReader;
use mz_repr::{GlobalId, Timestamp};
use timely::progress::frontier::{Antichain, AntichainRef};

use crate::metrics::TraceMetrics;
use crate::typedefs::{ErrsHandle, KeysValsHandle};

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
    pub fn del_trace(&mut self, id: &GlobalId) -> bool {
        self.traces.remove(id).is_some()
    }

    /// Removes all managed traces.
    pub fn del_all_traces(&mut self) {
        self.traces.clear();
    }
}

/// Bundles together traces for the successful computations (`oks`), the
/// failed computations (`errs`), additional tokens that should share
/// the lifetime of the bundled traces (`to_drop`), and a permutation
/// describing how to reconstruct the original row (`permutation`).
#[derive(Clone)]
pub struct TraceBundle {
    oks: KeysValsHandle,
    errs: ErrsHandle,
    to_drop: Option<Rc<dyn Any>>,
}

impl TraceBundle {
    /// Constructs a new trace bundle out of an `oks` trace and `errs` trace.
    pub fn new(oks: KeysValsHandle, errs: ErrsHandle) -> TraceBundle {
        TraceBundle {
            oks,
            errs,
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
    pub fn oks_mut(&mut self) -> &mut KeysValsHandle {
        &mut self.oks
    }

    /// Returns a mutable reference to the `errs` trace.
    pub fn errs_mut(&mut self) -> &mut ErrsHandle {
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
}
