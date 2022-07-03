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
use std::collections::HashMap;
use std::rc::Rc;

use differential_dataflow::trace::TraceReader;
use mz_ore::metric;
use mz_ore::metrics::{
    CounterVec, CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt,
    MetricsRegistry, UIntGaugeVec,
};
use timely::progress::frontier::{Antichain, AntichainRef};

use mz_compute_client::{ErrsHandle, KeysValsHandle};
use mz_repr::{GlobalId, Timestamp};

use prometheus::core::{AtomicF64, AtomicU64};
use std::time::Instant;

/// Base metrics for arrangements.
#[derive(Clone, Debug)]
pub struct TraceMetrics {
    total_maintenance_time: CounterVec,
    doing_maintenance: UIntGaugeVec,
}

impl TraceMetrics {
    /// TODO(undocumented)
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            total_maintenance_time: registry.register(metric!(
                name: "mz_arrangement_maintenance_seconds_total",
                help: "The total time spent maintaining an arrangement",
                var_labels: ["worker_id", "arrangement_id"],
            )),
            doing_maintenance: registry.register(metric!(
                name: "mz_arrangement_maintenance_active_info",
                help: "Whether or not maintenance is currently occurring",
                var_labels: ["worker_id"],
            )),
        }
    }

    fn maintenance_time_metric(
        &self,
        worker_id: usize,
        id: GlobalId,
    ) -> DeleteOnDropCounter<'static, AtomicF64, Vec<String>> {
        self.total_maintenance_time
            .get_delete_on_drop_counter(vec![worker_id.to_string(), id.to_string()])
    }

    fn maintenance_flag_metric(
        &self,
        worker_id: usize,
    ) -> DeleteOnDropGauge<'static, AtomicU64, Vec<String>> {
        self.doing_maintenance
            .get_delete_on_drop_gauge(vec![worker_id.to_string()])
    }
}

struct MaintenanceMetrics {
    /// total time spent doing maintenance. More useful in the general case.
    total_maintenance_time: DeleteOnDropCounter<'static, AtomicF64, Vec<String>>,
}

impl MaintenanceMetrics {
    fn new(metrics: &TraceMetrics, worker_id: usize, arrangement_id: GlobalId) -> Self {
        MaintenanceMetrics {
            total_maintenance_time: metrics.maintenance_time_metric(worker_id, arrangement_id),
        }
    }
}

/// A `TraceManager` stores maps from global identifiers to the primary arranged
/// representation of that collection.
pub struct TraceManager {
    pub(crate) traces: HashMap<GlobalId, TraceBundle>,
    worker_id: usize,
    maintenance_metrics: HashMap<GlobalId, MaintenanceMetrics>,
    /// 1 if this worker is currently doing maintenance.
    ///
    /// If maintenance turns out to take a very long time, this will allow us
    /// to gain a sense that materialize is stuck on maintenance before the
    /// maintenance completes
    doing_maintenance: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    metrics: TraceMetrics,
}

impl TraceManager {
    /// TODO(undocumented)
    pub fn new(metrics: TraceMetrics, worker_id: usize) -> Self {
        let doing_maintenance = metrics.maintenance_flag_metric(worker_id);
        TraceManager {
            traces: HashMap::new(),
            worker_id,
            metrics,
            maintenance_metrics: HashMap::new(),
            doing_maintenance,
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
        let mut antichain = Antichain::new();
        for (arrangement_id, bundle) in self.traces.iter_mut() {
            // Update maintenance metrics
            // Entry is guaranteed to exist as it gets created when we initialize the partition.
            let maintenance_metrics = self.maintenance_metrics.get_mut(arrangement_id).unwrap();

            // signal that maintenance is happening
            self.doing_maintenance.set(1);
            let now = Instant::now();

            bundle.oks.read_upper(&mut antichain);
            bundle.oks.set_physical_compaction(antichain.borrow());
            bundle.errs.read_upper(&mut antichain);
            bundle.errs.set_physical_compaction(antichain.borrow());

            maintenance_metrics
                .total_maintenance_time
                .inc_by(now.elapsed().as_secs_f64());
            // signal that maintenance has ended
            self.doing_maintenance.set(0);
        }
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
        self.traces.get(&id)
    }

    /// Returns a mutable reference to the trace for `id`, should it
    /// exist.
    pub fn get_mut(&mut self, id: &GlobalId) -> Option<&mut TraceBundle> {
        self.traces.get_mut(&id)
    }

    /// Binds the arrangement for `id` to `trace`.
    pub fn set(&mut self, id: GlobalId, trace: TraceBundle) {
        self.maintenance_metrics.insert(
            id,
            MaintenanceMetrics::new(&self.metrics, self.worker_id, id),
        );
        self.traces.insert(id, trace);
    }

    /// Removes the trace for `id`.
    pub fn del_trace(&mut self, id: &GlobalId) -> bool {
        self.maintenance_metrics.remove(id);
        self.traces.remove(&id).is_some()
    }

    /// Removes all managed traces.
    pub fn del_all_traces(&mut self) {
        self.maintenance_metrics.clear();
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
}
