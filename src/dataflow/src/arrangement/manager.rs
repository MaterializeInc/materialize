// Copyright Materialize, Inc. All rights reserved.
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

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::{OrdKeyBatch, OrdValBatch};
use differential_dataflow::trace::implementations::spine_fueled_neu::Spine;
use differential_dataflow::trace::TraceReader;
use timely::progress::frontier::{Antichain, AntichainRef};

use dataflow_types::{DataflowError, Diff, Timestamp};
use expr::GlobalId;
use repr::Row;

pub type OrdKeySpine<K, T, R, O = usize> = Spine<K, (), T, R, Rc<OrdKeyBatch<K, T, R, O>>>;
pub type OrdValSpine<K, V, T, R, O = usize> = Spine<K, V, T, R, Rc<OrdValBatch<K, V, T, R, O>>>;
pub type TraceKeyHandle<K, T, R> = TraceAgent<OrdKeySpine<K, T, R>>;
pub type TraceValHandle<K, V, T, R> = TraceAgent<OrdValSpine<K, V, T, R>>;
pub type KeysValsHandle = TraceValHandle<Row, Row, Timestamp, Diff>;
pub type ErrsHandle = TraceKeyHandle<DataflowError, Timestamp, Diff>;

use lazy_static::lazy_static;
use prometheus::core::{AtomicF64, AtomicU64};
use prometheus::{
    register_counter_vec, register_uint_gauge_vec, CounterVec, DeleteOnDropCounter,
    DeleteOnDropGauge, UIntGaugeVec,
};
use std::time::Instant;

struct MaintenanceMetrics {
    /// 1 if maintenance is happening; 0 if not.
    /// if maintenance turns out to take a very long time, this will allow us
    /// to gain a sense that materialize is stuck on maintenance before the
    /// maintenance completes
    doing_maintenance: DeleteOnDropGauge<'static, AtomicU64>,
    /// total time spent doing maintenance. More useful in the general case.
    total_maintenance_time: DeleteOnDropCounter<'static, AtomicF64>,
}

impl MaintenanceMetrics {
    fn new(worker_id: &str, arrangement_id: &str) -> Self {
        lazy_static! {
            static ref DOING_MAINTENANCE: UIntGaugeVec = register_uint_gauge_vec!(
                "mz_arrangement_maintenance_active_info",
                "Whether or not maintenance is occuring",
                &["worker_id", "arrangement_id"]
            )
            .unwrap();
            static ref TOTAL_MAINTENANCE_TIME: CounterVec = register_counter_vec!(
                "mz_arrangement_maintenance_seconds_total",
                "The total time spent maintaining an arrangement",
                &["worker_id", "arrangement_id"]
            )
            .unwrap();
        }
        let labels = &[worker_id, arrangement_id];
        MaintenanceMetrics {
            doing_maintenance: DeleteOnDropGauge::new_with_error_handler(
                DOING_MAINTENANCE.with_label_values(labels),
                &DOING_MAINTENANCE,
                |e, v| log::warn!("unable to delete metric {}: {}", v.fq_name(), e),
            ),
            total_maintenance_time: DeleteOnDropCounter::new_with_error_handler(
                TOTAL_MAINTENANCE_TIME.with_label_values(labels),
                &TOTAL_MAINTENANCE_TIME,
                |e, v| log::warn!("unable to delete metric {}: {}", v.fq_name(), e),
            ),
        }
    }
}

/// A `TraceManager` stores maps from global identifiers to the primary arranged
/// representation of that collection.
pub struct TraceManager {
    pub traces: HashMap<GlobalId, TraceBundle>,
    worker_id: usize,
    maintenance_metrics: HashMap<GlobalId, MaintenanceMetrics>,
}

impl TraceManager {
    pub fn new(worker_id: usize) -> Self {
        TraceManager {
            traces: HashMap::new(),
            worker_id,
            maintenance_metrics: HashMap::new(),
        }
    }

    /// Performs maintenance work on the managed traces.
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

            //signal that maintenance is happening
            maintenance_metrics.doing_maintenance.set(1);
            let now = Instant::now();

            bundle.oks.read_upper(&mut antichain);
            bundle.oks.distinguish_since(antichain.borrow());
            bundle.errs.read_upper(&mut antichain);
            bundle.errs.distinguish_since(antichain.borrow());

            maintenance_metrics
                .total_maintenance_time
                .inc_by(now.elapsed().as_secs_f64());
            // signal that maintenance has ended
            maintenance_metrics.doing_maintenance.set(0);
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
            bundle.oks.advance_by(frontier);
            bundle.errs.advance_by(frontier);
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
            MaintenanceMetrics::new(&self.worker_id.to_string(), &id.to_string()),
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
        self.traces.clear();
    }
}

/// Bundles together traces for the successful computations (`oks`), the
/// failed computations (`errs`), and additional tokens that should share
/// the lifetime of the bundled traces (`to_drop`).
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
            oks: self.oks,
            errs: self.errs,
            to_drop: Some(Rc::new(Box::new(to_drop))),
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
