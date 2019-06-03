// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
// use differential_dataflow::trace::TraceReader;
use std::collections::HashMap;
// use timely::dataflow::operators::probe::Handle as ProbeHandle;
// use timely::dataflow::operators::probe::Probe;
// use timely::dataflow::Scope;

use super::types::{Diff, Plan, ScalarExpr};
use crate::clock::Timestamp;
use crate::repr::Datum;

pub type TraceKeyHandle<K, T, R> = TraceAgent<OrdKeySpine<K, T, R>>;
pub type TraceValHandle<K, V, T, R> = TraceAgent<OrdValSpine<K, V, T, R>>;
pub type KeysOnlyHandle = TraceKeyHandle<Vec<Datum>, Timestamp, Diff>;
pub type KeysValsHandle = TraceValHandle<Vec<Datum>, Vec<Datum>, Timestamp, Diff>;

// This should be Box<FnOnce>, but requires Rust 1.35 (maybe):
// https://github.com/rust-lang/rust/issues/28796
pub type DeleteCallback = Box<FnMut()>;

/// A map from plans to cached arrangements.
///
/// A `TraceManager` stores maps from plans to various arranged representations
/// of the collection the plan computes. These arrangements can either be unkeyed,
/// or keyed by some expression.
pub struct TraceManager {
    traces: HashMap<
        Plan,
        (
            Option<TraceInfoUnkeyed>,
            HashMap<Vec<ScalarExpr>, TraceInfoKeyed>,
        ),
    >,
}

struct TraceInfoKeyed {
    trace: KeysValsHandle,
    delete_callback: DeleteCallback,
}

struct TraceInfoUnkeyed {
    trace: KeysOnlyHandle,
    delete_callback: DeleteCallback,
}

impl TraceManager {
    pub fn new() -> Self {
        TraceManager {
            traces: HashMap::new(),
        }
    }

    /// TODO: Sort out time domains.

    // /// Give managed traces permission to compact.
    // pub fn advance_time(&mut self, time: &Timestamp) {

    //     use differential_dataflow::trace::TraceReader;

    //     let frontier = &[time.clone()];
    //     for trace in self.keyed_traces.values_mut() {
    //         trace.0.as_mut().map(|t| t.advance_by(frontier));
    //         trace.0.as_mut().map(|t| t.distinguish_since(frontier));

    //         for keyed in trace.1.values_mut() {
    //             keyed.advance_by(frontier);
    //             keyed.distinguish_since(frontier);
    //         }
    //     }

    // }

    pub fn get_trace(&self, plan: &Plan) -> Option<KeysOnlyHandle> {
        self.traces
            .get(plan)
            .and_then(|x| x.0.as_ref().map(|ti| ti.trace.clone()))
    }

    pub fn get_keyed_trace(&self, plan: &Plan, key: &[ScalarExpr]) -> Option<KeysValsHandle> {
        self.traces
            .get(plan)
            .and_then(|x| x.1.get(key).map(|ti| ti.trace.clone()))
    }

    pub fn set_trace(
        &mut self,
        plan: &Plan,
        trace: KeysOnlyHandle,
        delete_callback: DeleteCallback,
    ) {
        let trace_info = TraceInfoUnkeyed {
            trace,
            delete_callback,
        };
        self.traces
            .insert(plan.clone(), (Some(trace_info), HashMap::new()));
    }

    #[allow(dead_code)]
    pub fn set_keyed_trace(
        &mut self,
        plan: &Plan,
        key: &[ScalarExpr],
        trace: KeysValsHandle,
        delete_callback: DeleteCallback,
    ) {
        let trace_info = TraceInfoKeyed {
            trace,
            delete_callback,
        };
        self.traces
            .entry(plan.clone())
            .or_insert((None, HashMap::new()))
            .1
            .insert(key.to_vec(), trace_info);
    }

    pub fn del_trace(&mut self, plan: &Plan) {
        if let Some((unkeyed, maps)) = self.traces.remove(plan) {
            if let Some(mut unkeyed) = unkeyed {
                (unkeyed.delete_callback)();
            }
            for (_, mut keyed) in maps.into_iter() {
                (keyed.delete_callback)();
            }
        }
    }

    pub fn del_all_traces(&mut self) {
        for (_, (unkeyed, maps)) in self.traces.drain() {
            if let Some(mut unkeyed) = unkeyed {
                (unkeyed.delete_callback)();
            }
            for (_, mut keyed) in maps.into_iter() {
                (keyed.delete_callback)();
            }
        }
    }
}
