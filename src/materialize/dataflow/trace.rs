// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use std::collections::HashMap;

use super::types::{Diff, Timestamp};
use crate::repr::Datum;

pub type TraceKeyHandle<K, T, R> = TraceAgent<OrdKeySpine<K, T, R>>;
#[allow(dead_code)]
pub type TraceValHandle<K, V, T, R> = TraceAgent<OrdValSpine<K, V, T, R>>;
pub type KeysOnlyHandle = TraceKeyHandle<Vec<Datum>, Timestamp, Diff>;
#[allow(dead_code)]
pub type KeysValsHandle = TraceValHandle<Vec<Datum>, Vec<Datum>, Timestamp, Diff>;

pub type DeleteCallback = Box<FnOnce()>;

/// A map from relation_exprs to cached arrangements.
///
/// A `TraceManager` stores maps from relation_exprs to various arranged representations
/// of the collection the relation_expr computes. These arrangements can either be unkeyed,
/// or keyed by some expression.
pub struct TraceManager {
    traces: HashMap<String, TraceInfo>,
}

struct TraceInfo {
    trace: KeysOnlyHandle,
    delete_callback: DeleteCallback,
}

impl TraceManager {
    pub fn new() -> Self {
        TraceManager {
            traces: HashMap::new(),
        }
    }

    // TODO: Sort out time domains.
    //
    // /// Give managed traces permission to compact.
    // pub fn advance_time(&mut self, time: &Timestamp) {
    //
    //     use differential_dataflow::trace::TraceReader;
    //
    //     let frontier = &[time.clone()];
    //     for trace in self.keyed_traces.values_mut() {
    //         trace.0.as_mut().map(|t| t.advance_by(frontier));
    //         trace.0.as_mut().map(|t| t.distinguish_since(frontier));
    //
    //         for keyed in trace.1.values_mut() {
    //             keyed.advance_by(frontier);
    //             keyed.distinguish_since(frontier);
    //         }
    //     }
    //
    // }

    pub fn get_trace(&self, name: &str) -> Option<KeysOnlyHandle> {
        self.traces.get(name).map(|ti| ti.trace.clone())
    }

    pub fn set_trace(
        &mut self,
        name: String,
        trace: KeysOnlyHandle,
        delete_callback: DeleteCallback,
    ) {
        let ti = TraceInfo {
            trace,
            delete_callback,
        };
        self.traces.insert(name, ti);
    }

    pub fn del_trace(&mut self, name: &str) {
        if let Some(ti) = self.traces.remove(name) {
            (ti.delete_callback)();
        }
    }

    pub fn del_all_traces(&mut self) {
        for (_, ti) in self.traces.drain() {
            (ti.delete_callback)();
        }
    }
}
