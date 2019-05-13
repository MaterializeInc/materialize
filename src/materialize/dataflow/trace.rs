// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::trace::TraceReader;
use std::collections::HashMap;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::Scope;

use super::types::Diff;
use crate::clock::Timestamp;
use crate::repr::Datum;

pub type TraceKeyHandle<K, T, R> = TraceAgent<K, (), T, R, OrdKeySpine<K, T, R>>;

pub type KeysOnlyHandle = TraceKeyHandle<Datum, Timestamp, Diff>;

pub type DeleteCallback = Box<FnMut()>;

pub struct TraceManager {
    traces: HashMap<String, TraceInfo>,
}

struct TraceInfo {
    trace: KeysOnlyHandle,
    probe: ProbeHandle<Timestamp>,
    delete_callback: DeleteCallback,
}

impl TraceManager {
    pub fn new() -> Self {
        TraceManager {
            traces: HashMap::new(),
        }
    }

    pub fn get_trace(&self, name: String) -> Option<(KeysOnlyHandle)> {
        self.traces.get(&name).map(|ti| ti.trace.clone())
    }

    pub fn get_trace_and_probe(
        &self,
        name: String,
    ) -> Option<(KeysOnlyHandle, ProbeHandle<Timestamp>)> {
        self.traces
            .get(&name)
            .map(|ti| (ti.trace.clone(), ti.probe.clone()))
    }

    pub fn set_trace<G>(
        &mut self,
        name: String,
        arrangement: &Arranged<G, Datum, (), Diff, KeysOnlyHandle>,
        delete_callback: DeleteCallback,
    ) where
        G: Scope<Timestamp = Timestamp>,
    {
        let mut trace = arrangement.trace.clone();
        trace.distinguish_since(&[]);
        let probe = arrangement.stream.probe();
        self.traces.insert(
            name.clone(),
            TraceInfo {
                trace,
                probe,
                delete_callback,
            },
        );
    }

    pub fn del_trace(&mut self, name: &str) {
        if let Some(mut ti) = self.traces.remove(name) {
            (ti.delete_callback)();
        }
    }

    pub fn del_all_traces(&mut self) {
        for (_, mut ti) in self.traces.drain() {
            (ti.delete_callback)();
        }
    }
}
