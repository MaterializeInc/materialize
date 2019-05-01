// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::trace::TraceReader;
use std::collections::HashMap;

use crate::clock::Timestamp;
use crate::repr::Datum;
use super::types::Diff;

pub type TraceKeyHandle<K, T, R> = TraceAgent<K, (), T, R, OrdKeySpine<K, T, R>>;

pub type KeysOnlyHandle = TraceKeyHandle<Datum, Timestamp, Diff>;

pub type DeleteCallback = Box<Fn()>;

pub struct TraceManager {
    traces: HashMap<String, (KeysOnlyHandle, DeleteCallback)>,
}

impl TraceManager {
    pub fn new() -> Self {
        TraceManager {
            traces: HashMap::new(),
        }
    }

    pub fn get_trace(&self, name: String) -> Option<KeysOnlyHandle> {
        self.traces.get(&name).map(|(trace, _)| trace.clone())
    }

    pub fn set_trace(
        &mut self,
        name: String,
        handle: &KeysOnlyHandle,
        delete_callback: DeleteCallback,
    ) {
        let mut handle = handle.clone();
        handle.distinguish_since(&[]);
        self.traces.insert(name.clone(), (handle, delete_callback));
    }

    pub fn del_trace(&mut self, name: &str) {
        if let Some((_, callback)) = self.traces.remove(name) {
            callback();
        }
    }
}
