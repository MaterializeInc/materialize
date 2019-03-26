// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::trace::TraceReader;
use std::collections::HashMap;

use super::types::{Diff, Time};
use crate::repr::Datum;

pub type TraceKeyHandle<K, T, R> = TraceAgent<K, (), T, R, OrdKeySpine<K, T, R>>;

pub type KeysOnlyHandle = TraceKeyHandle<Datum, Time, Diff>;

pub struct TraceManager {
    pub traces: HashMap<String, KeysOnlyHandle>,
}

impl TraceManager {
    pub fn new() -> Self {
        TraceManager {
            traces: HashMap::new(),
        }
    }

    pub fn get_trace(&self, name: String) -> Option<KeysOnlyHandle> {
        self.traces.get(&name).map(|handle| handle.clone())
    }

    pub fn set_trace(&mut self, name: String, handle: &KeysOnlyHandle) {
        let mut handle = handle.clone();
        handle.distinguish_since(&[]);
        self.traces.insert(name.clone(), handle);
    }
}
