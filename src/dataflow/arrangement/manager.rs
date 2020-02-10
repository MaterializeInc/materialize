// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Management of arrangements across dataflows.

use differential_dataflow::operators::arrange::TraceAgent;
use std::collections::HashMap;

use dataflow_types::{Diff, Timestamp};
use expr::GlobalId;
use repr::Row;

use differential_dataflow::trace::implementations::ord::OrdValBatch;
use differential_dataflow::trace::implementations::spine_fueled_neu::Spine;
use std::rc::Rc;
pub type OrdValSpine<K, V, T, R, O = usize> = Spine<K, V, T, R, Rc<OrdValBatch<K, V, T, R, O>>>;

#[allow(dead_code)]
pub type KeysValsSpine = OrdValSpine<Row, Row, Timestamp, Diff>;
pub type TraceValHandle<K, V, T, R> = TraceAgent<OrdValSpine<K, V, T, R>>;
pub type KeysValsHandle = TraceValHandle<Row, Row, Timestamp, Diff>;

/// A map from collection names to cached arrangements.
///
/// A `TraceManager` stores maps from global identifiers to various arranged
/// representations of a collection. These arrangements can either be unkeyed,
/// or keyed by some expression.
pub struct TraceManager {
    /// A map from global identifiers to maintained traces.
    pub traces: HashMap<GlobalId, WithDrop<KeysValsHandle>>,
}

impl Default for TraceManager {
    fn default() -> Self {
        TraceManager {
            traces: HashMap::new(),
        }
    }
}

impl TraceManager {
    /// Performs maintenance work on the managed traces.
    ///
    /// In particular, this method enables the physical merging of batches, so that at most a logarithmic
    /// number of batches need to be maintained. Any new batches introduced after this method is called
    /// will not be physically merged until the method is called again. This is mostly due to limitations
    /// of differential dataflow, which requires users to perform this explicitly; if that changes we may
    /// be able to remove this code.
    pub fn maintenance(&mut self) {
        let mut antichain = timely::progress::frontier::Antichain::new();
        for handle in self.traces.values_mut() {
            use differential_dataflow::trace::TraceReader;
            handle.read_upper(&mut antichain);
            handle.distinguish_since(antichain.elements());
        }
    }

    /// Enables compaction of traces associated with the identifier.
    ///
    /// Compaction may not occur immediately, but once this method is called the
    /// associated traces may not accumulate to the correct quantities for times
    /// not in advance of `frontier`. Users should take care to only rely on
    /// accumulations at times in advance of `frontier`.
    pub fn allow_compaction(&mut self, id: GlobalId, frontier: &[Timestamp]) {
        use differential_dataflow::trace::TraceReader;
        if let Some(val) = self.traces.get_mut(&id) {
            val.advance_by(frontier);
        }
    }

    /// Returns a copy of a by_key arrangement, should it exist.
    #[allow(dead_code)]
    pub fn get(&self, id: &GlobalId) -> Option<&WithDrop<KeysValsHandle>> {
        self.traces.get(&id)
    }

    /// Returns a copy of a by_key arrangement, should it exist.
    #[allow(dead_code)]
    pub fn get_mut(&mut self, id: &GlobalId) -> Option<&mut WithDrop<KeysValsHandle>> {
        self.traces.get_mut(&id)
    }

    /// Binds a by_keys arrangement.
    #[allow(dead_code)]
    pub fn set(&mut self, id: GlobalId, trace: WithDrop<KeysValsHandle>) {
        //Currently it is assumed that the first arrangement for a collection is the one
        //keyed by the primary keys
        self.traces.insert(id, trace);
    }

    /// Removes a trace
    pub fn del_trace(&mut self, id: &GlobalId) -> bool {
        self.traces.remove(&id).is_some()
    }

    /// Removes all remnants of all named traces.
    pub fn del_all_traces(&mut self) {
        self.traces.clear();
    }
}

/// A thin wrapper containing an associated item to drop.
///
/// This type is used for controlled shutdown of dataflows as handles are dropped.
/// The associated `to_drop` will be dropped with the element, and can be observed
/// by other bits of clean-up code.
#[derive(Clone)]
pub struct WithDrop<T> {
    element: T,
    to_drop: Option<std::rc::Rc<Box<dyn std::any::Any>>>,
}

impl<T> WithDrop<T> {
    /// Creates a new wrapper with an item to drop.
    pub fn new<S: std::any::Any>(element: T, to_drop: S) -> Self {
        Self {
            element,
            to_drop: Some(std::rc::Rc::new(Box::new(to_drop))),
        }
    }

    /// Read access to the drop token, so that others can clone it.
    pub fn to_drop(&self) -> &Option<std::rc::Rc<Box<dyn std::any::Any>>> {
        &self.to_drop
    }
}

impl<T> From<T> for WithDrop<T> {
    fn from(element: T) -> Self {
        Self {
            element,
            to_drop: None,
        }
    }
}

impl<T> std::ops::Deref for WithDrop<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.element
    }
}

impl<T> std::ops::DerefMut for WithDrop<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.element
    }
}
