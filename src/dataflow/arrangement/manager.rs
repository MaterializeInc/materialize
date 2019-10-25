// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Management of arrangements across dataflows.

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use std::collections::HashMap;

use dataflow_types::{Diff, Timestamp};
use repr::Row;

#[allow(dead_code)]
pub type KeysValsSpine = OrdValSpine<Row, Row, Timestamp, Diff>;
pub type TraceValHandle<K, V, T, R> = TraceAgent<OrdValSpine<K, V, T, R>>;
pub type KeysValsHandle = TraceValHandle<Row, Row, Timestamp, Diff>;

/// A map from collection names to cached arrangements.
///
/// A `TraceManager` stores maps from string names to various arranged representations
/// of a collection. These arrangements can either be unkeyed, or keyed by some expression.
pub struct TraceManager {
    /// A map from named collections to maintained traces.
    pub traces: HashMap<String, CollectionTraces>,
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
        for collection_traces in self.traces.values_mut() {
            collection_traces.merge_physical(&mut antichain);
        }
    }

    /// Enables compaction of traces associated with the name.
    ///
    /// Compaction may not occur immediately, but once this method is called the
    /// associated traces may not accumulate to the correct quantities for times
    /// not in advance of `frontier`. Users should take care to only rely on
    /// accumulations at times in advance of `frontier`.
    pub fn allow_compaction(&mut self, name: &str, frontier: &[Timestamp]) {
        if let Some(val) = self.traces.get_mut(name) {
            val.merge_logical(frontier);
        }
    }

    /// Returns a copy of a by_key arrangement, should it exist.
    #[allow(dead_code)]
    pub fn get_by_keys(&self, name: &str, keys: &[usize]) -> Option<&WithDrop<KeysValsHandle>> {
        self.traces.get(name)?.by_keys.get(keys)
    }

    /// Returns a copy of a by_key arrangement, should it exist.
    #[allow(dead_code)]
    pub fn get_by_keys_mut(
        &mut self,
        name: &str,
        keys: &[usize],
    ) -> Option<&mut WithDrop<KeysValsHandle>> {
        self.traces.get_mut(name)?.by_keys.get_mut(keys)
    }

    /// Returns a copy of all by_key arrangements, should they exist.
    pub fn get_all_keyed(
        &mut self,
        name: &str,
    ) -> Option<impl Iterator<Item = (&Vec<usize>, &mut WithDrop<KeysValsHandle>)>> {
        Some(self.traces.get_mut(name)?.by_keys.iter_mut())
    }

    /// Binds a by_keys arrangement.
    #[allow(dead_code)]
    pub fn set_by_keys(&mut self, name: String, keys: &[usize], trace: WithDrop<KeysValsHandle>) {
        self.traces
            .entry(name)
            .or_insert_with(CollectionTraces::default)
            .by_keys
            .insert(keys.to_vec(), trace);
    }

    /// Removes all remnants of a named trace.
    pub fn del_trace(&mut self, name: &str) {
        self.traces.remove(name);
    }

    /// Removes all remnants of all named traces.
    pub fn del_all_traces(&mut self) {
        self.traces.clear();
    }
}

/// Maintained traces for a collection.
pub struct CollectionTraces {
    /// The collection arranged by various keys, indicated by a sequence of column identifiers.
    by_keys: HashMap<Vec<usize>, WithDrop<KeysValsHandle>>,
}

impl CollectionTraces {
    /// Advances the frontiers for physical merging to their current limits.
    pub fn merge_physical(
        &mut self,
        antichain: &mut timely::progress::frontier::Antichain<Timestamp>,
    ) {
        use differential_dataflow::trace::TraceReader;
        for handle in self.by_keys.values_mut() {
            handle.read_upper(antichain);
            handle.distinguish_since(antichain.elements());
        }
    }

    /// Advances the frontiers for logical merging to the supplied frontier limit.
    ///
    /// Logical compaction does not immediately occur, rather it happens only when
    /// the next physical merge happens, and users should take care to ensure that
    /// the times observed in traces may need to be advanced to this frontier.
    pub fn merge_logical(&mut self, frontier: &[Timestamp]) {
        use differential_dataflow::trace::TraceReader;
        for handle in self.by_keys.values_mut() {
            handle.advance_by(frontier);
        }
    }
}

impl Default for CollectionTraces {
    fn default() -> Self {
        Self {
            by_keys: HashMap::new(),
        }
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
