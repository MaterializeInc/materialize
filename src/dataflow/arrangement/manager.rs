// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Management of arrangements across dataflows.

use differential_dataflow::operators::arrange::TraceAgent;
use std::collections::{BTreeMap, HashMap};

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
    pub traces: HashMap<GlobalId, CollectionTraces>,
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

    /// Enables compaction of traces associated with the identifier.
    ///
    /// Compaction may not occur immediately, but once this method is called the
    /// associated traces may not accumulate to the correct quantities for times
    /// not in advance of `frontier`. Users should take care to only rely on
    /// accumulations at times in advance of `frontier`.
    pub fn allow_compaction(&mut self, id: GlobalId, frontier: &[Timestamp]) {
        if let Some(val) = self.traces.get_mut(&id) {
            val.merge_logical(frontier);
        }
    }

    /// Returns a copy of a by_key arrangement, should it exist.
    #[allow(dead_code)]
    pub fn get_by_keys(&self, id: GlobalId, keys: &[usize]) -> Option<&WithDrop<KeysValsHandle>> {
        let collection = self.traces.get(&id)?;
        if let Some(system) = collection.system.get(keys) {
            Some(system)
        } else {
            collection.user.get(keys)
        }
    }

    /// Returns a copy of a by_key arrangement, should it exist.
    #[allow(dead_code)]
    pub fn get_by_keys_mut(
        &mut self,
        id: GlobalId,
        keys: &[usize],
    ) -> Option<&mut WithDrop<KeysValsHandle>> {
        let collection = self.traces.get_mut(&id)?;
        if let Some(system) = collection.system.get_mut(keys) {
            Some(system)
        } else {
            collection.user.get_mut(keys)
        }
    }

    /// Returns a copy of all by_key arrangements, should they exist.
    pub fn get_all_keyed(
        &mut self,
        id: GlobalId,
    ) -> Option<impl Iterator<Item = (&Vec<usize>, &mut WithDrop<KeysValsHandle>)>> {
        let collection_trace = self.traces.get_mut(&id)?;
        Some(
            collection_trace
                .system
                .iter_mut()
                .chain(collection_trace.user.iter_mut()),
        )
    }

    pub fn get_default_with_key(
        &mut self,
        id: GlobalId,
    ) -> Option<(&[usize], &mut WithDrop<KeysValsHandle>)> {
        if let Some(collection) = self.traces.get_mut(&id) {
            Some((
                &collection.default_arr_key,
                collection
                    .system
                    .get_mut(&collection.default_arr_key)
                    .unwrap(),
            ))
        } else {
            None
        }
    }

    /// get the default arrangement, which is by primary key
    pub fn get_default(&self, id: GlobalId) -> Option<&WithDrop<KeysValsHandle>> {
        if let Some(collection) = self.traces.get(&id) {
            collection.system.get(&collection.default_arr_key)
        } else {
            None
        }
    }

    /// Binds a by_keys arrangement.
    #[allow(dead_code)]
    pub fn set_by_keys(&mut self, id: GlobalId, keys: &[usize], trace: WithDrop<KeysValsHandle>) {
        //Currently it is assumed that the first arrangement for a collection is the one
        //keyed by the primary keys
        self.traces
            .entry(id)
            .or_insert_with(|| CollectionTraces::new(keys.to_vec()))
            .system
            .insert(keys.to_vec(), trace);
    }

    /// Add a user created index
    pub fn set_user_created(
        &mut self,
        collection_id: GlobalId,
        keys: &[usize],
        trace: WithDrop<KeysValsHandle>,
    ) {
        // The collection should exist already
        self.traces
            .get_mut(&collection_id)
            .unwrap()
            .user
            .insert(keys.to_vec(), trace);
    }

    /// Removes all of a collection's traces
    pub fn del_collection_traces(&mut self, id: GlobalId) -> Option<CollectionTraces> {
        self.traces.remove(&id)
    }

    /// Removes a user-created trace
    pub fn del_user_trace(&mut self, id: GlobalId, keys: &[usize]) -> bool {
        self.traces
            .get_mut(&id)
            .unwrap()
            .user
            .remove(keys)
            .is_some()
    }

    /// Removes all remnants of all named traces.
    pub fn del_all_traces(&mut self) {
        self.traces.clear();
    }
}

// TODO (wangandi) replace the Vec<usize> keys with Vec<ScalarExpr>
/// Maintained traces for a collection.
pub struct CollectionTraces {
    /// The key for the default system arrangement, which contains all columns in the collection.
    default_arr_key: Vec<usize>,
    /// Arrangements generated by the system
    /// The collection arranged by various keys, indicated by a sequence of column identifiers.
    system: BTreeMap<Vec<usize>, WithDrop<KeysValsHandle>>,
    /// Arrangements generated by the user
    /// The collection arranged by various keys, indicated by a sequence of column identifiers.
    user: BTreeMap<Vec<usize>, WithDrop<KeysValsHandle>>,
}

impl CollectionTraces {
    /// Advances the frontiers for physical merging to their current limits.
    pub fn merge_physical(
        &mut self,
        antichain: &mut timely::progress::frontier::Antichain<Timestamp>,
    ) {
        use differential_dataflow::trace::TraceReader;
        for handle in self.system.values_mut().chain(self.user.values_mut()) {
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
        for handle in self.system.values_mut().chain(self.user.values_mut()) {
            handle.advance_by(frontier);
        }
    }

    fn new(default_arr_key: Vec<usize>) -> Self {
        Self {
            default_arr_key,
            system: BTreeMap::new(),
            user: BTreeMap::new(),
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
