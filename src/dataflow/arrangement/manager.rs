// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Management of arrangements across dataflows.

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use std::collections::{BTreeMap, HashMap};

use dataflow_types::{Diff, Timestamp};

use expr::RelationExpr;

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
    /// A map from user-created trace names to the collection the trace belongs to
    pub name_to_collection: HashMap<String, String>,
}

impl Default for TraceManager {
    fn default() -> Self {
        TraceManager {
            traces: HashMap::new(),
            name_to_collection: HashMap::new(),
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
    pub fn get_by_keys(
        &self,
        name: &str,
        expr: &Option<RelationExpr>,
        columns: &[usize],
    ) -> Option<&WithDrop<KeysValsHandle>> {
        self.traces.get(name)?.by_keys.get(&CollectionTraceKey {
            expr: expr.clone(),
            columns: columns.to_vec(),
        })
    }

    /// Returns a copy of a by_key arrangement, should it exist.
    #[allow(dead_code)]
    pub fn get_by_keys_mut(
        &mut self,
        name: &str,
        expr: &Option<RelationExpr>,
        columns: &[usize],
    ) -> Option<&mut WithDrop<KeysValsHandle>> {
        self.traces
            .get_mut(name)?
            .by_keys
            .get_mut(&CollectionTraceKey {
                expr: expr.clone(),
                columns: columns.to_vec(),
            })
    }

    /// Returns a copy of all by_key arrangements, should they exist.
    pub fn get_all_keyed(
        &mut self,
        name: &str,
    ) -> Option<impl Iterator<Item = (&CollectionTraceKey, &mut WithDrop<KeysValsHandle>)>> {
        Some(self.traces.get_mut(name)?.by_keys.iter_mut())
    }

    /// get the default arrangement, which is by primary key
    pub fn get_default(&mut self, name: &str) -> Option<&WithDrop<KeysValsHandle>> {
        if let Some(collection) = self.traces.get_mut(name) {
            collection.by_keys.get(&collection.primary_key)
        } else {
            None
        }
    }

    /// Binds a by_keys arrangement.
    #[allow(dead_code)]
    pub fn set_by_keys(&mut self, name: String, keys: &[usize], trace: WithDrop<KeysValsHandle>) {
        //Currently it is assumed that the first arrangement for a collection is the one
        //keyed by the primary keys
        self.traces
            .entry(name)
            .or_insert_with(|| CollectionTraces::new(keys.to_vec()))
            .by_keys
            .insert(
                CollectionTraceKey {
                    expr: None,
                    columns: keys.to_vec(),
                },
                trace,
            );
    }

    /// Add a user created index on a collection
    pub fn set_named_by_keys(
        &mut self,
        collection_name: String,
        trace_name: String,
        expr: Option<RelationExpr>,
        columns: &[usize],
        trace: WithDrop<KeysValsHandle>,
    ) {
        // The collection should exist already
        let collection_traces = self.traces.get_mut(&collection_name).unwrap();
        let collection_trace_key = CollectionTraceKey {
            expr,
            columns: columns.to_vec(),
        };
        collection_traces
            .keys_by_name
            .insert(trace_name.clone(), collection_trace_key.clone());
        collection_traces
            .by_keys
            .insert(collection_trace_key, trace);
        self.name_to_collection.insert(trace_name, collection_name);
    }

    /// When a user requests to create an arrangement that already exists,
    /// map the name to the extant arrangement and increment the count on the arrangement
    pub fn set_alias(
        &mut self,
        collection_name: String,
        trace_name: String,
        expr: Option<RelationExpr>,
        columns: &[usize],
    ) {
        let collection_traces = self.traces.get_mut(&collection_name).unwrap();
        let collection_trace_key = CollectionTraceKey {
            expr,
            columns: columns.to_vec(),
        };
        collection_traces
            .by_keys
            .get_mut(&collection_trace_key)
            .unwrap()
            .increment_count();
        collection_traces
            .keys_by_name
            .insert(trace_name.clone(), collection_trace_key);
        self.name_to_collection.insert(trace_name, collection_name);
    }

    /// Removes all of a collection's traces
    pub fn del_collection_traces(&mut self, name: &str) -> Option<CollectionTraces> {
        if let Some(collection) = self.traces.get(name) {
            for user_trace_name in collection.keys_by_name.keys() {
                self.name_to_collection.remove(user_trace_name);
            }
        }
        self.traces.remove(name)
    }

    /// Removes a named trace. Since a particular trace can have multiple names,
    /// what actually happens is that the counter on the physical trace is decremented,
    /// and the name is removed from the manager. Return whether the named trace was found
    pub fn del_index_trace(&mut self, name: &str) -> bool {
        println!("found index name {}", name);
        let result = if let Some(collection_name) = self.name_to_collection.get(name) {
            println!("found index name {}", name);
            let collection_trace = self.traces.get_mut(collection_name).unwrap();
            let internal_key = collection_trace.keys_by_name.get(name).unwrap();
            if let Some(trace) = collection_trace.by_keys.get_mut(internal_key) {
                if trace.decrement_count() {
                    collection_trace.by_keys.remove(internal_key);
                }
            } else {
                unreachable!()
            };
            collection_trace.keys_by_name.remove(name);
            true
        } else {
            false
        };
        self.name_to_collection.remove(name);
        result
    }

    /// Removes all remnants of all named traces.
    pub fn del_all_traces(&mut self) {
        self.traces.clear();
        self.name_to_collection.clear();
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CollectionTraceKey {
    pub expr: Option<RelationExpr>,
    pub columns: Vec<usize>,
}

/// Maintained traces for a collection.
pub struct CollectionTraces {
    primary_key: CollectionTraceKey,
    /// The collection arranged by various keys, indicated by a sequence of column identifiers.
    by_keys: HashMap<CollectionTraceKey, WithDrop<KeysValsHandle>>,
    /// Maps user-defined traces names to the internal keys used to find the trace
    keys_by_name: BTreeMap<String, CollectionTraceKey>,
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

    fn new(primary_key: Vec<usize>) -> Self {
        Self {
            primary_key: CollectionTraceKey {
                expr: None,
                columns: primary_key,
            },
            by_keys: HashMap::new(),
            keys_by_name: BTreeMap::new(),
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
    counter: usize,
}

impl<T> WithDrop<T> {
    /// Creates a new wrapper with an item to drop.
    pub fn new<S: std::any::Any>(element: T, to_drop: S) -> Self {
        Self {
            element,
            to_drop: Some(std::rc::Rc::new(Box::new(to_drop))),
            counter: 1,
        }
    }

    /// Read access to the drop token, so that others can clone it.
    pub fn to_drop(&self) -> &Option<std::rc::Rc<Box<dyn std::any::Any>>> {
        &self.to_drop
    }

    fn increment_count(&mut self) {
        self.counter += 1;
    }

    fn decrement_count(&mut self) -> bool {
        self.counter -= 1;
        self.counter == 0
    }
}

impl<T> From<T> for WithDrop<T> {
    fn from(element: T) -> Self {
        Self {
            element,
            to_drop: None,
            counter: 1,
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
