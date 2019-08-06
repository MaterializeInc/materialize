// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Management of arrangements across dataflows.

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use std::collections::HashMap;

use crate::dataflow::types::{Diff, Timestamp};
use crate::repr::Datum;

pub type KeysOnlySpine = OrdKeySpine<Vec<Datum>, Timestamp, Diff>;
#[allow(dead_code)]
pub type KeysValsSpine = OrdValSpine<Vec<Datum>, Vec<Datum>, Timestamp, Diff>;
pub type TraceKeyHandle<K, T, R> = TraceAgent<OrdKeySpine<K, T, R>>;
pub type TraceValHandle<K, V, T, R> = TraceAgent<OrdValSpine<K, V, T, R>>;
pub type KeysOnlyHandle = TraceKeyHandle<Vec<Datum>, Timestamp, Diff>;
pub type KeysValsHandle = TraceValHandle<Vec<Datum>, Vec<Datum>, Timestamp, Diff>;

pub type DeleteCallback = Box<FnOnce()>;

/// A map from collection names to cached arrangements.
///
/// A `TraceManager` stores maps from string names to various arranged representations
/// of a collection. These arrangements can either be unkeyed, or keyed by some expression.
pub struct TraceManager {
    /// A map from named collections to maintained traces.
    traces: HashMap<String, CollectionTraces>,
}

impl Default for TraceManager {
    fn default() -> Self {
        TraceManager {
            traces: HashMap::new(),
        }
    }
}

impl TraceManager {
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

    /// Returns a copy of the by_self arrangement, should it exist.
    #[allow(dead_code)]
    pub fn get_by_self(&self, name: &str) -> Option<&KeysOnlyHandle> {
        self.traces.get(name)?.by_self.as_ref().map(|(t, _d)| t)
    }

    /// Returns a copy of the by_self arrangement, should it exist.
    #[allow(dead_code)]
    pub fn get_by_self_mut(&mut self, name: &str) -> Option<&mut KeysOnlyHandle> {
        self.traces.get_mut(name)?.by_self.as_mut().map(|(t, _d)| t)
    }

    /// Binds the by_self arrangement.
    #[allow(dead_code)]
    pub fn set_by_self(
        &mut self,
        name: String,
        trace: KeysOnlyHandle,
        delete_callback: Option<DeleteCallback>,
    ) {
        self.traces
            .entry(name)
            .or_insert_with(CollectionTraces::default)
            .by_self = Some((trace, delete_callback));
    }

    /// Returns a copy of a by_key arrangement, should it exist.
    #[allow(dead_code)]
    pub fn get_by_keys(&self, name: &str, keys: &[usize]) -> Option<&KeysValsHandle> {
        self.traces.get(name)?.by_keys.get(keys).map(|(t, _d)| t)
    }

    /// Returns a copy of a by_key arrangement, should it exist.
    #[allow(dead_code)]
    pub fn get_by_keys_mut(&mut self, name: &str, keys: &[usize]) -> Option<&mut KeysValsHandle> {
        self.traces
            .get_mut(name)?
            .by_keys
            .get_mut(keys)
            .map(|(t, _d)| t)
    }

    #[allow(dead_code)]
    pub fn get_all_keyed(
        &mut self,
        name: &str,
    ) -> Option<impl Iterator<Item = (&Vec<usize>, &mut KeysValsHandle)>> {
        Some(
            self.traces
                .get_mut(name)?
                .by_keys
                .iter_mut()
                .map(|(key, (handle, _d))| (key, handle)),
        )
    }

    /// Binds a by_keys arrangement.
    #[allow(dead_code)]
    pub fn set_by_keys(
        &mut self,
        name: String,
        keys: &[usize],
        trace: KeysValsHandle,
        delete_callback: Option<DeleteCallback>,
    ) {
        self.traces
            .entry(name)
            .or_insert_with(CollectionTraces::default)
            .by_keys
            .insert(keys.to_vec(), (trace, delete_callback));
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
struct CollectionTraces {
    /// The collection arranged "by self", where the key is the record.
    by_self: Option<(KeysOnlyHandle, Option<DeleteCallback>)>,
    /// The collection arranged by various keys, indicated by a sequence of column identifiers.
    by_keys: HashMap<Vec<usize>, (KeysValsHandle, Option<DeleteCallback>)>,
}

impl CollectionTraces {
    /// Advances the frontiers for physical merging to their current limits.
    pub fn merge_physical(
        &mut self,
        antichain: &mut timely::progress::frontier::Antichain<Timestamp>,
    ) {
        use differential_dataflow::trace::TraceReader;
        if let Some((handle, _d)) = &mut self.by_self {
            handle.read_upper(antichain);
            handle.distinguish_since(antichain.elements());
        }
        for (handle, _d) in self.by_keys.values_mut() {
            handle.read_upper(antichain);
            handle.distinguish_since(antichain.elements());
        }
    }
}

impl Default for CollectionTraces {
    fn default() -> Self {
        Self {
            by_self: None,
            by_keys: HashMap::new(),
        }
    }
}

impl Drop for CollectionTraces {
    fn drop(&mut self) {
        for (_keys, (_handle, mut on_delete)) in self.by_keys.drain() {
            on_delete.take().map(|func| func());
        }
        if let Some((_handle, mut on_delete)) = self.by_self.take() {
            on_delete.take().map(|func| func());
        }
    }
}
