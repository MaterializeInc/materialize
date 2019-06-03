// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::collections::HashMap;

use timely::dataflow::{Scope, ScopeParent};
use timely::progress::{timestamp::Refines, Timestamp};

use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::Data;
use differential_dataflow::{lattice::Lattice, Collection};

/// A trace handle for key-only data.
pub type TraceKeyHandle<K, T, R> = TraceAgent<OrdKeySpine<K, T, R>>;
/// A trace handle for key-value data.
pub type TraceValHandle<K, V, T, R> = TraceAgent<OrdValSpine<K, V, T, R>>;

type Diff = isize;

// Local type definition to avoid the horror in signatures.
type Arrangement<S, V> =
    Arranged<S, TraceValHandle<Vec<V>, Vec<V>, <S as ScopeParent>::Timestamp, Diff>>;
type ArrangementImport<S, V, T> =
    Arranged<S, TraceEnter<TraceValHandle<Vec<V>, Vec<V>, T, Diff>, <S as ScopeParent>::Timestamp>>;

/// Dataflow-local collections and arrangements.
pub struct Context<S: Scope, P: Eq + std::hash::Hash, V: Data, T>
where
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    /// Dataflow local collections.
    pub collections: HashMap<P, Collection<S, Vec<V>, Diff>>,
    /// Dataflow local arrangements.
    pub local: HashMap<P, HashMap<Vec<usize>, Arrangement<S, V>>>,
    /// Imported arrangements.
    pub trace: HashMap<P, HashMap<Vec<usize>, ArrangementImport<S, V, T>>>,
}

impl<S: Scope, P: Eq + std::hash::Hash, V: Data, T> Context<S, P, V, T>
where
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    pub fn collection(&self, plan: &P) -> Option<Collection<S, Vec<V>, Diff>> {
        if let Some(collection) = self.collections.get(plan) {
            Some(collection.clone())
        } else if let Some(local) = self.local.get(plan) {
            Some(
                local
                    .values()
                    .next()
                    .expect("Empty arrangement")
                    .as_collection(|_k, v| v.clone()),
            )
        } else if let Some(trace) = self.trace.get(plan) {
            Some(
                trace
                    .values()
                    .next()
                    .expect("Empty arrangement")
                    .as_collection(|_k, v| v.clone()),
            )
        } else {
            None
        }
    }

    /// Reports if we have an arrangement available.
    pub fn arrangement(&self, plan: &P, keys: &[usize]) -> Option<ArrangementFlavor<S, V, T>> {
        if let Some(local) = self.local.get(plan).and_then(|x| x.get(keys)) {
            Some(ArrangementFlavor::Local(local.clone()))
        } else if let Some(trace) = self.trace.get(plan).and_then(|x| x.get(keys)) {
            Some(ArrangementFlavor::Trace(trace.clone()))
        } else {
            None
        }
    }

    /// Retrieves an arrangement from a plan and keys.
    pub fn get_local(&self, plan: &P, keys: &[usize]) -> Option<&Arrangement<S, V>> {
        self.local.get(plan).and_then(|x| x.get(keys))
    }
    /// Binds a plan and keys to an arrangement.
    pub fn set_local(&mut self, plan: P, keys: &[usize], arranged: Arrangement<S, V>) {
        self.local
            .entry(plan)
            .or_insert_with(|| HashMap::new())
            .insert(keys.to_vec(), arranged);
    }
    /// Retrieves an arrangement from a plan and keys.
    pub fn get_trace(&self, plan: &P, keys: &[usize]) -> Option<&ArrangementImport<S, V, T>> {
        self.trace.get(plan).and_then(|x| x.get(keys))
    }
    /// Binds a plan and keys to an arrangement.
    pub fn set_trace(&mut self, plan: P, keys: &[usize], arranged: ArrangementImport<S, V, T>) {
        self.trace
            .entry(plan)
            .or_insert_with(|| HashMap::new())
            .insert(keys.to_vec(), arranged);
    }

    /// Creates a new empty Context.
    pub fn new() -> Self {
        Self {
            collections: HashMap::new(),
            local: HashMap::new(),
            trace: HashMap::new(),
        }
    }
}

/// Describes flavor of arrangement: local or imported trace.
pub enum ArrangementFlavor<S: Scope, V: Data, T: Lattice>
where
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    /// A dataflow-local arrangement.
    Local(Arrangement<S, V>),
    /// An imported trace from outside the dataflow.
    Trace(ArrangementImport<S, V, T>),
}
