// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Management of dataflow-local state, like arrangements, while building a
//! dataflow.

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use differential_dataflow::trace::BatchReader;
use differential_dataflow::trace::{Cursor, TraceReader};
use differential_dataflow::Collection;
use differential_dataflow::Data;
use timely::communication::message::RefOrMut;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Scope, ScopeParent};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use dataflow_types::{DataflowDesc, DataflowError};
use expr::{GlobalId, MirScalarExpr};

use crate::source::SourceToken;

/// A trace handle for key-only data.
pub type TraceKeyHandle<K, T, R> = TraceAgent<OrdKeySpine<K, T, R>>;

/// A trace handle for key-value data.
pub type TraceValHandle<K, V, T, R> = TraceAgent<OrdValSpine<K, V, T, R>>;

pub type Diff = isize;

// Local type definition to avoid the horror in signatures.
pub type Arrangement<S, V> = Arranged<S, TraceValHandle<V, V, <S as ScopeParent>::Timestamp, Diff>>;
pub type ErrArrangement<S> =
    Arranged<S, TraceKeyHandle<DataflowError, <S as ScopeParent>::Timestamp, Diff>>;
pub type ArrangementImport<S, V, T> = Arranged<
    S,
    TraceEnter<TraceFrontier<TraceValHandle<V, V, T, Diff>>, <S as ScopeParent>::Timestamp>,
>;
pub type ErrArrangementImport<S, T> = Arranged<
    S,
    TraceEnter<
        TraceFrontier<TraceKeyHandle<DataflowError, T, Diff>>,
        <S as ScopeParent>::Timestamp,
    >,
>;

/// Dataflow-local collections and arrangements.
///
/// A context means to wrap available data assets and present them in an easy-to-use manner.
/// These assets include dataflow-local collections and arrangements, as well as imported
/// arrangements from outside the dataflow.
///
/// Context has two timestamp types, one from `S::Timestamp` and one from `T`, where the
/// former must refine the latter. The former is the timestamp used by the scope in question,
/// and the latter is the timestamp of imported traces. The two may be different in the case
/// of regions or iteration.
pub struct Context<S: Scope, P, V: Data, T>
where
    P: Eq + std::hash::Hash + Clone,
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    /// The debug name of the dataflow associated with this context.
    pub debug_name: String,
    /// The Timely ID of the dataflow associated with this context.
    pub dataflow_id: usize,
    /// Indicates a frontier that can be used to compact input timestamps
    /// without affecting the results. We *should* apply it, to sources and
    /// imported traces, both because it improves performance, and because
    /// potentially incorrect results are visible in sinks.
    pub as_of_frontier: Antichain<repr::Timestamp>,
    /// The source tokens for all sources that have been built in this context.
    pub source_tokens: HashMap<GlobalId, Rc<Option<SourceToken>>>,
    /// Any other tokens that need to be dropped when an object is dropped.
    pub additional_tokens: HashMap<GlobalId, Vec<Rc<dyn Any>>>,
    /// Tokens for CDCv2 capture sources that have been built in this context.
    pub cdc_tokens: HashMap<GlobalId, Rc<dyn Any>>,
    /// Dataflow local collections.
    pub collections: HashMap<P, (Collection<S, V, Diff>, Collection<S, DataflowError, Diff>)>,
    /// Dataflow local arrangements.
    pub local: HashMap<P, BTreeMap<Vec<MirScalarExpr>, (Arrangement<S, V>, ErrArrangement<S>)>>,
    /// Imported arrangements.
    #[allow(clippy::type_complexity)] // TODO(fms): fix or ignore lint globally.
    pub trace: HashMap<
        P,
        BTreeMap<
            Vec<MirScalarExpr>,
            (
                GlobalId,
                ArrangementImport<S, V, T>,
                ErrArrangementImport<S, T>,
            ),
        >,
    >,
}

impl<S: Scope, P, V: Data, T> Context<S, P, V, T>
where
    P: Eq + std::hash::Hash + Clone,
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    /// Creates a new empty Context.
    pub fn for_dataflow(dataflow: &DataflowDesc, dataflow_id: usize) -> Self {
        let as_of_frontier = dataflow
            .as_of
            .clone()
            .unwrap_or_else(|| Antichain::from_elem(0));

        Self {
            debug_name: dataflow.debug_name.clone(),
            dataflow_id,
            as_of_frontier,
            source_tokens: HashMap::new(),
            additional_tokens: HashMap::new(),
            collections: HashMap::new(),
            local: HashMap::new(),
            trace: HashMap::new(),
            cdc_tokens: HashMap::new(),
        }
    }

    /// Indicates if a collection is available.
    pub fn has_collection(&self, relation_expr: &P) -> bool {
        self.collections.get(relation_expr).is_some()
            || self.local.get(relation_expr).is_some()
            || self.trace.get(relation_expr).is_some()
    }

    /// Assembles a collection if available.
    ///
    /// This method consults all available data assets to create the appropriate
    /// collection. This can be either a collection itself, or if absent we may
    /// also be able to find a stashed arrangement for the same relation_expr, which we
    /// flatten down to a collection.
    ///
    /// If insufficient data assets exist to create the collection the method
    /// will return `None`.
    pub fn collection(
        &self,
        relation_expr: &P,
    ) -> Option<(Collection<S, V, Diff>, Collection<S, DataflowError, Diff>)> {
        if let Some(collection) = self.collections.get(relation_expr) {
            Some(collection.clone())
        } else if let Some(local) = self.local.get(relation_expr) {
            let (oks, errs) = local.values().next().expect("Empty arrangement");
            Some((
                oks.as_collection(|_k, v| v.clone()),
                errs.as_collection(|k, _v| k.clone()),
            ))
        } else if let Some(trace) = self.trace.get(relation_expr) {
            let (_id, oks, errs) = trace.values().next().expect("Empty arrangement");
            Some((
                oks.as_collection(|_k, v| v.clone()),
                errs.as_collection(|k, _v| k.clone()),
            ))
        } else {
            None
        }
    }

    /// Applies logic to elements of a collection and returns the results.
    ///
    /// This method allows savvy users to avoid the potential allocation
    /// required by `self.collection(expr)` when converting data from an
    /// arrangement; if less data are required, the `logic` argument is able
    /// to use a reference and produce the minimal amount of data instead.
    ///
    /// The method also takes a `key_selector` argument which allows the
    /// caller to indicate that certain scalar expressions may have literal
    /// values (at least, for all yielded results). This can enable the
    /// use of keys in arrangements, and in particular drive the choice of
    /// which arrangement to use toward one whose keys must be literals.
    pub fn flat_map_ref<I, L, K>(
        &self,
        relation_expr: &P,
        mut key_selector: K,
        mut logic: L,
    ) -> Option<(
        Collection<S, I::Item, Diff>,
        Collection<S, DataflowError, Diff>,
    )>
    where
        I: IntoIterator,
        I::Item: Data,
        L: FnMut(RefOrMut<V>) -> I + 'static,
        K: FnMut(&[MirScalarExpr]) -> Option<V>,
    {
        if let Some((oks, err)) = self.collections.get(relation_expr) {
            Some((
                oks.flat_map(move |mut v| logic(RefOrMut::Mut(&mut v))),
                err.clone(),
            ))
        } else if let Some(local) = self.local.get(relation_expr) {
            // Determine a key that is constrained to a literal value.
            let mut constrained_keys = local
                .keys()
                .filter_map(|key| key_selector(key).map(|row| (key.clone(), row)))
                .collect::<Vec<_>>();
            // Sort keys by length as a proxy for selectivity.
            // TODO(#4580): improve this heuristic when possible.
            constrained_keys.sort_by_key(|(key, _row)| key.len());
            // If we found such a key, use a more advanced implementation.
            if let Some((key, row)) = constrained_keys.pop() {
                let (oks, errs) = &local[&key];
                let result = Self::flat_map_ref_inner(oks, row, logic);
                Some((result, errs.as_collection(|k, _v| k.clone())))
            } else {
                let (oks, errs) = local.values().next().expect("Empty arrangement");
                Some((
                    oks.flat_map_ref(move |_k, v| logic(RefOrMut::Ref(&v))),
                    errs.as_collection(|k, _v| k.clone()),
                ))
            }
        } else if let Some(trace) = self.trace.get(relation_expr) {
            // Determine a key that is constrained to a literal value.
            let mut constrained_keys = trace
                .keys()
                .filter_map(|key| key_selector(key).map(|row| (key.clone(), row)))
                .collect::<Vec<_>>();
            // Sort keys by length as a proxy for selectivity.
            // TODO(#4580): improve this heuristic when possible.
            constrained_keys.sort_by_key(|(key, _row)| key.len());
            // If we found such a key, use a more advanced implementation.
            if let Some((key, row)) = constrained_keys.pop() {
                let (_id, oks, errs) = &trace[&key];
                let result = Self::flat_map_ref_inner(oks, row, logic);
                Some((result, errs.as_collection(|k, _v| k.clone())))
            } else {
                let (_id, oks, errs) = trace.values().next().expect("Empty arrangement");
                Some((
                    oks.flat_map_ref(move |_k, v| logic(RefOrMut::Ref(&v))),
                    errs.as_collection(|k, _v| k.clone()),
                ))
            }
        } else {
            None
        }
    }

    /// Factored out common logic for using literal keys in general traces.
    ///
    /// This logic is sufficiently interesting that we want to write it only
    /// once, and thereby avoid any skew in the two uses of the logic.
    fn flat_map_ref_inner<Tr, I, L>(
        trace: &Arranged<S, Tr>,
        row: V,
        mut logic: L,
    ) -> Collection<S, I::Item, Diff>
    where
        Tr: TraceReader<Key = V, Val = V, Time = S::Timestamp, R = repr::Diff> + Clone + 'static,
        Tr::Batch: BatchReader<V, Tr::Val, S::Timestamp, repr::Diff> + 'static,
        Tr::Cursor: Cursor<V, Tr::Val, S::Timestamp, repr::Diff> + 'static,
        I: IntoIterator,
        I::Item: Data,
        L: FnMut(RefOrMut<V>) -> I + 'static,
    {
        use differential_dataflow::AsCollection;
        use timely::dataflow::operators::Operator;
        trace
            .stream
            .unary(Pipeline, "AsCollection", move |_, _| {
                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session(&time);
                        for wrapper in data.iter() {
                            let batch = &wrapper;
                            let mut cursor = batch.cursor();
                            cursor.seek_key(batch, &row);
                            if cursor.get_key(batch) == Some(&row) {
                                while let Some(val) = cursor.get_val(batch) {
                                    for datum in logic(RefOrMut::Ref(val)) {
                                        cursor.map_times(batch, |time, diff| {
                                            session.give((
                                                datum.clone(),
                                                time.clone(),
                                                diff.clone(),
                                            ));
                                        });
                                    }
                                    cursor.step_val(batch);
                                }
                            }
                        }
                    });
                }
            })
            .as_collection()
    }

    /// Convenience method for accessing `arrangement` when all keys are plain columns
    pub fn arrangement_columns(
        &self,
        relation_expr: &P,
        columns: &[usize],
    ) -> Option<ArrangementFlavor<S, V, T>> {
        let mut keys = Vec::new();
        for column in columns {
            keys.push(MirScalarExpr::Column(*column));
        }
        self.arrangement(relation_expr, &keys)
    }

    /// Produces an arrangement if available.
    ///
    /// A context store multiple types of arrangements, and prioritizes
    /// dataflow-local arrangements in its return values.
    pub fn arrangement(
        &self,
        relation_expr: &P,
        keys: &[MirScalarExpr],
    ) -> Option<ArrangementFlavor<S, V, T>> {
        if let Some(local) = self.get_local(relation_expr, keys) {
            let (oks, errs) = local.clone();
            Some(ArrangementFlavor::Local(oks, errs))
        } else if let Some((gid, oks, errs)) = self.get_trace(relation_expr, keys) {
            Some(ArrangementFlavor::Trace(*gid, oks.clone(), errs.clone()))
        } else {
            None
        }
    }

    /// Retrieves a local arrangement from a relation_expr and keys.
    pub fn get_local(
        &self,
        relation_expr: &P,
        keys: &[MirScalarExpr],
    ) -> Option<&(Arrangement<S, V>, ErrArrangement<S>)> {
        self.local.get(relation_expr).and_then(|x| x.get(keys))
    }

    /// Convenience method for `set_local` when all keys are plain columns
    pub fn set_local_columns(
        &mut self,
        relation_expr: &P,
        columns: &[usize],
        arranged: (Arrangement<S, V>, ErrArrangement<S>),
    ) {
        let mut keys = Vec::new();
        for column in columns {
            keys.push(MirScalarExpr::Column(*column));
        }
        self.set_local(relation_expr, &keys, arranged);
    }

    /// Binds a relation_expr and keys to a local arrangement.
    pub fn set_local(
        &mut self,
        relation_expr: &P,
        keys: &[MirScalarExpr],
        arranged: (Arrangement<S, V>, ErrArrangement<S>),
    ) {
        self.local
            .entry(relation_expr.clone())
            .or_insert_with(BTreeMap::new)
            .insert(keys.to_vec(), arranged);
    }

    /// Retrieves an imported arrangement from a relation_expr and keys.
    pub fn get_trace(
        &self,
        relation_expr: &P,
        keys: &[MirScalarExpr],
    ) -> Option<&(
        GlobalId,
        ArrangementImport<S, V, T>,
        ErrArrangementImport<S, T>,
    )> {
        self.trace.get(relation_expr).and_then(|x| x.get(keys))
    }

    /// Binds a relation_expr and keys to an imported arrangement.
    pub fn set_trace(
        &mut self,
        gid: GlobalId,
        relation_expr: &P,
        keys: &[MirScalarExpr],
        arranged: (ArrangementImport<S, V, T>, ErrArrangementImport<S, T>),
    ) {
        self.trace
            .entry(relation_expr.clone())
            .or_insert_with(BTreeMap::new)
            .insert(keys.to_vec(), (gid, arranged.0, arranged.1));
    }

    /// Clones from one key to another, as needed in let binding.
    pub fn clone_from_to(&mut self, key1: &P, key2: &P) {
        if let Some(collection) = self.collections.get(key1).cloned() {
            self.collections.insert(key2.clone(), collection);
        }
        if let Some(handles) = self.local.get(key1).cloned() {
            self.local.insert(key2.clone(), handles);
        }
        if let Some(handles) = self.trace.get(key1).cloned() {
            self.trace.insert(key2.clone(), handles);
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
    Local(Arrangement<S, V>, ErrArrangement<S>),
    /// An imported trace from outside the dataflow.
    Trace(
        GlobalId,
        ArrangementImport<S, V, T>,
        ErrArrangementImport<S, T>,
    ),
}
