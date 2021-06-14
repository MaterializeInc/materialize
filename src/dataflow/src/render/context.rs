// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Management of dataflow-local state, like arrangements, while building a
//! dataflow.

use std::collections::BTreeMap;

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
use expr::{GlobalId, Id, MapFilterProject, MirScalarExpr};
use repr::{Row, RowArena};

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
pub struct Context<S: Scope, V: Data, T>
where
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
    /// Bindings of identifiers to collections.
    pub bindings: BTreeMap<Id, CollectionBundle<S, V, T>>,
}

impl<S: Scope, V: Data, T> Context<S, V, T>
where
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
            bindings: BTreeMap::new(),
        }
    }

    /// Insert a collection bundle by an identifier.
    ///
    /// This is expected to be used to install external collections (sources, indexes, other views),
    /// as well as for `Let` bindings of local collections.
    pub fn insert_id(
        &mut self,
        id: Id,
        collection: CollectionBundle<S, V, T>,
    ) -> Option<CollectionBundle<S, V, T>> {
        self.bindings.insert(id, collection)
    }
    /// Remove a collection bundle by an identifier.
    ///
    /// The primary use of this method is uninstalling `Let` bindings.
    pub fn remove_id(&mut self, id: Id) -> Option<CollectionBundle<S, V, T>> {
        self.bindings.remove(&id)
    }
    /// Melds a collection bundle to whatever exists.
    pub fn update_id(&mut self, id: Id, collection: CollectionBundle<S, V, T>) {
        if !self.bindings.contains_key(&id) {
            self.bindings.insert(id, collection);
        } else {
            let binding = self
                .bindings
                .get_mut(&id)
                .expect("Binding verified to exist");
            if collection.collection.is_some() {
                binding.collection = collection.collection;
            }
            for (key, flavor) in collection.arranged.into_iter() {
                binding.arranged.insert(key, flavor);
            }
        }
    }
    /// Look up a collection bundle by an identifier.
    pub fn lookup_id(&self, id: Id) -> Option<CollectionBundle<S, V, T>> {
        self.bindings.get(&id).cloned()
    }
}

/// Describes flavor of arrangement: local or imported trace.
#[derive(Clone)]
pub enum ArrangementFlavor<S: Scope, V: Data, T: Lattice>
where
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    /// A dataflow-local arrangement.
    Local(Arrangement<S, V>, ErrArrangement<S>),
    /// An imported trace from outside the dataflow.
    ///
    /// The `GlobalId` identifier exists so that exports of this same trace
    /// can refer back to and depend on the original instance.
    Trace(
        GlobalId,
        ArrangementImport<S, V, T>,
        ErrArrangementImport<S, T>,
    ),
}

/// A bundle of the various ways a collection can be represented.
///
/// This type maintains the invariant that it does contain at least one valid
/// source of data, either a collection or at least one arrangement.
#[derive(Clone)]
pub struct CollectionBundle<S: Scope, V: Data, T: Lattice>
where
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    pub collection: Option<(Collection<S, V, Diff>, Collection<S, DataflowError, Diff>)>,
    pub arranged: BTreeMap<Vec<MirScalarExpr>, ArrangementFlavor<S, V, T>>,
}

impl<S: Scope, V: Data, T: Lattice> CollectionBundle<S, V, T>
where
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    /// Construct a new collection bundle from update streams.
    pub fn from_collections(
        oks: Collection<S, V, Diff>,
        errs: Collection<S, DataflowError, Diff>,
    ) -> Self {
        Self {
            collection: Some((oks, errs)),
            arranged: BTreeMap::default(),
        }
    }

    /// Inserts arrangements by the expressions on which they are keyed.
    pub fn from_expressions(
        exprs: Vec<MirScalarExpr>,
        arrangements: ArrangementFlavor<S, V, T>,
    ) -> Self {
        let mut arranged = BTreeMap::new();
        arranged.insert(exprs, arrangements);
        Self {
            collection: None,
            arranged,
        }
    }

    /// Inserts arrangements by the columns on which they are keyed.
    pub fn from_columns<I: IntoIterator<Item = usize>>(
        columns: I,
        arrangements: ArrangementFlavor<S, V, T>,
    ) -> Self {
        let mut keys = Vec::new();
        for column in columns {
            keys.push(MirScalarExpr::Column(column));
        }
        Self::from_expressions(keys, arrangements)
    }

    /// Presents `self` as a stream of updates.
    ///
    /// This method presents the contents as they are, without further computation.
    /// If you have logic that could be applied to each record, consider using the
    /// `as_collection_core` methods which allows this and can reduce the work done.
    pub fn as_collection(&self) -> (Collection<S, V, Diff>, Collection<S, DataflowError, Diff>) {
        if let Some(collection) = &self.collection {
            collection.clone()
        } else {
            let arranged = self
                .arranged
                .values()
                .next()
                .expect("Invariant violated: CollectionBundle contains no collection.");
            match arranged {
                ArrangementFlavor::Local(oks, errs) => (
                    oks.as_collection(|_k, v| v.clone()),
                    errs.as_collection(|k, _v| k.clone()),
                ),
                ArrangementFlavor::Trace(_, oks, errs) => (
                    oks.as_collection(|_k, v| v.clone()),
                    errs.as_collection(|k, _v| k.clone()),
                ),
            }
        }
    }

    /// Applies logic to elements of a collection and returns the results.
    ///
    /// If `key_val` is set, this is a promise that `logic` will produce no results on
    /// records for which the key does not evaluate to the value. This is used when we
    /// have an arrangement by that key to leap directly to exactly those records.
    /// It is important that `logic` still guard against data that does not satisfy
    /// this constraint, as this method does not statically know that it will have
    /// that arrangement.
    pub fn flat_map<I, L>(
        &self,
        key_val: Option<(Vec<MirScalarExpr>, V)>,
        mut logic: L,
    ) -> (
        timely::dataflow::Stream<S, I::Item>,
        Collection<S, DataflowError, Diff>,
    )
    where
        I: IntoIterator,
        I::Item: Data,
        L: for<'a, 'b> FnMut(RefOrMut<'b, V>, &'a S::Timestamp, &'a Diff) -> I + 'static,
    {
        // If `key_val` is set, and we have the arrangement by that key, we should
        // use that arrangement.
        if let Some((key, val)) = key_val {
            if let Some(flavor) = self.arrangement(&key) {
                match flavor {
                    ArrangementFlavor::Local(oks, errs) => {
                        let oks = Self::flat_map_core(&oks, Some(val), logic);
                        let errs = errs.as_collection(|k, _v| k.clone());
                        return (oks, errs);
                    }
                    ArrangementFlavor::Trace(_, oks, errs) => {
                        let oks = Self::flat_map_core(&oks, Some(val), logic);
                        let errs = errs.as_collection(|k, _v| k.clone());
                        return (oks, errs);
                    }
                }
            }
        }

        // No key, or a key but no matching arrangement (the latter would likely imply
        // an error in the contract between key-production and available arrangements).
        // We should now prefer to use an arrangement if available (avoids allocation),
        // and resort to using a collection if not available (copies all rows).
        if let Some(flavor) = self.arranged.values().next() {
            match flavor {
                ArrangementFlavor::Local(oks, errs) => {
                    let oks = Self::flat_map_core(&oks, None, logic);
                    let errs = errs.as_collection(|k, _v| k.clone());
                    (oks, errs)
                }
                ArrangementFlavor::Trace(_, oks, errs) => {
                    let oks = Self::flat_map_core(&oks, None, logic);
                    let errs = errs.as_collection(|k, _v| k.clone());
                    (oks, errs)
                }
            }
        } else {
            use timely::dataflow::operators::Map;
            let (oks, errs) = self.as_collection();
            (
                oks.inner
                    .flat_map(move |(mut v, t, d)| logic(RefOrMut::Mut(&mut v), &t, &d)),
                errs,
            )
        }
    }

    /// Factored out common logic for using literal keys in general traces.
    ///
    /// This logic is sufficiently interesting that we want to write it only
    /// once, and thereby avoid any skew in the two uses of the logic.
    fn flat_map_core<Tr, I, L>(
        trace: &Arranged<S, Tr>,
        key: Option<V>,
        mut logic: L,
    ) -> timely::dataflow::Stream<S, I::Item>
    where
        Tr: TraceReader<Key = V, Val = V, Time = S::Timestamp, R = repr::Diff> + Clone + 'static,
        Tr::Batch: BatchReader<V, Tr::Val, S::Timestamp, repr::Diff> + 'static,
        Tr::Cursor: Cursor<V, Tr::Val, S::Timestamp, repr::Diff> + 'static,
        I: IntoIterator,
        I::Item: Data,
        L: for<'a, 'b> FnMut(RefOrMut<'b, V>, &'a S::Timestamp, &'a repr::Diff) -> I + 'static,
    {
        let mode = if key.is_some() { "index" } else { "scan" };
        let name = format!("ArrangementFlatMap({})", mode);
        use timely::dataflow::operators::Operator;
        trace.stream.unary(Pipeline, &name, move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    let mut session = output.session(&time);
                    for wrapper in data.iter() {
                        let batch = &wrapper;
                        let mut cursor = batch.cursor();
                        if let Some(key) = &key {
                            cursor.seek_key(batch, key);
                            if cursor.get_key(batch) == Some(key) {
                                while let Some(val) = cursor.get_val(batch) {
                                    cursor.map_times(batch, |time, diff| {
                                        for datum in logic(RefOrMut::Ref(val), time, diff) {
                                            session.give(datum);
                                        }
                                    });
                                    cursor.step_val(batch);
                                }
                            }
                        } else {
                            while let Some(_key) = cursor.get_key(batch) {
                                while let Some(val) = cursor.get_val(batch) {
                                    cursor.map_times(batch, |time, diff| {
                                        for datum in logic(RefOrMut::Ref(val), time, diff) {
                                            session.give(datum);
                                        }
                                    });
                                    cursor.step_val(batch);
                                }
                                cursor.step_key(batch);
                            }
                        }
                    }
                });
            }
        })
    }

    /// Look up an arrangemement by the expressions that form the key.
    ///
    /// The result may be `None` if no such arrangement exists, or it may be one of many
    /// "arrangement flavors" that represent the types of arranged data we might have.
    pub fn arrangement(&self, key: &[MirScalarExpr]) -> Option<ArrangementFlavor<S, V, T>> {
        self.arranged.get(key).map(|x| x.clone())
    }

    /// Reports the keys for any arrangement which evaluate to a literal under `key_selector`.
    pub fn constrained_keys<K>(&self, mut key_selector: K) -> Vec<(Vec<MirScalarExpr>, V)>
    where
        K: FnMut(&[MirScalarExpr]) -> Option<V>,
    {
        self.arranged
            .keys()
            .filter_map(|key| key_selector(key).map(|val| (key.clone(), val)))
            .collect::<Vec<_>>()
    }
}

impl<S: Scope, T: Lattice> CollectionBundle<S, repr::Row, T>
where
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    /// Ensures that arrangements in `keys` are present, creating them if they do not exist.
    pub fn ensure_arrangements<K: IntoIterator<Item = Vec<MirScalarExpr>>>(
        mut self,
        keys: K,
    ) -> Self {
        for key in keys {
            if !self.arranged.contains_key(&key) {
                // TODO: Consider allowing more expressive names.
                let name = format!("ArrangeBy[{:?}]", key);
                let key2 = key.clone();
                if self.collection.is_none() {
                    // Cache collection to avoid reforming it each time.
                    // TODO(mcsherry): In theory this could be faster run out of another arrangement,
                    // as the `map_fallible` that follows could be run against an arrangement itself.
                    self.collection = Some(self.as_collection());
                }
                let (oks, errs) = self.as_collection();
                use crate::operator::CollectionExt;
                let (oks_keyed, errs_keyed) = oks.map_fallible(move |row| {
                    let datums = row.unpack();
                    let temp_storage = RowArena::new();
                    let key_row =
                        Row::try_pack(key2.iter().map(|k| k.eval(&datums, &temp_storage)))?;
                    Ok::<(Row, Row), DataflowError>((key_row, row))
                });

                use differential_dataflow::operators::arrange::Arrange;
                let oks = oks_keyed.arrange_named::<OrdValSpine<Row, Row, _, _>>(&name);
                let errs = errs
                    .concat(&errs_keyed)
                    .arrange_named::<OrdKeySpine<_, _, _>>(&format!("{}-errors", name));
                self.arranged
                    .insert(key, ArrangementFlavor::Local(oks, errs));
            }
        }
        self
    }
}

impl<S> CollectionBundle<S, repr::Row, repr::Timestamp>
where
    S: Scope<Timestamp = repr::Timestamp>,
{
    /// Presents `self` as a stream of updates, having been subjected to `mfp`.
    ///
    /// This operator is able to apply the logic of `mfp` early, which can substantially
    /// reduce the amount of data produced when `mfp` is non-trivial.
    pub fn as_collection_core(
        &self,
        mut mfp: MapFilterProject,
    ) -> (
        Collection<S, repr::Row, Diff>,
        Collection<S, DataflowError, Diff>,
    ) {
        if mfp.is_identity() {
            self.as_collection()
        } else {
            mfp.optimize();
            let mfp2 = mfp.clone();
            let mfp_plan = mfp.into_plan().unwrap();
            // TODO: Improve key selection heuristic.
            let key_val = self
                .constrained_keys(move |exprs| mfp2.literal_constraints(exprs))
                .pop();
            let (stream, errors) = self.flat_map(key_val, {
                let mut datums = crate::render::datum_vec::DatumVec::new();
                move |data, time, diff| {
                    let temp_storage = repr::RowArena::new();
                    let mut datums_local = datums.borrow_with(&data);
                    mfp_plan
                        .evaluate(&mut datums_local, &temp_storage, time.clone(), diff.clone())
                        .map(|x| x.map_err(|(e, t, d)| (e.into(), t, d)))
                }
            });

            use timely::dataflow::operators::ok_err::OkErr;
            let (oks, errs) = stream.ok_err(|x| x);

            use differential_dataflow::AsCollection;
            let oks = oks.as_collection();
            let errs = errs.as_collection();
            (oks, errors.concat(&errs))
        }
    }
}
