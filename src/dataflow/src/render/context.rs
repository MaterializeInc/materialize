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
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::operators::arrange::Arranged;
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

use crate::arrangement::manager::{ErrSpine, RowSpine, TraceErrHandle, TraceRowHandle};
use crate::operator::CollectionExt;
use crate::render::datum_vec::{DatumVec, DatumVecBorrow};
use crate::render::Permutation;
use dataflow_types::{DataflowDescription, DataflowError};
use expr::{GlobalId, Id, MapFilterProject, MirScalarExpr};
use repr::{Diff, Row, RowArena};

// Local type definition to avoid the horror in signatures.
pub type Arrangement<S, V> = Arranged<S, TraceRowHandle<V, V, <S as ScopeParent>::Timestamp, Diff>>;
pub type ErrArrangement<S> =
    Arranged<S, TraceErrHandle<DataflowError, <S as ScopeParent>::Timestamp, Diff>>;
pub type ArrangementImport<S, V, T> = Arranged<
    S,
    TraceEnter<TraceFrontier<TraceRowHandle<V, V, T, Diff>>, <S as ScopeParent>::Timestamp>,
>;
pub type ErrArrangementImport<S, T> = Arranged<
    S,
    TraceEnter<
        TraceFrontier<TraceErrHandle<DataflowError, T, Diff>>,
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
    pub fn for_dataflow<Plan>(dataflow: &DataflowDescription<Plan>, dataflow_id: usize) -> Self {
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
    Local(Arrangement<S, V>, ErrArrangement<S>, Permutation),
    /// An imported trace from outside the dataflow.
    ///
    /// The `GlobalId` identifier exists so that exports of this same trace
    /// can refer back to and depend on the original instance.
    Trace(
        GlobalId,
        ArrangementImport<S, V, T>,
        ErrArrangementImport<S, T>,
        Permutation,
    ),
}

impl<S: Scope, T> ArrangementFlavor<S, Row, T>
where
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    /// Presents `self` as a stream of updates.
    ///
    /// This method presents the contents as they are, without further computation.
    /// If you have logic that could be applied to each record, consider using the
    /// `flat_map` methods which allows this and can reduce the work done.
    pub fn as_collection(&self) -> (Collection<S, Row, Diff>, Collection<S, DataflowError, Diff>) {
        let mut datum_vec = DatumVec::new();
        let mut row_builder = Row::default();
        match &self {
            ArrangementFlavor::Local(oks, errs, permutation) => {
                let permutation = permutation.clone();
                (
                    oks.as_collection(move |k, v| {
                        let mut borrow = datum_vec.borrow_with_many(&[k, v]);
                        permutation.permute_in_place(&mut borrow);
                        row_builder.extend(&*borrow);
                        row_builder.finish_and_reuse()
                    }),
                    errs.as_collection(|k, &()| k.clone()),
                )
            }
            ArrangementFlavor::Trace(_, oks, errs, permutation) => {
                let permutation = permutation.clone();
                (
                    oks.as_collection(move |k, v| {
                        let mut borrow = datum_vec.borrow_with_many(&[k, v]);
                        permutation.permute_in_place(&mut borrow);
                        row_builder.extend(&*borrow);
                        row_builder.finish_and_reuse()
                    }),
                    errs.as_collection(|k, &()| k.clone()),
                )
            }
        }
    }

    /// Applies logic to elements of `self` and returns the results.
    ///
    /// If `key` is set, this is a promise that `logic` will produce no results on
    /// records for which the key does not evaluate to the value. This is used to
    /// leap directly to exactly those records.
    pub fn flat_map<I, L>(
        &self,
        key: Option<Row>,
        mut logic: L,
    ) -> (
        timely::dataflow::Stream<S, I::Item>,
        Collection<S, DataflowError, Diff>,
    )
    where
        I: IntoIterator,
        I::Item: Data,
        L: for<'a, 'b> FnMut(DatumVecBorrow<'b>, &'a S::Timestamp, &'a Diff, &'b RowArena) -> I
            + 'static,
    {
        // Set a number of tuples after which the operator should yield.
        // This allows us to remain responsive even when enumerating a substantial
        // arrangement, as well as provides time to accumulate our produced output.
        let refuel = 1000000;

        let mut datum_vec = DatumVec::new();

        match &self {
            ArrangementFlavor::Local(oks, errs, permutation) => {
                let permutation = permutation.clone();
                let oks = CollectionBundle::<S, Row, T>::flat_map_core(
                    &oks,
                    key,
                    move |k, v, t, d| {
                        let mut borrow = datum_vec.borrow_with_many(&[&k, &v]);
                        permutation.permute_in_place(&mut borrow);
                        logic(borrow, t, d, &RowArena::default())
                    },
                    refuel,
                );
                let errs = errs.as_collection(|k, &()| k.clone());
                return (oks, errs);
            }
            ArrangementFlavor::Trace(_, oks, errs, permutation) => {
                let permutation = permutation.clone();
                let oks = CollectionBundle::<S, Row, T>::flat_map_core(
                    &oks,
                    key,
                    move |k, v, t, d| {
                        let mut borrow = datum_vec.borrow_with_many(&[&k, &v]);
                        permutation.permute_in_place(&mut borrow);
                        logic(borrow, t, d, &RowArena::default())
                    },
                    refuel,
                );
                let errs = errs.as_collection(|k, &()| k.clone());
                return (oks, errs);
            }
        }
    }
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
}

/// Parameter type to [CollectionBundle::ensure_arrangements], describing a key, a permutation of
/// data and a thinning expression.
pub type EnsureArrangement = (Vec<MirScalarExpr>, Permutation, Vec<usize>);

impl<S: Scope, T: Lattice> CollectionBundle<S, Row, T>
where
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    /// Presents `self` as a stream of updates.
    ///
    /// This method presents the contents as they are, without further computation.
    /// If you have logic that could be applied to each record, consider using the
    /// `as_collection_core` methods which allows this and can reduce the work done.
    pub fn as_collection(&self) -> (Collection<S, Row, Diff>, Collection<S, DataflowError, Diff>) {
        if let Some(collection) = &self.collection {
            collection.clone()
        } else {
            self.arranged
                .values()
                .next()
                .expect("Invariant violated: CollectionBundle contains no collection.")
                .as_collection()
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
        key_val: Option<(Vec<MirScalarExpr>, Row)>,
        mut logic: L,
    ) -> (
        timely::dataflow::Stream<S, I::Item>,
        Collection<S, DataflowError, Diff>,
    )
    where
        I: IntoIterator,
        I::Item: Data,
        L: for<'a, 'b> FnMut(DatumVecBorrow<'b>, &'a S::Timestamp, &'a Diff, &'b RowArena) -> I
            + 'static,
    {
        // If `key_val` is set, and we have the arrangement by that key, we should
        // use that arrangement.
        if let Some((key, val)) = key_val {
            if let Some(flavor) = self.arrangement(&key) {
                return flavor.flat_map(Some(val), logic);
            }
        }

        // No key, or a key but no matching arrangement (the latter would likely imply
        // an error in the contract between key-production and available arrangements).
        // We should now prefer to use an arrangement if available (avoids allocation),
        // and resort to using a collection if not available (copies all rows).
        if let Some(flavor) = self.arranged.values().next() {
            flavor.flat_map(None, logic)
        } else {
            use timely::dataflow::operators::Map;
            let (oks, errs) = self.as_collection();
            let mut datum_vec = DatumVec::new();
            (
                oks.inner.flat_map(move |(v, t, d)| {
                    let arena = RowArena::default();
                    let borrow = datum_vec.borrow_with(&v);
                    logic(borrow, &t, &d, &arena)
                }),
                errs,
            )
        }
    }

    /// Factored out common logic for using literal keys in general traces.
    ///
    /// This logic is sufficiently interesting that we want to write it only
    /// once, and thereby avoid any skew in the two uses of the logic.
    ///
    /// The function presents the contents of the trace as `(key, value, time, delta)` tuples,
    /// where key and value are rows. Often, a [Permutation] is approriate to present the row's
    /// columns in the expected order.
    fn flat_map_core<Tr, I, L>(
        trace: &Arranged<S, Tr>,
        key: Option<Row>,
        mut logic: L,
        refuel: usize,
    ) -> timely::dataflow::Stream<S, I::Item>
    where
        Tr: TraceReader<Key = Row, Val = Row, Time = S::Timestamp, R = repr::Diff>
            + Clone
            + 'static,
        Tr::Batch: BatchReader<Row, Tr::Val, S::Timestamp, repr::Diff> + 'static,
        Tr::Cursor: Cursor<Row, Tr::Val, S::Timestamp, repr::Diff> + 'static,
        I: IntoIterator,
        I::Item: Data,
        L: for<'a, 'b> FnMut(
                RefOrMut<'b, Row>,
                RefOrMut<'b, Row>,
                &'a S::Timestamp,
                &'a repr::Diff,
            ) -> I
            + 'static,
    {
        let mode = if key.is_some() { "index" } else { "scan" };
        let name = format!("ArrangementFlatMap({})", mode);
        use timely::dataflow::operators::Operator;
        trace.stream.unary(Pipeline, &name, move |_, info| {
            // Acquire an activator to reschedule the operator when it has unfinished work.
            use timely::scheduling::Activator;
            let activations = trace.stream.scope().activations();
            let activator = Activator::new(&info.address[..], activations);
            // Maintain a list of work to do, cursor to navigate and process.
            let mut todo = std::collections::VecDeque::new();
            move |input, output| {
                // First, dequeue all batches.
                input.for_each(|time, data| {
                    let capability = time.retain();
                    for batch in data.iter() {
                        // enqueue a capability, cursor, and batch.
                        todo.push_back(PendingWork::new(
                            capability.clone(),
                            batch.cursor(),
                            batch.clone(),
                        ));
                    }
                });

                // Second, make progress on `todo`.
                let mut fuel = refuel;
                while !todo.is_empty() && fuel > 0 {
                    todo.front_mut()
                        .unwrap()
                        .do_work(&key, &mut logic, &mut fuel, output);
                    if fuel > 0 {
                        todo.pop_front();
                    }
                }
                // If we have not finished all work, re-activate the operator.
                if !todo.is_empty() {
                    activator.activate();
                }
            }
        })
    }

    /// Look up an arrangement by the expressions that form the key.
    ///
    /// The result may be `None` if no such arrangement exists, or it may be one of many
    /// "arrangement flavors" that represent the types of arranged data we might have.
    pub fn arrangement(&self, key: &[MirScalarExpr]) -> Option<ArrangementFlavor<S, Row, T>> {
        self.arranged.get(key).map(|x| x.clone())
    }

    /// Ensures that arrangements in `keys` are present, creating them if they do not exist.
    pub fn ensure_arrangements<K: IntoIterator<Item = EnsureArrangement>>(
        mut self,
        keys: K,
    ) -> Self {
        for (key, permutation, thinning) in keys {
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
                let mut row_packer = Row::default();

                let (oks_keyed, errs_keyed) = oks.map_fallible("FormArrangementKey", move |row| {
                    let datums = row.unpack();
                    let temp_storage = RowArena::new();
                    row_packer.try_extend(key2.iter().map(|k| k.eval(&datums, &temp_storage)))?;
                    let key_row = row_packer.finish_and_reuse();
                    row_packer.extend(thinning.iter().map(|c| datums[*c]));
                    let val_row = row_packer.finish_and_reuse();
                    Ok::<(Row, Row), DataflowError>((key_row, val_row))
                });

                let oks = oks_keyed.arrange_named::<RowSpine<Row, Row, _, _>>(&name);
                let errs = errs
                    .concat(&errs_keyed)
                    .arrange_named::<ErrSpine<_, _, _>>(&format!("{}-errors", name));
                self.arranged
                    .insert(key, ArrangementFlavor::Local(oks, errs, permutation));
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
    ///
    /// The `key_val` argument, when present, indicates that a specific arrangement should
    /// be used, and that we can seek to the supplied row.
    pub fn as_collection_core(
        &self,
        mut mfp: MapFilterProject,
        key_val: Option<(Vec<MirScalarExpr>, Row)>,
    ) -> (
        Collection<S, repr::Row, Diff>,
        Collection<S, DataflowError, Diff>,
    ) {
        if mfp.is_identity() {
            self.as_collection()
        } else {
            mfp.optimize();
            let mfp_plan = mfp.into_plan().unwrap();
            let (stream, errors) = self.flat_map(key_val, {
                let mut row_builder = Row::default();
                move |mut datums_local, time, diff, temp_storage| {
                    mfp_plan.evaluate(
                        &mut datums_local,
                        &temp_storage,
                        time.clone(),
                        diff.clone(),
                        &mut row_builder,
                    )
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

use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::Capability;
struct PendingWork<K, V, T: Timestamp, R, C: Cursor<K, V, T, R>> {
    capability: Capability<T>,
    cursor: C,
    batch: C::Storage,
}

impl<K: PartialEq, V, T: Timestamp, R, C: Cursor<K, V, T, R>> PendingWork<K, V, T, R, C> {
    /// Create a new bundle of pending work, from the capability, cursor, and backing storage.
    fn new(capability: Capability<T>, cursor: C, batch: C::Storage) -> Self {
        Self {
            capability,
            cursor,
            batch,
        }
    }
    /// Perform roughly `fuel` work through the cursor, applying `logic` and sending results to `output`.
    fn do_work<I, L>(
        &mut self,
        key: &Option<K>,
        logic: &mut L,
        fuel: &mut usize,
        output: &mut OutputHandle<
            '_,
            T,
            I::Item,
            timely::dataflow::channels::pushers::Tee<T, I::Item>,
        >,
    ) where
        I: IntoIterator,
        I::Item: Data,
        L: for<'a, 'b> FnMut(RefOrMut<'b, K>, RefOrMut<'b, V>, &'a T, &'a R) -> I + 'static,
    {
        // Attempt to make progress on this batch.
        let mut work: usize = 0;
        let mut session = output.session(&self.capability);
        if let Some(key) = key {
            if self.cursor.get_key(&self.batch) != Some(key) {
                self.cursor.seek_key(&self.batch, key);
            }
            if self.cursor.get_key(&self.batch) == Some(key) {
                while let Some(val) = self.cursor.get_val(&self.batch) {
                    self.cursor.map_times(&self.batch, |time, diff| {
                        for datum in logic(RefOrMut::Ref(key), RefOrMut::Ref(val), time, diff) {
                            session.give(datum);
                            work += 1;
                        }
                    });
                    self.cursor.step_val(&self.batch);
                    if work >= *fuel {
                        *fuel = 0;
                        return;
                    }
                }
            }
        } else {
            while let Some(key) = self.cursor.get_key(&self.batch) {
                while let Some(val) = self.cursor.get_val(&self.batch) {
                    self.cursor.map_times(&self.batch, |time, diff| {
                        for datum in logic(RefOrMut::Ref(key), RefOrMut::Ref(val), time, diff) {
                            session.give(datum);
                            work += 1;
                        }
                    });
                    self.cursor.step_val(&self.batch);
                    if work >= *fuel {
                        *fuel = 0;
                        return;
                    }
                }
                self.cursor.step_key(&self.batch);
            }
        }
        *fuel -= work;
    }
}
