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
use mz_compute_client::plan::AvailableCollections;
use timely::communication::message::RefOrMut;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Scope, ScopeParent};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use mz_compute_client::command::DataflowDescription;
use mz_expr::{Id, MapFilterProject, MirScalarExpr};
use mz_repr::{DatumVec, Diff, GlobalId, Row, RowArena};
use mz_storage::controller::CollectionMetadata;
use mz_storage::types::errors::DataflowError;
use mz_timely_util::operator::CollectionExt;

use crate::typedefs::{ErrSpine, RowSpine, TraceErrHandle, TraceRowHandle};

// Local type definition to avoid the horror in signatures.
pub(crate) type Arrangement<S, V> =
    Arranged<S, TraceRowHandle<V, V, <S as ScopeParent>::Timestamp, Diff>>;
pub(crate) type ErrArrangement<S> =
    Arranged<S, TraceErrHandle<DataflowError, <S as ScopeParent>::Timestamp, Diff>>;
pub(crate) type ArrangementImport<S, V, T> = Arranged<
    S,
    TraceEnter<TraceFrontier<TraceRowHandle<V, V, T, Diff>>, <S as ScopeParent>::Timestamp>,
>;
pub(crate) type ErrArrangementImport<S, T> = Arranged<
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
pub struct Context<S: Scope, V: Data, T = mz_repr::Timestamp>
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
    pub as_of_frontier: Antichain<T>,
    /// Bindings of identifiers to collections.
    pub bindings: BTreeMap<Id, CollectionBundle<S, V, T>>,
}

impl<S: Scope, V: Data> Context<S, V>
where
    S::Timestamp: Lattice + Refines<mz_repr::Timestamp>,
{
    /// Creates a new empty Context.
    pub fn for_dataflow<Plan>(
        dataflow: &DataflowDescription<Plan, CollectionMetadata>,
        dataflow_id: usize,
    ) -> Self {
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
}

impl<S: Scope, V: Data, T> Context<S, V, T>
where
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
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
pub enum ArrangementFlavor<S: Scope, V: Data, T = mz_repr::Timestamp>
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
        let mut row_buf = Row::default();
        match &self {
            ArrangementFlavor::Local(oks, errs) => (
                oks.as_collection(move |k, v| {
                    let borrow = datum_vec.borrow_with_many(&[k, v]);
                    row_buf.packer().extend(&*borrow);
                    row_buf.clone()
                }),
                errs.as_collection(|k, &()| k.clone()),
            ),
            ArrangementFlavor::Trace(_, oks, errs) => (
                oks.as_collection(move |k, v| {
                    let borrow = datum_vec.borrow_with_many(&[k, v]);
                    row_buf.packer().extend(&*borrow);
                    row_buf.clone()
                }),
                errs.as_collection(|k, &()| k.clone()),
            ),
        }
    }

    /// Constructs and applies logic to elements of `self` and returns the results.
    ///
    /// `constructor` takes a permutation and produces the logic to apply on elements. The logic
    /// conceptually receives `(&Row, &Row)` pairs in the form of a slice. Only after borrowing
    /// the elements and applying the permutation the datums will be in the expected order.
    ///
    /// If `key` is set, this is a promise that `logic` will produce no results on
    /// records for which the key does not evaluate to the value. This is used to
    /// leap directly to exactly those records.
    pub fn flat_map<I, C, L>(
        &self,
        key: Option<Row>,
        constructor: C,
    ) -> (
        timely::dataflow::Stream<S, I::Item>,
        Collection<S, DataflowError, Diff>,
    )
    where
        I: IntoIterator,
        I::Item: Data,
        C: FnOnce() -> L,
        L: for<'a, 'b> FnMut(&'a [&'b RefOrMut<'b, Row>], &'a S::Timestamp, &'a Diff) -> I
            + 'static,
    {
        // Set a number of tuples after which the operator should yield.
        // This allows us to remain responsive even when enumerating a substantial
        // arrangement, as well as provides time to accumulate our produced output.
        let refuel = 1000000;

        match &self {
            ArrangementFlavor::Local(oks, errs) => {
                let mut logic = constructor();
                let oks = CollectionBundle::<S, Row, T>::flat_map_core(
                    &oks,
                    key,
                    move |k, v, t, d| logic(&[&k, &v], t, d),
                    refuel,
                );
                let errs = errs.as_collection(|k, &()| k.clone());
                return (oks, errs);
            }
            ArrangementFlavor::Trace(_, oks, errs) => {
                let mut logic = constructor();
                let oks = CollectionBundle::<S, Row, T>::flat_map_core(
                    &oks,
                    key,
                    move |k, v, t, d| logic(&[&k, &v], t, d),
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
pub struct CollectionBundle<S: Scope, V: Data, T = mz_repr::Timestamp>
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

impl<S: Scope, T: Lattice> CollectionBundle<S, Row, T>
where
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    /// Asserts that the arrangement for a specific key
    /// (or the raw collection for no key) exists,
    /// and returns the corresponding collection.
    ///
    /// This returns the collection as-is, without
    /// doing any unthinning transformation.
    /// Therefore, it should be used when the appropriate transformation
    /// was planned as part of a following MFP.
    pub fn as_specific_collection(
        &self,
        key: Option<&[MirScalarExpr]>,
    ) -> (Collection<S, Row, Diff>, Collection<S, DataflowError, Diff>) {
        // Any operator that uses this method was told to use a particular
        // collection during LIR planning, where we should have made
        // sure that that collection exists.
        //
        // If it doesn't, we panic.
        match key {
            None => self
                .collection
                .clone()
                .expect("The unarranged collection doesn't exist."),
            Some(key) => self
                .arranged
                .get(key)
                .unwrap_or_else(|| panic!("The collection arranged by {:?} doesn't exist.", key))
                .as_collection(),
        }
    }

    /// Constructs and applies logic to elements of a collection and returns the results.
    ///
    /// `constructor` takes a permutation and produces the logic to apply on elements. The logic
    /// conceptually receives `(&Row, &Row)` pairs in the form of a slice. Only after borrowing
    /// the elements and applying the permutation the datums will be in the expected order.
    ///
    /// If `key_val` is set, this is a promise that `logic` will produce no results on
    /// records for which the key does not evaluate to the value. This is used when we
    /// have an arrangement by that key to leap directly to exactly those records.
    /// It is important that `logic` still guard against data that does not satisfy
    /// this constraint, as this method does not statically know that it will have
    /// that arrangement.
    pub fn flat_map<I, C, L>(
        &self,
        key_val: Option<(Vec<MirScalarExpr>, Option<Row>)>,
        constructor: C,
    ) -> (
        timely::dataflow::Stream<S, I::Item>,
        Collection<S, DataflowError, Diff>,
    )
    where
        I: IntoIterator,
        I::Item: Data,
        C: FnOnce() -> L,
        L: for<'a, 'b> FnMut(&'a [&'b RefOrMut<'b, Row>], &'a S::Timestamp, &'a Diff) -> I
            + 'static,
    {
        // If `key_val` is set, we should have use the corresponding arrangement.
        // If there isn't one, that implies an error in the contract between
        // key-production and available arrangements.
        if let Some((key, val)) = key_val {
            let flavor = self
                .arrangement(&key)
                .expect("Should have ensured during planning that this arrangement exists.");
            flavor.flat_map(val, constructor)
        } else {
            use timely::dataflow::operators::Map;
            let (oks, errs) = self
                .collection
                .clone()
                .expect("Invariant violated: CollectionBundle contains no collection.");
            let mut logic = constructor();
            (
                oks.inner
                    .flat_map(move |(mut v, t, d)| logic(&[&RefOrMut::Mut(&mut v)], &t, &d)),
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
    /// where key and value are rows.
    fn flat_map_core<Tr, I, L>(
        trace: &Arranged<S, Tr>,
        key: Option<Row>,
        mut logic: L,
        refuel: usize,
    ) -> timely::dataflow::Stream<S, I::Item>
    where
        Tr: TraceReader<Key = Row, Val = Row, Time = S::Timestamp, R = mz_repr::Diff>
            + Clone
            + 'static,
        I: IntoIterator,
        I::Item: Data,
        L: for<'a, 'b> FnMut(
                RefOrMut<'b, Row>,
                RefOrMut<'b, Row>,
                &'a S::Timestamp,
                &'a mz_repr::Diff,
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
}

impl<S, T> CollectionBundle<S, mz_repr::Row, T>
where
    T: timely::progress::Timestamp + Lattice,
    S: Scope,
    S::Timestamp:
        Refines<T> + Lattice + timely::progress::Timestamp + crate::render::RenderTimestamp,
{
    /// Presents `self` as a stream of updates, having been subjected to `mfp`.
    ///
    /// This operator is able to apply the logic of `mfp` early, which can substantially
    /// reduce the amount of data produced when `mfp` is non-trivial.
    ///
    /// The `key_val` argument, when present, indicates that a specific arrangement should
    /// be used, and if, in addition, the `val` component is present,
    /// that we can seek to the supplied row.
    pub fn as_collection_core(
        &self,
        mut mfp: MapFilterProject,
        key_val: Option<(Vec<MirScalarExpr>, Option<Row>)>,
    ) -> (
        Collection<S, mz_repr::Row, Diff>,
        Collection<S, DataflowError, Diff>,
    ) {
        mfp.optimize();
        let mfp_plan = mfp.into_plan().unwrap();

        // If the MFP is trivial, we can just call `as_collection`.
        // In the case that we weren't going to apply the `key_val` optimization,
        // this path results in a slightly smaller and faster
        // dataflow graph, and is intended to fix
        // https://github.com/MaterializeInc/materialize/issues/10507
        let has_key_val = if let Some((_key, Some(_val))) = &key_val {
            true
        } else {
            false
        };

        if mfp_plan.is_identity() && !has_key_val {
            let key = key_val.map(|(k, _v)| k);
            return self.as_specific_collection(key.as_deref());
        }
        let (stream, errors) = self.flat_map(key_val, || {
            let mut row_builder = Row::default();
            let mut datum_vec = DatumVec::new();

            move |row_parts, time, diff| {
                use crate::render::RenderTimestamp;

                let temp_storage = RowArena::new();
                let mut datums_local = datum_vec.borrow_with_many(row_parts);
                let time = time.clone();
                let event_time: mz_repr::Timestamp = *time.clone().event_time();
                mfp_plan
                    .evaluate(
                        &mut datums_local,
                        &temp_storage,
                        event_time,
                        diff.clone(),
                        &mut row_builder,
                    )
                    .map(move |x| match x {
                        Ok((row, event_time, diff)) => {
                            // Copy the whole time, and re-populate event time.
                            let mut time: S::Timestamp = time.clone();
                            *time.event_time() = event_time;
                            Ok((row, time, diff))
                        }
                        Err((e, event_time, diff)) => {
                            // Copy the whole time, and re-populate event time.
                            let mut time: S::Timestamp = time.clone();
                            *time.event_time() = event_time;
                            Err((e, time, diff))
                        }
                    })
            }
        });

        use timely::dataflow::operators::ok_err::OkErr;
        let (oks, errs) = stream.ok_err(|x| x);

        use differential_dataflow::AsCollection;
        let oks = oks.as_collection();
        let errs = errs.as_collection();
        (oks, errors.concat(&errs))
    }
    pub fn ensure_collections(
        mut self,
        collections: AvailableCollections,
        input_key: Option<Vec<MirScalarExpr>>,
        input_mfp: MapFilterProject,
    ) -> Self {
        if collections == Default::default() {
            return self;
        }
        // Cache collection to avoid reforming it each time.
        //
        // TODO(mcsherry): In theory this could be faster run out of another arrangement,
        // as the `map_fallible` that follows could be run against an arrangement itself.
        //
        // Note(btv): If we ever do that, we would then only need to make the raw collection here
        // if `collections.raw` is true.

        // We need the collection if either (1) it is explicitly demanded, or (2) we are going to render any arrangement
        let form_raw_collection = collections.raw
            || collections
                .arranged
                .iter()
                .any(|(key, _, _)| !self.arranged.contains_key(key));
        if form_raw_collection && self.collection.is_none() {
            self.collection =
                Some(self.as_collection_core(input_mfp, input_key.map(|k| (k, None))));
        }
        for (key, _, thinning) in collections.arranged {
            if !self.arranged.contains_key(&key) {
                // TODO: Consider allowing more expressive names.
                let name = format!("ArrangeBy[{:?}]", key);
                let key2 = key.clone();
                let (oks, errs) = self
                    .collection
                    .clone()
                    .expect("Collection constructed above");

                let mut row_buf = Row::default();

                let mut datums = DatumVec::new();
                let (oks_keyed, errs_keyed) = oks.map_fallible("FormArrangementKey", move |row| {
                    // TODO: Consider reusing the `row` allocation; probably in *next* invocation.
                    let datums = datums.borrow_with(&row);
                    let temp_storage = RowArena::new();
                    row_buf
                        .packer()
                        .try_extend(key2.iter().map(|k| k.eval(&datums, &temp_storage)))?;
                    let key_row = row_buf.clone();
                    row_buf.packer().extend(thinning.iter().map(|c| datums[*c]));
                    let val_row = row_buf.clone();
                    Ok::<(Row, Row), DataflowError>((key_row, val_row))
                });

                let oks = oks_keyed.arrange_named::<RowSpine<Row, Row, _, _>>(&name);
                let errs = errs
                    .concat(&errs_keyed)
                    .arrange_named::<ErrSpine<_, _, _>>(&format!("{}-errors", name));
                self.arranged
                    .insert(key, ArrangementFlavor::Local(oks, errs));
            }
        }
        self
    }
}

use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::Capability;
struct PendingWork<C>
where
    C: Cursor,
    C::Time: Timestamp,
{
    capability: Capability<C::Time>,
    cursor: C,
    batch: C::Storage,
}

impl<C: Cursor> PendingWork<C>
where
    C::Key: PartialEq,
    C::Time: Timestamp,
{
    /// Create a new bundle of pending work, from the capability, cursor, and backing storage.
    fn new(capability: Capability<C::Time>, cursor: C, batch: C::Storage) -> Self {
        Self {
            capability,
            cursor,
            batch,
        }
    }
    /// Perform roughly `fuel` work through the cursor, applying `logic` and sending results to `output`.
    fn do_work<I, L>(
        &mut self,
        key: &Option<C::Key>,
        logic: &mut L,
        fuel: &mut usize,
        output: &mut OutputHandle<
            '_,
            C::Time,
            I::Item,
            timely::dataflow::channels::pushers::Tee<C::Time, I::Item>,
        >,
    ) where
        I: IntoIterator,
        I::Item: Data,
        L: for<'a, 'b> FnMut(
                RefOrMut<'b, C::Key>,
                RefOrMut<'b, C::Val>,
                &'a C::Time,
                &'a C::R,
            ) -> I
            + 'static,
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
