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
use std::rc::Rc;

use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use differential_dataflow::{AsCollection, Data, VecCollection};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::dyncfgs::{
    ENABLE_COMPUTE_RENDER_FUELED_AS_SPECIFIC_COLLECTION, ENABLE_COMPUTE_TEMPORAL_BUCKETING,
    TEMPORAL_BUCKETING_SUMMARY,
};
use mz_compute_types::plan::{ArrangementStrategy, AvailableCollections};
use mz_dyncfg::ConfigSet;
use mz_expr::{Eval, Id, MapFilterProject, MirScalarExpr};
use mz_ore::soft_assert_or_log;
use mz_repr::fixed_length::ToDatumIter;
use mz_repr::{DatumVec, DatumVecBorrow, Diff, GlobalId, Row, RowArena, SharedRow};
use mz_storage_types::controller::CollectionMetadata;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::{Col2ValBatcher, columnar_exchange};
use timely::ContainerBuilder;
use timely::container::{CapacityContainerBuilder, PushInto};
use timely::dataflow::channels::pact::{ExchangeCore, Pipeline};
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::{OutputBuilder, OutputBuilderSession};
use timely::dataflow::{Scope, Stream};
use timely::progress::operate::FrontierInterest;
use timely::progress::{Antichain, Timestamp};

use crate::compute_state::ComputeState;
use crate::extensions::arrange::{KeyCollection, MzArrange, MzArrangeCore};
use crate::render::errors::{DataflowErrorSer, ErrorLogger};
use crate::render::{LinearJoinSpec, MaybeBucketByTime, RenderTimestamp};
use crate::typedefs::{
    ErrAgent, ErrBatcher, ErrBuilder, ErrEnter, ErrSpine, RowRowAgent, RowRowEnter, RowRowSpine,
};
use mz_row_spine::{DatumSeq, RowRowBuilder};

/// Dataflow-local collections and arrangements.
///
/// A context means to wrap available data assets and present them in an easy-to-use manner.
/// These assets include dataflow-local collections and arrangements, as well as imported
/// arrangements from outside the dataflow.
///
/// Context has a timestamp type `T`, which is the timestamp used by the scope in question.
pub struct Context<'scope, T: RenderTimestamp> {
    /// The scope within which all managed collections exist.
    ///
    /// It is an error to add any collections not contained in this scope.
    pub(crate) scope: Scope<'scope, T>,
    /// The debug name of the dataflow associated with this context.
    pub debug_name: String,
    /// The Timely ID of the dataflow associated with this context.
    pub dataflow_id: usize,
    /// The collection IDs of exports of the dataflow associated with this context.
    pub export_ids: Vec<GlobalId>,
    /// Frontier before which updates should not be emitted.
    ///
    /// We *must* apply it to sinks, to ensure correct outputs.
    /// We *should* apply it to sources and imported traces, because it improves performance.
    pub as_of_frontier: Antichain<mz_repr::Timestamp>,
    /// Frontier after which updates should not be emitted.
    /// Used to limit the amount of work done when appropriate.
    pub until: Antichain<mz_repr::Timestamp>,
    /// Bindings of identifiers to collections.
    pub bindings: BTreeMap<Id, CollectionBundle<'scope, T>>,
    /// The logger, from Timely's logging framework, if logs are enabled.
    pub(super) compute_logger: Option<crate::logging::compute::Logger>,
    /// Specification for rendering linear joins.
    pub(super) linear_join_spec: LinearJoinSpec,
    /// The expiration time for dataflows in this context. The output's frontier should never advance
    /// past this frontier, except the empty frontier.
    pub dataflow_expiration: Antichain<mz_repr::Timestamp>,
    /// The config set for this context.
    pub config_set: Rc<ConfigSet>,
}

impl<'scope, T: RenderTimestamp> Context<'scope, T> {
    /// Creates a new empty Context.
    pub fn for_dataflow_in<Plan>(
        dataflow: &DataflowDescription<Plan, CollectionMetadata>,
        scope: Scope<'scope, T>,
        compute_state: &ComputeState,
        until: Antichain<mz_repr::Timestamp>,
        dataflow_expiration: Antichain<mz_repr::Timestamp>,
    ) -> Self {
        use mz_ore::collections::CollectionExt as IteratorExt;
        let dataflow_id = *scope.addr().into_first();
        let as_of_frontier = dataflow
            .as_of
            .clone()
            .unwrap_or_else(|| Antichain::from_elem(Timestamp::minimum()));

        let export_ids = dataflow.export_ids().collect();

        // Skip compute event logging for transient dataflows. We do this to avoid overhead for
        // slow-path peeks, but it also affects subscribes. For now that seems fine, but we may
        // want to reconsider in the future.
        let compute_logger = if dataflow.is_transient() {
            None
        } else {
            compute_state.compute_logger.clone()
        };

        Self {
            scope,
            debug_name: dataflow.debug_name.clone(),
            dataflow_id,
            export_ids,
            as_of_frontier,
            until,
            bindings: BTreeMap::new(),
            compute_logger,
            linear_join_spec: compute_state.linear_join_spec,
            dataflow_expiration,
            config_set: Rc::clone(&compute_state.worker_config),
        }
    }
}

impl<'scope, T: RenderTimestamp> Context<'scope, T> {
    /// Insert a collection bundle by an identifier.
    ///
    /// This is expected to be used to install external collections (sources, indexes, other views),
    /// as well as for `Let` bindings of local collections.
    pub fn insert_id(
        &mut self,
        id: Id,
        collection: CollectionBundle<'scope, T>,
    ) -> Option<CollectionBundle<'scope, T>> {
        self.bindings.insert(id, collection)
    }
    /// Remove a collection bundle by an identifier.
    ///
    /// The primary use of this method is uninstalling `Let` bindings.
    pub fn remove_id(&mut self, id: Id) -> Option<CollectionBundle<'scope, T>> {
        self.bindings.remove(&id)
    }
    /// Melds a collection bundle to whatever exists.
    pub fn update_id(&mut self, id: Id, collection: CollectionBundle<'scope, T>) {
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
    pub fn lookup_id(&self, id: Id) -> Option<CollectionBundle<'scope, T>> {
        self.bindings.get(&id).cloned()
    }

    pub(super) fn error_logger(&self) -> ErrorLogger {
        ErrorLogger::new(self.debug_name.clone())
    }
}

impl<'scope, T: RenderTimestamp> Context<'scope, T> {
    /// Brings the underlying arrangements and collections into a region.
    pub fn enter_region<'a>(
        &self,
        region: Scope<'a, T>,
        bindings: Option<&std::collections::BTreeSet<Id>>,
    ) -> Context<'a, T> {
        let bindings = self
            .bindings
            .iter()
            .filter(|(key, _)| bindings.as_ref().map(|b| b.contains(key)).unwrap_or(true))
            .map(|(key, bundle)| (*key, bundle.enter_region(region)))
            .collect();

        Context {
            scope: region,
            debug_name: self.debug_name.clone(),
            dataflow_id: self.dataflow_id.clone(),
            export_ids: self.export_ids.clone(),
            as_of_frontier: self.as_of_frontier.clone(),
            until: self.until.clone(),
            compute_logger: self.compute_logger.clone(),
            linear_join_spec: self.linear_join_spec.clone(),
            bindings,
            dataflow_expiration: self.dataflow_expiration.clone(),
            config_set: Rc::clone(&self.config_set),
        }
    }
}

/// Describes flavor of arrangement: local or imported trace.
#[derive(Clone)]
pub enum ArrangementFlavor<'scope, T: RenderTimestamp> {
    /// A dataflow-local arrangement.
    Local(
        Arranged<'scope, RowRowAgent<T, Diff>>,
        Arranged<'scope, ErrAgent<T, Diff>>,
    ),
    /// An imported trace from outside the dataflow.
    ///
    /// The `GlobalId` identifier exists so that exports of this same trace
    /// can refer back to and depend on the original instance.
    Trace(
        GlobalId,
        Arranged<'scope, RowRowEnter<mz_repr::Timestamp, Diff, T>>,
        Arranged<'scope, ErrEnter<mz_repr::Timestamp, T>>,
    ),
}

impl<'scope, T: RenderTimestamp> ArrangementFlavor<'scope, T> {
    /// Presents `self` as a stream of updates.
    ///
    /// Deprecated: This function is not fueled and hence risks flattening the whole arrangement.
    ///
    /// This method presents the contents as they are, without further computation.
    /// If you have logic that could be applied to each record, consider using the
    /// `flat_map` methods which allows this and can reduce the work done.
    #[deprecated(note = "Use `flat_map` instead.")]
    pub fn as_collection(
        &self,
    ) -> (
        VecCollection<'scope, T, Row, Diff>,
        VecCollection<'scope, T, DataflowErrorSer, Diff>,
    ) {
        let mut datums = DatumVec::new();
        let logic = move |k: DatumSeq, v: DatumSeq| {
            let mut datums_borrow = datums.borrow();
            datums_borrow.extend(k);
            datums_borrow.extend(v);
            SharedRow::pack(&**datums_borrow)
        };
        match &self {
            ArrangementFlavor::Local(oks, errs) => (
                oks.clone().as_collection(logic),
                errs.clone().as_collection(|k, &()| k.clone()),
            ),
            ArrangementFlavor::Trace(_, oks, errs) => (
                oks.clone().as_collection(logic),
                errs.clone().as_collection(|k, &()| k.clone()),
            ),
        }
    }

    /// Constructs and applies logic to elements of `self` and returns the results.
    ///
    /// The `logic` callback receives a borrow of the decoded datum vector, a timestamp, a
    /// diff, and two output sessions: one for `ok` updates of type `(D, T, Diff)` and one for
    /// MFP-style `DataflowErrorSer` updates. It must return the number of records *produced*
    /// (written to either session), not the number of input tuples consumed.
    ///
    /// # Fuel
    ///
    /// The operator accumulates the returned counts as fuel and yields when the total reaches
    /// an internal refuel threshold. The metric is output-produced (not input-consumed) on
    /// purpose: it regulates two asymmetric pressures.
    ///
    /// * **Drain inputs.** The operator holds a clone of each pending `Batch` until its work
    ///   item pops; we want to release that memory back to the upstream arrangement as soon
    ///   as possible. A `filter(false)` MFP returns 0 for every tuple, so fuel never trips
    ///   and the cursor runs to end-of-batch in one activation.
    /// * **Throttle outputs.** A `map("1KB-string")` MFP produces large records per input;
    ///   stopping when emit count hits the threshold caps how much data a single activation
    ///   dumps on the next operator.
    ///
    /// The refuel constant is a pragmatic compromise: large enough to be a non-event in
    /// steady-state, small enough that one activation can't flood downstream. There is no
    /// universal value across MFP shapes.
    ///
    /// If `key` is set, this is a promise that `logic` will produce no results on
    /// records for which the key does not evaluate to the value. This is used to
    /// leap directly to exactly those records.
    ///
    /// The `max_demand` parameter limits the number of columns decoded from the
    /// input. Only the first `max_demand` columns are decoded. Pass `usize::MAX` to
    /// decode all columns.
    pub fn flat_map<D, DCB, L>(
        &self,
        key: Option<&Row>,
        max_demand: usize,
        mut logic: L,
    ) -> (
        Stream<'scope, T, DCB::Container>,
        VecCollection<'scope, T, DataflowErrorSer, Diff>,
    )
    where
        D: Data,
        DCB: ContainerBuilder + PushInto<(D, T, Diff)>,
        L: for<'a, 'b> FnMut(
                &'a mut DatumVecBorrow<'b>,
                T,
                Diff,
                &mut Session<T, DCB>,
                &mut Session<T, ECB<T>>,
            ) -> usize
            + 'static,
    {
        let mut datums = DatumVec::new();
        let logic = move |k: DatumSeq,
                          v: DatumSeq,
                          t,
                          d,
                          ok_session: &mut Session<T, DCB>,
                          err_session: &mut Session<T, ECB<T>>| {
            let mut datums_borrow = datums.borrow();
            datums_borrow.extend(k.to_datum_iter().take(max_demand));
            let max_demand = max_demand.saturating_sub(datums_borrow.len());
            datums_borrow.extend(v.to_datum_iter().take(max_demand));
            logic(&mut datums_borrow, t, d, ok_session, err_session)
        };

        match &self {
            ArrangementFlavor::Local(oks, errs) => {
                let (oks, mfp_errs) = CollectionBundle::<T>::flat_map_core_fallible::<_, _, DCB, _>(
                    oks.clone(),
                    key,
                    logic,
                    REFUEL,
                );
                let errs = errs.clone().as_collection(|k, &()| k.clone());
                let errs = errs.concat(mfp_errs.as_collection());
                (oks, errs)
            }
            ArrangementFlavor::Trace(_, oks, errs) => {
                let (oks, mfp_errs) = CollectionBundle::<T>::flat_map_core_fallible::<_, _, DCB, _>(
                    oks.clone(),
                    key,
                    logic,
                    REFUEL,
                );
                let errs = errs.clone().as_collection(|k, &()| k.clone());
                let errs = errs.concat(mfp_errs.as_collection());
                (oks, errs)
            }
        }
    }

    /// Ok-only variant of [`Self::flat_map`]. The `logic` callback receives a single output
    /// session, cannot produce errors, and returns the number of records produced (see
    /// [`Self::flat_map`] for fuel semantics). The returned err collection comes solely from
    /// the arrangement; no extra operator is built to carry an empty MFP-error stream.
    pub fn flat_map_ok<D, DCB, L>(
        &self,
        key: Option<&Row>,
        max_demand: usize,
        mut logic: L,
    ) -> (
        Stream<'scope, T, DCB::Container>,
        VecCollection<'scope, T, DataflowErrorSer, Diff>,
    )
    where
        D: Data,
        DCB: ContainerBuilder + PushInto<(D, T, Diff)>,
        L: for<'a, 'b> FnMut(&'a mut DatumVecBorrow<'b>, T, Diff, &mut Session<T, DCB>) -> usize
            + 'static,
    {
        let mut datums = DatumVec::new();
        let logic = move |k: DatumSeq, v: DatumSeq, t, d, ok_session: &mut Session<T, DCB>| {
            let mut datums_borrow = datums.borrow();
            datums_borrow.extend(k.to_datum_iter().take(max_demand));
            let max_demand = max_demand.saturating_sub(datums_borrow.len());
            datums_borrow.extend(v.to_datum_iter().take(max_demand));
            logic(&mut datums_borrow, t, d, ok_session)
        };

        match &self {
            ArrangementFlavor::Local(oks, errs) => {
                let oks = CollectionBundle::<T>::flat_map_core_ok::<_, _, DCB, _>(
                    oks.clone(),
                    key,
                    logic,
                    REFUEL,
                );
                let errs = errs.clone().as_collection(|k, &()| k.clone());
                (oks, errs)
            }
            ArrangementFlavor::Trace(_, oks, errs) => {
                let oks = CollectionBundle::<T>::flat_map_core_ok::<_, _, DCB, _>(
                    oks.clone(),
                    key,
                    logic,
                    REFUEL,
                );
                let errs = errs.clone().as_collection(|k, &()| k.clone());
                (oks, errs)
            }
        }
    }
}
impl<'scope, T: RenderTimestamp> ArrangementFlavor<'scope, T> {
    /// The scope containing the collection bundle.
    pub fn scope(&self) -> Scope<'scope, T> {
        match self {
            ArrangementFlavor::Local(oks, _errs) => oks.stream.scope(),
            ArrangementFlavor::Trace(_gid, oks, _errs) => oks.stream.scope(),
        }
    }

    /// Brings the arrangement flavor into a region.
    pub fn enter_region<'a>(&self, region: Scope<'a, T>) -> ArrangementFlavor<'a, T> {
        match self {
            ArrangementFlavor::Local(oks, errs) => ArrangementFlavor::Local(
                oks.clone().enter_region(region),
                errs.clone().enter_region(region),
            ),
            ArrangementFlavor::Trace(gid, oks, errs) => ArrangementFlavor::Trace(
                *gid,
                oks.clone().enter_region(region),
                errs.clone().enter_region(region),
            ),
        }
    }
}
impl<'scope, T: RenderTimestamp> ArrangementFlavor<'scope, T> {
    /// Extracts the arrangement flavor from a region.
    pub fn leave_region<'outer>(&self, outer: Scope<'outer, T>) -> ArrangementFlavor<'outer, T> {
        match self {
            ArrangementFlavor::Local(oks, errs) => ArrangementFlavor::Local(
                oks.clone().leave_region(outer),
                errs.clone().leave_region(outer),
            ),
            ArrangementFlavor::Trace(gid, oks, errs) => ArrangementFlavor::Trace(
                *gid,
                oks.clone().leave_region(outer),
                errs.clone().leave_region(outer),
            ),
        }
    }
}

/// A bundle of the various ways a collection can be represented.
///
/// This type maintains the invariant that it does contain at least one valid
/// source of data, either a collection or at least one arrangement.
#[derive(Clone)]
pub struct CollectionBundle<'scope, T: RenderTimestamp> {
    pub collection: Option<(
        VecCollection<'scope, T, Row, Diff>,
        VecCollection<'scope, T, DataflowErrorSer, Diff>,
    )>,
    pub arranged: BTreeMap<Vec<MirScalarExpr>, ArrangementFlavor<'scope, T>>,
}

impl<'scope, T: RenderTimestamp> CollectionBundle<'scope, T> {
    /// Construct a new collection bundle from update streams.
    pub fn from_collections(
        oks: VecCollection<'scope, T, Row, Diff>,
        errs: VecCollection<'scope, T, DataflowErrorSer, Diff>,
    ) -> Self {
        Self {
            collection: Some((oks, errs)),
            arranged: BTreeMap::default(),
        }
    }

    /// Inserts arrangements by the expressions on which they are keyed.
    pub fn from_expressions(
        exprs: Vec<MirScalarExpr>,
        arrangements: ArrangementFlavor<'scope, T>,
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
        arrangements: ArrangementFlavor<'scope, T>,
    ) -> Self {
        let mut keys = Vec::new();
        for column in columns {
            keys.push(MirScalarExpr::column(column));
        }
        Self::from_expressions(keys, arrangements)
    }

    /// The scope containing the collection bundle.
    pub fn scope(&self) -> Scope<'scope, T> {
        if let Some((oks, _errs)) = &self.collection {
            oks.inner.scope()
        } else {
            self.arranged
                .values()
                .next()
                .expect("Must contain a valid collection")
                .scope()
        }
    }

    /// Brings the collection bundle into a region.
    pub fn enter_region<'inner>(&self, region: Scope<'inner, T>) -> CollectionBundle<'inner, T> {
        CollectionBundle {
            collection: self.collection.as_ref().map(|(oks, errs)| {
                (
                    oks.clone().enter_region(region),
                    errs.clone().enter_region(region),
                )
            }),
            arranged: self
                .arranged
                .iter()
                .map(|(key, bundle)| (key.clone(), bundle.enter_region(region)))
                .collect(),
        }
    }
}

impl<'scope, T: RenderTimestamp> CollectionBundle<'scope, T> {
    /// Extracts the collection bundle from a region.
    pub fn leave_region<'outer>(&self, outer: Scope<'outer, T>) -> CollectionBundle<'outer, T> {
        CollectionBundle {
            collection: self.collection.as_ref().map(|(oks, errs)| {
                (
                    oks.clone().leave_region(outer),
                    errs.clone().leave_region(outer),
                )
            }),
            arranged: self
                .arranged
                .iter()
                .map(|(key, bundle)| (key.clone(), bundle.leave_region(outer)))
                .collect(),
        }
    }
}

impl<'scope, T: RenderTimestamp> CollectionBundle<'scope, T> {
    /// Asserts that the arrangement for a specific key
    /// (or the raw collection for no key) exists,
    /// and returns the corresponding collection.
    ///
    /// This returns the collection as-is, without
    /// doing any unthinning transformation.
    /// Therefore, it should be used when the appropriate transformation
    /// was planned as part of a following MFP.
    ///
    /// If `key` is specified, the function converts the arrangement to a collection. It uses either
    /// the fueled `flat_map` or `as_collection` method, depending on the flag
    /// [`ENABLE_COMPUTE_RENDER_FUELED_AS_SPECIFIC_COLLECTION`].
    pub fn as_specific_collection(
        &self,
        key: Option<&[MirScalarExpr]>,
        config_set: &ConfigSet,
    ) -> (
        VecCollection<'scope, T, Row, Diff>,
        VecCollection<'scope, T, DataflowErrorSer, Diff>,
    ) {
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
            Some(key) => {
                let arranged = self.arranged.get(key).unwrap_or_else(|| {
                    panic!("The collection arranged by {:?} doesn't exist.", key)
                });
                if ENABLE_COMPUTE_RENDER_FUELED_AS_SPECIFIC_COLLECTION.get(config_set) {
                    // Decode all columns, pass max_demand as usize::MAX. Output is 1:1 from the
                    // cursor (no duplicates), so a non-consolidating container builder is the
                    // right choice.
                    let (ok, err) = arranged
                        .flat_map_ok::<_, CapacityContainerBuilder<Vec<(Row, T, Diff)>>, _>(
                            None,
                            usize::MAX,
                            |borrow, t, r, ok_session| {
                                ok_session.give((SharedRow::pack(borrow.iter()), t, r));
                                1
                            },
                        );
                    (ok.as_collection(), err)
                } else {
                    #[allow(deprecated)]
                    arranged.as_collection()
                }
            }
        }
    }

    /// Constructs and applies logic to elements of a collection and returns the results.
    ///
    /// The function applies `logic` on elements. The logic conceptually receives
    /// `(&Row, &Row)` pairs in the form of a datum vec in the expected order.
    ///
    /// If `key_val` is set, this is a promise that `logic` will produce no results on
    /// records for which the key does not evaluate to the value. This is used when we
    /// have an arrangement by that key to leap directly to exactly those records.
    /// It is important that `logic` still guard against data that does not satisfy
    /// this constraint, as this method does not statically know that it will have
    /// that arrangement.
    ///
    /// The `max_demand` parameter limits the number of columns decoded from the
    /// input. Only the first `max_demand` columns are decoded. Pass `usize::MAX` to
    /// decode all columns.
    pub fn flat_map<D, DCB, L>(
        &self,
        key_val: Option<(Vec<MirScalarExpr>, Option<Row>)>,
        max_demand: usize,
        mut logic: L,
    ) -> (
        Stream<'scope, T, DCB::Container>,
        VecCollection<'scope, T, DataflowErrorSer, Diff>,
    )
    where
        D: Data,
        DCB: ContainerBuilder + PushInto<(D, T, Diff)>,
        L: for<'a> FnMut(
                &'a mut DatumVecBorrow<'_>,
                T,
                Diff,
                &mut Session<T, DCB>,
                &mut Session<T, ECB<T>>,
            ) -> usize
            + 'static,
    {
        // If `key_val` is set, we should have to use the corresponding arrangement.
        // If there isn't one, that implies an error in the contract between
        // key-production and available arrangements.
        if let Some((key, val)) = key_val {
            self.arrangement(&key)
                .expect("Should have ensured during planning that this arrangement exists.")
                .flat_map::<_, DCB, _>(val.as_ref(), max_demand, logic)
        } else {
            let (oks, errs) = self
                .collection
                .clone()
                .expect("Invariant violated: CollectionBundle contains no collection.");
            let scope = oks.inner.scope();
            let mut builder = OperatorBuilder::new("CollectionFlatMap".to_string(), scope);
            let (ok_output, ok_stream) = builder.new_output();
            let mut ok_output = OutputBuilder::<_, DCB>::from(ok_output);
            let (err_output, err_stream) = builder.new_output();
            let mut err_output = OutputBuilder::<_, ECB<T>>::from(err_output);
            let mut input = builder.new_input(oks.inner, Pipeline);
            builder.build(move |_capabilities| {
                let mut datums = DatumVec::new();
                move |_frontiers| {
                    let mut ok_output = ok_output.activate();
                    let mut err_output = err_output.activate();
                    input.for_each(|time, data| {
                        // Retain the input capability to derive a `Capability` for each output;
                        // the `Session` type alias is fixed to `Capability<T>`.
                        let ok_cap = time.retain(0);
                        let err_cap = time.retain(1);
                        let mut ok_session = ok_output.session_with_builder(&ok_cap);
                        let mut err_session = err_output.session_with_builder(&err_cap);
                        for (v, t, d) in data.drain(..) {
                            logic(
                                &mut datums.borrow_with_limit(&v, max_demand),
                                t,
                                d,
                                &mut ok_session,
                                &mut err_session,
                            );
                        }
                    });
                }
            });
            let errs = errs.concat(err_stream.as_collection());
            (ok_stream, errs)
        }
    }

    /// Factored out common logic for using literal keys in general traces.
    ///
    /// This logic is sufficiently interesting that we want to write it only
    /// once, and thereby avoid any skew in the two uses of the logic.
    ///
    /// The function presents the contents of the trace as `(key, value, time, delta)` tuples,
    /// where key and value are potentially specialized, but convertible into rows. The `logic`
    /// callback writes ok results into the first session and errors into the second, returning
    /// the number of records produced. See [`ArrangementFlavor::flat_map`] for the fuel
    /// rationale.
    fn flat_map_core_fallible<Tr, D, DCB, L>(
        trace: Arranged<'scope, Tr>,
        key: Option<&<Tr::KeyContainer as BatchContainer>::Owned>,
        mut logic: L,
        refuel: usize,
    ) -> (
        Stream<'scope, T, DCB::Container>,
        Stream<'scope, T, Vec<(DataflowErrorSer, T, Diff)>>,
    )
    where
        Tr: for<'a> TraceReader<
                Key<'a>: ToDatumIter,
                Val<'a>: ToDatumIter,
                Time = T,
                Diff = mz_repr::Diff,
            > + Clone
            + 'static,
        <Tr::KeyContainer as BatchContainer>::Owned: PartialEq,
        D: Data,
        DCB: ContainerBuilder + PushInto<(D, T, Diff)>,
        L: FnMut(
                Tr::Key<'_>,
                Tr::Val<'_>,
                T,
                mz_repr::Diff,
                &mut Session<T, DCB>,
                &mut Session<T, ECB<T>>,
            ) -> usize
            + 'static,
    {
        let scope = trace.stream.scope();

        let mut key_con = Tr::KeyContainer::with_capacity(1);
        if let Some(key) = &key {
            key_con.push_own(key);
        }
        let mode = if key.is_some() { "index" } else { "scan" };
        let name = format!("ArrangementFlatMap({})", mode);

        let mut builder = OperatorBuilder::new(name, scope.clone());
        let (ok_output, ok_stream) = builder.new_output();
        let mut ok_output = OutputBuilder::<_, DCB>::from(ok_output);
        let (err_output, err_stream) = builder.new_output();
        let mut err_output = OutputBuilder::<_, ECB<T>>::from(err_output);
        let mut input = builder.new_input(trace.stream.clone(), Pipeline);
        let operator_info = builder.operator_info();

        builder.build(move |_capabilities| {
            // Acquire an activator to reschedule the operator when it has unfinished work.
            let activator = scope.activator_for(operator_info.address);
            // Maintain a list of work to do, cursor to navigate and process.
            let mut todo = std::collections::VecDeque::new();
            move |_frontiers| {
                let key = key_con.get(0);
                let mut ok_output = ok_output.activate();
                let mut err_output = err_output.activate();

                // First, dequeue all batches.
                input.for_each(|time, data| {
                    // Retain a capability for each output, as the work may complete across
                    // multiple activations.
                    let ok_cap = time.retain(0);
                    let err_cap = time.retain(1);
                    for batch in data.iter() {
                        todo.push_back(PendingWork::new(
                            ok_cap.clone(),
                            err_cap.clone(),
                            batch.cursor(),
                            batch.clone(),
                        ));
                    }
                });

                // Second, make progress on `todo`.
                let mut fuel = refuel;
                while !todo.is_empty() && fuel > 0 {
                    todo.front_mut().unwrap().do_work(
                        key.as_ref(),
                        &mut logic,
                        &mut fuel,
                        &mut ok_output,
                        &mut err_output,
                    );
                    if fuel > 0 {
                        todo.pop_front();
                    }
                }
                // If we have not finished all work, re-activate the operator.
                if !todo.is_empty() {
                    activator.activate();
                }
            }
        });

        (ok_stream, err_stream)
    }

    /// Ok-only variant of [`Self::flat_map_core_fallible`]. The `logic` callback writes results
    /// into a single output session and returns the number of records produced (see the
    /// fallible variant for fuel semantics). Use this when the caller statically knows it
    /// will never produce `DataflowErrorSer` records, to avoid building a second output port
    /// and the empty err stream that would follow it.
    fn flat_map_core_ok<Tr, D, DCB, L>(
        trace: Arranged<'scope, Tr>,
        key: Option<&<Tr::KeyContainer as BatchContainer>::Owned>,
        mut logic: L,
        refuel: usize,
    ) -> Stream<'scope, T, DCB::Container>
    where
        Tr: for<'a> TraceReader<
                Key<'a>: ToDatumIter,
                Val<'a>: ToDatumIter,
                Time = T,
                Diff = mz_repr::Diff,
            > + Clone
            + 'static,
        <Tr::KeyContainer as BatchContainer>::Owned: PartialEq,
        D: Data,
        DCB: ContainerBuilder + PushInto<(D, T, Diff)>,
        L: FnMut(Tr::Key<'_>, Tr::Val<'_>, T, mz_repr::Diff, &mut Session<T, DCB>) -> usize
            + 'static,
    {
        let scope = trace.stream.scope();

        let mut key_con = Tr::KeyContainer::with_capacity(1);
        if let Some(key) = &key {
            key_con.push_own(key);
        }
        let mode = if key.is_some() { "index" } else { "scan" };
        let name = format!("ArrangementFlatMapOk({})", mode);

        let mut builder = OperatorBuilder::new(name, scope.clone());
        let (ok_output, ok_stream) = builder.new_output();
        let mut ok_output = OutputBuilder::<_, DCB>::from(ok_output);
        let mut input = builder.new_input(trace.stream.clone(), Pipeline);
        let operator_info = builder.operator_info();

        builder.build(move |_capabilities| {
            let activator = scope.activator_for(operator_info.address);
            let mut todo = std::collections::VecDeque::new();
            move |_frontiers| {
                let key = key_con.get(0);
                let mut ok_output = ok_output.activate();

                input.for_each(|time, data| {
                    let cap = time.retain(0);
                    for batch in data.iter() {
                        todo.push_back(PendingWorkOk::new(
                            cap.clone(),
                            batch.cursor(),
                            batch.clone(),
                        ));
                    }
                });

                let mut fuel = refuel;
                while !todo.is_empty() && fuel > 0 {
                    todo.front_mut().unwrap().do_work(
                        key.as_ref(),
                        &mut logic,
                        &mut fuel,
                        &mut ok_output,
                    );
                    if fuel > 0 {
                        todo.pop_front();
                    }
                }
                if !todo.is_empty() {
                    activator.activate();
                }
            }
        });

        ok_stream
    }

    /// Look up an arrangement by the expressions that form the key.
    ///
    /// The result may be `None` if no such arrangement exists, or it may be one of many
    /// "arrangement flavors" that represent the types of arranged data we might have.
    pub fn arrangement(&self, key: &[MirScalarExpr]) -> Option<ArrangementFlavor<'scope, T>> {
        self.arranged.get(key).map(|x| x.clone())
    }
}

impl<'scope, T: RenderTimestamp> CollectionBundle<'scope, T> {
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
        until: Antichain<mz_repr::Timestamp>,
        config_set: &ConfigSet,
    ) -> (
        VecCollection<'scope, T, mz_repr::Row, Diff>,
        VecCollection<'scope, T, DataflowErrorSer, Diff>,
    ) {
        mfp.optimize();
        let mfp_plan = mfp.clone().into_plan().unwrap();

        // If the MFP is trivial, we can just call `as_collection`.
        // In the case that we weren't going to apply the `key_val` optimization,
        // this path results in a slightly smaller and faster
        // dataflow graph, and is intended to fix
        // https://github.com/MaterializeInc/database-issues/issues/3111
        let has_key_val = if let Some((_key, Some(_val))) = &key_val {
            true
        } else {
            false
        };

        if mfp_plan.is_identity() && !has_key_val {
            let key = key_val.map(|(k, _v)| k);
            return self.as_specific_collection(key.as_deref(), config_set);
        }

        let max_demand = mfp.demand().last().map(|x| *x + 1).unwrap_or(0);
        mfp.permute_fn(|c| c, max_demand);
        mfp.optimize();
        let mfp_plan = mfp.into_plan().unwrap();

        let mut datum_vec = DatumVec::new();
        // Wrap in an `Rc` so that lifetimes work out.
        let until = std::rc::Rc::new(until);

        let (stream, errors) = self
            .flat_map::<_, ConsolidatingContainerBuilder<Vec<(Row, T, Diff)>>, _>(
                key_val,
                max_demand,
                move |row_datums, time, diff, ok_session, err_session| {
                    let mut row_builder = SharedRow::get();
                    let until = std::rc::Rc::clone(&until);
                    let temp_storage = RowArena::new();
                    let row_iter = row_datums.iter();
                    let mut datums_local = datum_vec.borrow();
                    datums_local.extend(row_iter);
                    let event_time = time.event_time();
                    let mut work: usize = 0;
                    for result in mfp_plan.evaluate(
                        &mut datums_local,
                        &temp_storage,
                        event_time,
                        diff.clone(),
                        move |time| !until.less_equal(time),
                        &mut row_builder,
                    ) {
                        work += 1;
                        match result {
                            Ok((row, event_time, diff)) => {
                                // Copy the whole time, and re-populate event time.
                                let mut time: T = time.clone();
                                *time.event_time_mut() = event_time;
                                ok_session.give((row, time, diff));
                            }
                            Err((e, event_time, diff)) => {
                                // Copy the whole time, and re-populate event time.
                                let mut time: T = time.clone();
                                *time.event_time_mut() = event_time;
                                err_session.give((e, time, diff));
                            }
                        }
                    }
                    work
                },
            );

        (stream.as_collection(), errors)
    }
    pub fn ensure_collections(
        mut self,
        collections: AvailableCollections,
        input_key: Option<Vec<MirScalarExpr>>,
        input_mfp: MapFilterProject,
        as_of: Antichain<mz_repr::Timestamp>,
        until: Antichain<mz_repr::Timestamp>,
        config_set: &ConfigSet,
        strategy: ArrangementStrategy,
    ) -> Self
    where
        T: MaybeBucketByTime,
    {
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

        for (key, _, _) in collections.arranged.iter() {
            soft_assert_or_log!(
                !self.arranged.contains_key(key),
                "LIR ArrangeBy tried to create an existing arrangement"
            );
        }

        // Track whether we already applied temporal bucketing in this call, to
        // avoid bucketing the same updates twice.
        let mut bucketed = false;

        // True iff at least one new arrangement will actually be built below. Bucketing only
        // pays off when something downstream merges/compacts the future-stamped updates; on a
        // pure raw collection (no new arrangement) the work is wasted.
        let will_create_arrangement = collections
            .arranged
            .iter()
            .any(|(key, _, _)| !self.arranged.contains_key(key));

        // We need the collection if either (1) it is explicitly demanded, or (2) we are going to render any arrangement
        let form_raw_collection = collections.raw || will_create_arrangement;
        if form_raw_collection && self.collection.is_none() {
            let (oks, errs) =
                self.as_collection_core(input_mfp, input_key.map(|k| (k, None)), until, config_set);
            // Apply temporal bucketing when the lowering selected `TemporalBucketing` and
            // we will build at least one arrangement. This path fires when the collection
            // must be formed from scratch (e.g., from an arrangement via as_collection_core).
            let effective_strategy = if will_create_arrangement {
                strategy
            } else {
                ArrangementStrategy::Direct
            };
            let oks = if matches!(effective_strategy, ArrangementStrategy::TemporalBucketing)
                && ENABLE_COMPUTE_TEMPORAL_BUCKETING.get(config_set)
            {
                let summary: mz_repr::Timestamp = TEMPORAL_BUCKETING_SUMMARY
                    .get(config_set)
                    .try_into()
                    .expect("must fit");
                bucketed = true;
                T::maybe_apply_temporal_bucketing(oks.inner, as_of.clone(), summary)
            } else {
                oks
            };
            self.collection = Some((oks, errs));
        }
        for (key, _, thinning) in collections.arranged {
            if !self.arranged.contains_key(&key) {
                // TODO: Consider allowing more expressive names.
                let name = format!("ArrangeBy[{:?}]", key);

                let (oks, errs) = self
                    .collection
                    .take()
                    .expect("Collection constructed above");
                // Apply temporal bucketing if the collection already existed on
                // the bundle (e.g., from an upstream temporal Mfp or Get) and we
                // haven't bucketed yet. This is the common path for temporal-MFP
                // → ArrangeBy flows.
                let effective_strategy = if bucketed {
                    ArrangementStrategy::Direct
                } else {
                    strategy
                };
                let oks = if matches!(effective_strategy, ArrangementStrategy::TemporalBucketing)
                    && ENABLE_COMPUTE_TEMPORAL_BUCKETING.get(config_set)
                {
                    let summary: mz_repr::Timestamp = TEMPORAL_BUCKETING_SUMMARY
                        .get(config_set)
                        .try_into()
                        .expect("must fit");
                    bucketed = true;
                    T::maybe_apply_temporal_bucketing(oks.inner, as_of.clone(), summary)
                } else {
                    oks
                };
                let (oks, errs_keyed, passthrough) =
                    Self::arrange_collection(&name, oks, key.clone(), thinning.clone());
                let errs_concat: KeyCollection<_, _, _> = errs.clone().concat(errs_keyed).into();
                self.collection = Some((passthrough, errs));
                let errs =
                    errs_concat.mz_arrange::<ErrBatcher<_, _>, ErrBuilder<_, _>, ErrSpine<_, _>>(
                        &format!("{}-errors", name),
                    );
                self.arranged
                    .insert(key, ArrangementFlavor::Local(oks, errs));
            }
        }
        self
    }

    /// Builds an arrangement from a collection, using the specified key and value thinning.
    ///
    /// The arrangement's key is based on the `key` expressions, and the value the input with
    /// the `thinning` applied to it. It selects which of the input columns are included in the
    /// value of the arrangement. The thinning is in support of permuting arrangements such that
    /// columns in the key are not included in the value.
    ///
    /// In addition to the ok and err streams, we produce a passthrough stream that forwards
    /// the input as-is, which allows downstream consumers to reuse the collection without
    /// teeing the stream.
    fn arrange_collection(
        name: &String,
        oks: VecCollection<'scope, T, Row, Diff>,
        key: Vec<MirScalarExpr>,
        thinning: Vec<usize>,
    ) -> (
        Arranged<'scope, RowRowAgent<T, Diff>>,
        VecCollection<'scope, T, DataflowErrorSer, Diff>,
        VecCollection<'scope, T, Row, Diff>,
    ) {
        // This operator implements a `map_fallible`, but produces columnar updates for the ok
        // stream. The `map_fallible` cannot be used here because the closure cannot return
        // references, which is what we need to push into columnar streams. Instead, we use a
        // bespoke operator that also optimizes reuse of allocations across individual updates.
        let mut builder = OperatorBuilder::new("FormArrangementKey".to_string(), oks.inner.scope());
        let (ok_output, ok_stream) = builder.new_output();
        let mut ok_output =
            OutputBuilder::<_, ColumnBuilder<((Row, Row), T, Diff)>>::from(ok_output);
        let (err_output, err_stream) = builder.new_output();
        let mut err_output = OutputBuilder::from(err_output);
        let (passthrough_output, passthrough_stream) = builder.new_output();
        let mut passthrough_output = OutputBuilder::from(passthrough_output);
        let mut input = builder.new_input(oks.inner, Pipeline);
        builder.set_notify_for(0, FrontierInterest::Never);
        builder.build(move |_capabilities| {
            let mut key_buf = Row::default();
            let mut val_buf = Row::default();
            let mut datums = DatumVec::new();
            let mut temp_storage = RowArena::new();
            move |_frontiers| {
                let mut ok_output = ok_output.activate();
                let mut err_output = err_output.activate();
                let mut passthrough_output = passthrough_output.activate();
                input.for_each(|time, data| {
                    let mut ok_session = ok_output.session_with_builder(&time);
                    let mut err_session = err_output.session(&time);
                    for (row, time, diff) in data.iter() {
                        temp_storage.clear();
                        let datums = datums.borrow_with(row);
                        let key_iter = key.iter().map(|k| k.eval(&datums, &temp_storage));
                        match key_buf.packer().try_extend(key_iter) {
                            Ok(()) => {
                                let val_datum_iter = thinning.iter().map(|c| datums[*c]);
                                val_buf.packer().extend(val_datum_iter);
                                ok_session.give(((&*key_buf, &*val_buf), time, diff));
                            }
                            Err(e) => {
                                err_session.give((e.into(), time.clone(), *diff));
                            }
                        }
                    }
                    passthrough_output.session(&time).give_container(data);
                });
            }
        });

        let oks = ok_stream
            .mz_arrange_core::<
                _,
                Col2ValBatcher<_, _, _, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >(
                ExchangeCore::<ColumnBuilder<_>, _>::new_core(
                    columnar_exchange::<Row, Row, T, Diff>,
                ),
                name
            );
        (
            oks,
            err_stream.as_collection(),
            passthrough_stream.as_collection(),
        )
    }
}

/// Type alias for a timely output `Session` whose capability is a `Capability<T>`. The container
/// builder `CB` is left to the caller; sessions can therefore drive consolidating, capacity, or
/// (in the future) columnar output builders without changing call sites.
type Session<'a, 'b, T, CB> =
    timely::dataflow::operators::generic::Session<'a, 'b, T, CB, Capability<T>>;

/// Container builder used for the err output of every flat_map variant. Pre-refactor the
/// merged Ok/Err stream flowed through a [`ConsolidatingContainerBuilder`] before the
/// `map_fallible` demux split it; we preserve that consolidation here so errors with the
/// same `(error, time)` cancel within a batch rather than propagating to downstream.
type ECB<T> = ConsolidatingContainerBuilder<Vec<(DataflowErrorSer, T, Diff)>>;

/// Number of output records the arrangement flat_map operators may produce before yielding.
/// See [`ArrangementFlavor::flat_map`] for the fuel rationale; the constant is a pragmatic
/// compromise and not tuned empirically.
const REFUEL: usize = 1_000_000;

struct PendingWork<C>
where
    C: Cursor,
{
    /// Capability for the `ok` output (output port 0).
    ok_capability: Capability<C::Time>,
    /// Capability for the `err` output (output port 1).
    err_capability: Capability<C::Time>,
    cursor: C,
    batch: C::Storage,
}

impl<C> PendingWork<C>
where
    C: Cursor<KeyContainer: BatchContainer<Owned: PartialEq + Sized>>,
{
    /// Create a new bundle of pending work, from a pair of capabilities (one per output),
    /// a cursor, and backing storage.
    fn new(
        ok_capability: Capability<C::Time>,
        err_capability: Capability<C::Time>,
        cursor: C,
        batch: C::Storage,
    ) -> Self {
        Self {
            ok_capability,
            err_capability,
            cursor,
            batch,
        }
    }
    /// Perform roughly `fuel` work through the cursor, applying `logic` and sending results to
    /// the two output sessions.
    fn do_work<D, DCB, L>(
        &mut self,
        key: Option<&C::Key<'_>>,
        logic: &mut L,
        fuel: &mut usize,
        ok_output: &mut OutputBuilderSession<'_, C::Time, DCB>,
        err_output: &mut OutputBuilderSession<'_, C::Time, ECB<C::Time>>,
    ) where
        D: Data,
        DCB: ContainerBuilder + PushInto<(D, C::Time, C::Diff)>,
        L: FnMut(
                C::Key<'_>,
                C::Val<'_>,
                C::Time,
                C::Diff,
                &mut Session<C::Time, DCB>,
                &mut Session<C::Time, ECB<C::Time>>,
            ) -> usize
            + 'static,
    {
        let mut ok_session = ok_output.session_with_builder(&self.ok_capability);
        let mut err_session = err_output.session_with_builder(&self.err_capability);
        walk_cursor(&mut self.cursor, &self.batch, key, fuel, |k, v, t, d| {
            logic(k, v, t, d, &mut ok_session, &mut err_session)
        });
    }
}

/// Pending work for the Ok-only variant of `flat_map_core_fallible`. Holds a single capability since
/// the operator has only one output port.
struct PendingWorkOk<C>
where
    C: Cursor,
{
    capability: Capability<C::Time>,
    cursor: C,
    batch: C::Storage,
}

impl<C> PendingWorkOk<C>
where
    C: Cursor<KeyContainer: BatchContainer<Owned: PartialEq + Sized>>,
{
    fn new(capability: Capability<C::Time>, cursor: C, batch: C::Storage) -> Self {
        Self {
            capability,
            cursor,
            batch,
        }
    }

    /// Perform roughly `fuel` work through the cursor, applying `logic` and sending results to
    /// the single output session.
    fn do_work<D, DCB, L>(
        &mut self,
        key: Option<&C::Key<'_>>,
        logic: &mut L,
        fuel: &mut usize,
        ok_output: &mut OutputBuilderSession<'_, C::Time, DCB>,
    ) where
        D: Data,
        DCB: ContainerBuilder + PushInto<(D, C::Time, C::Diff)>,
        L: FnMut(C::Key<'_>, C::Val<'_>, C::Time, C::Diff, &mut Session<C::Time, DCB>) -> usize
            + 'static,
    {
        let mut ok_session = ok_output.session_with_builder(&self.capability);
        walk_cursor(&mut self.cursor, &self.batch, key, fuel, |k, v, t, d| {
            logic(k, v, t, d, &mut ok_session)
        });
    }
}

/// Walk a cursor, calling `emit` for each consolidated `(key, val, time, diff)` tuple. If
/// `key` is set, the cursor is seeked to it and only values for that key are produced.
///
/// `emit` returns the number of records it produced for the given input tuple. The cursor
/// stops as soon as the accumulated emit count reaches `*fuel`, leaving the cursor in place
/// so work can resume on a later call. Within a batch, both the inner val loop and the
/// outer key loop are bounded only by emit count, so selective filters (`emit` returns 0)
/// run to batch completion in a single activation — see [`ArrangementFlavor::flat_map`]
/// for why fuel counts output rather than input.
fn walk_cursor<C, F>(
    cursor: &mut C,
    batch: &C::Storage,
    key: Option<&C::Key<'_>>,
    fuel: &mut usize,
    mut emit: F,
) where
    C: Cursor<KeyContainer: BatchContainer<Owned: PartialEq + Sized>>,
    F: FnMut(C::Key<'_>, C::Val<'_>, C::Time, C::Diff) -> usize,
{
    use differential_dataflow::consolidation::consolidate;

    let mut work: usize = 0;
    let mut buffer = Vec::new();
    if let Some(key) = key {
        let key = C::KeyContainer::reborrow(*key);
        if cursor.get_key(batch).map(|k| k == key) != Some(true) {
            cursor.seek_key(batch, key);
        }
        if cursor.get_key(batch).map(|k| k == key) == Some(true) {
            let key = cursor.key(batch);
            while let Some(val) = cursor.get_val(batch) {
                cursor.map_times(batch, |time, diff| {
                    buffer.push((C::owned_time(time), C::owned_diff(diff)));
                });
                consolidate(&mut buffer);
                for (time, diff) in buffer.drain(..) {
                    work += emit(key, val, time, diff);
                }
                cursor.step_val(batch);
                if work >= *fuel {
                    *fuel = 0;
                    return;
                }
            }
        }
    } else {
        while let Some(key) = cursor.get_key(batch) {
            while let Some(val) = cursor.get_val(batch) {
                cursor.map_times(batch, |time, diff| {
                    buffer.push((C::owned_time(time), C::owned_diff(diff)));
                });
                consolidate(&mut buffer);
                for (time, diff) in buffer.drain(..) {
                    work += emit(key, val, time, diff);
                }
                cursor.step_val(batch);
                if work >= *fuel {
                    *fuel = 0;
                    return;
                }
            }
            cursor.step_key(batch);
        }
    }
    *fuel -= work;
}
