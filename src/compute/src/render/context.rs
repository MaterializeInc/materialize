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

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::mpsc;

use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use differential_dataflow::{AsCollection, Collection, Data};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::dyncfgs::ENABLE_COMPUTE_RENDER_FUELED_AS_SPECIFIC_COLLECTION;
use mz_compute_types::plan::{AvailableCollections, LirId};
use mz_dyncfg::ConfigSet;
use mz_expr::{Id, MapFilterProject, MirScalarExpr};
use mz_ore::soft_assert_or_log;
use mz_repr::fixed_length::ToDatumIter;
use mz_repr::{DatumVec, DatumVecBorrow, Diff, GlobalId, Row, RowArena, SharedRow};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_timely_util::builder_async::{ButtonHandle, PressOnDropButton};
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::{Col2ValBatcher, columnar_exchange};
use mz_timely_util::operator::{CollectionExt, StreamExt};
use timely::Container;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{ExchangeCore, Pipeline};
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::OutputHandleCore;
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};
use tracing::error;

use crate::compute_state::{ComputeState, HydrationEvent};
use crate::extensions::arrange::{KeyCollection, MzArrange, MzArrangeCore};
use crate::render::errors::ErrorLogger;
use crate::render::{LinearJoinSpec, RenderTimestamp};
use crate::row_spine::{DatumSeq, RowRowBuilder};
use crate::typedefs::{
    ErrAgent, ErrBatcher, ErrBuilder, ErrEnter, ErrSpine, MzTimestamp, RowRowAgent, RowRowEnter,
    RowRowSpine,
};

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
pub struct Context<S: Scope, T = mz_repr::Timestamp>
where
    T: MzTimestamp,
    S::Timestamp: MzTimestamp + Refines<T>,
{
    /// The scope within which all managed collections exist.
    ///
    /// It is an error to add any collections not contained in this scope.
    pub(crate) scope: S,
    /// The debug name of the dataflow associated with this context.
    pub debug_name: String,
    /// The Timely ID of the dataflow associated with this context.
    pub dataflow_id: usize,
    /// Frontier before which updates should not be emitted.
    ///
    /// We *must* apply it to sinks, to ensure correct outputs.
    /// We *should* apply it to sources and imported traces, because it improves performance.
    pub as_of_frontier: Antichain<T>,
    /// Frontier after which updates should not be emitted.
    /// Used to limit the amount of work done when appropriate.
    pub until: Antichain<T>,
    /// Bindings of identifiers to collections.
    pub bindings: BTreeMap<Id, CollectionBundle<S, T>>,
    /// A handle that operators can probe to know whether the dataflow is shutting down.
    pub(super) shutdown_probe: ShutdownProbe,
    /// A logger that operators can use to report hydration events.
    ///
    /// `None` if no hydration events should be logged in this context.
    pub(super) hydration_logger: Option<HydrationLogger>,
    /// The logger, from Timely's logging framework, if logs are enabled.
    pub(super) compute_logger: Option<crate::logging::compute::Logger>,
    /// Specification for rendering linear joins.
    pub(super) linear_join_spec: LinearJoinSpec,
    /// The expiration time for dataflows in this context. The output's frontier should never advance
    /// past this frontier, except the empty frontier.
    pub dataflow_expiration: Antichain<T>,
    /// The config set for this context.
    pub config_set: Rc<ConfigSet>,
}

impl<S: Scope> Context<S>
where
    S::Timestamp: MzTimestamp + Refines<mz_repr::Timestamp>,
{
    /// Creates a new empty Context.
    pub fn for_dataflow_in<Plan>(
        dataflow: &DataflowDescription<Plan, CollectionMetadata>,
        scope: S,
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

        // Skip operator hydration logging for transient dataflows. We do this to avoid overhead
        // for slow-path peeks, but it also affects subscribes. For now that seems fine, but we may
        // want to reconsider in the future.
        //
        // Similarly, we won't capture a compute_logger for logging LIR->address mappings for transient dataflows.
        let (hydration_logger, compute_logger) = if dataflow.is_transient() {
            (None, None)
        } else {
            (
                Some(HydrationLogger {
                    export_ids: dataflow.export_ids().collect(),
                    tx: compute_state.hydration_tx.clone(),
                }),
                compute_state.compute_logger.clone(),
            )
        };

        Self {
            scope,
            debug_name: dataflow.debug_name.clone(),
            dataflow_id,
            as_of_frontier,
            until,
            bindings: BTreeMap::new(),
            shutdown_probe: Default::default(),
            hydration_logger,
            compute_logger,
            linear_join_spec: compute_state.linear_join_spec,
            dataflow_expiration,
            config_set: Rc::clone(&compute_state.worker_config),
        }
    }
}

impl<S: Scope, T> Context<S, T>
where
    T: MzTimestamp,
    S::Timestamp: MzTimestamp + Refines<T>,
{
    /// Insert a collection bundle by an identifier.
    ///
    /// This is expected to be used to install external collections (sources, indexes, other views),
    /// as well as for `Let` bindings of local collections.
    pub fn insert_id(
        &mut self,
        id: Id,
        collection: CollectionBundle<S, T>,
    ) -> Option<CollectionBundle<S, T>> {
        self.bindings.insert(id, collection)
    }
    /// Remove a collection bundle by an identifier.
    ///
    /// The primary use of this method is uninstalling `Let` bindings.
    pub fn remove_id(&mut self, id: Id) -> Option<CollectionBundle<S, T>> {
        self.bindings.remove(&id)
    }
    /// Melds a collection bundle to whatever exists.
    pub fn update_id(&mut self, id: Id, collection: CollectionBundle<S, T>) {
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
    pub fn lookup_id(&self, id: Id) -> Option<CollectionBundle<S, T>> {
        self.bindings.get(&id).cloned()
    }

    pub(super) fn error_logger(&self) -> ErrorLogger {
        ErrorLogger::new(self.shutdown_probe.clone(), self.debug_name.clone())
    }
}

impl<S: Scope, T> Context<S, T>
where
    T: MzTimestamp,
    S::Timestamp: MzTimestamp + Refines<T>,
{
    /// Brings the underlying arrangements and collections into a region.
    pub fn enter_region<'a>(
        &self,
        region: &Child<'a, S, S::Timestamp>,
        bindings: Option<&std::collections::BTreeSet<Id>>,
    ) -> Context<Child<'a, S, S::Timestamp>, T> {
        let bindings = self
            .bindings
            .iter()
            .filter(|(key, _)| bindings.as_ref().map(|b| b.contains(key)).unwrap_or(true))
            .map(|(key, bundle)| (*key, bundle.enter_region(region)))
            .collect();

        Context {
            scope: region.clone(),
            debug_name: self.debug_name.clone(),
            dataflow_id: self.dataflow_id.clone(),
            as_of_frontier: self.as_of_frontier.clone(),
            until: self.until.clone(),
            shutdown_probe: self.shutdown_probe.clone(),
            hydration_logger: self.hydration_logger.clone(),
            compute_logger: self.compute_logger.clone(),
            linear_join_spec: self.linear_join_spec.clone(),
            bindings,
            dataflow_expiration: self.dataflow_expiration.clone(),
            config_set: Rc::clone(&self.config_set),
        }
    }
}

pub(super) fn shutdown_token<G: Scope>(scope: &mut G) -> (ShutdownProbe, PressOnDropButton) {
    let (button_handle, button) = mz_timely_util::builder_async::button(scope, scope.addr());
    let probe = ShutdownProbe::new(button_handle);
    let token = button.press_on_drop();
    (probe, token)
}

/// Convenient wrapper around an optional `ButtonHandle` that can be used to check whether a
/// dataflow is shutting down.
///
/// Instances created through the `Default` impl act as if the dataflow never shuts down.
/// Instances created through [`ShutdownProbe::new`] defer to the wrapped button.
#[derive(Clone, Default)]
pub(super) struct ShutdownProbe(Option<Rc<RefCell<ButtonHandle>>>);

impl ShutdownProbe {
    /// Construct a `ShutdownProbe` instance that defers to `button`.
    fn new(button: ButtonHandle) -> Self {
        Self(Some(Rc::new(RefCell::new(button))))
    }

    /// Returns whether the dataflow is in the process of shutting down.
    ///
    /// The result of this method is synchronized among workers: It only returns `true` once all
    /// workers have dropped their shutdown token.
    pub(super) fn in_shutdown(&self) -> bool {
        match &self.0 {
            Some(t) => t.borrow_mut().all_pressed(),
            None => false,
        }
    }

    /// Returns whether the dataflow is in the process of shutting down on the current worker.
    ///
    /// In contrast to [`ShutdownProbe::in_shutdown`], this method returns `true` as soon as the
    /// current worker has dropped its shutdown token, without waiting for other workers.
    pub(super) fn in_local_shutdown(&self) -> bool {
        match &self.0 {
            Some(t) => t.borrow_mut().local_pressed(),
            None => false,
        }
    }
}

/// A logger for operator hydration events emitted for a dataflow export.
#[derive(Clone)]
pub(super) struct HydrationLogger {
    export_ids: Vec<GlobalId>,
    tx: mpsc::Sender<HydrationEvent>,
}

impl HydrationLogger {
    /// Log a hydration event for the identified LIR node.
    ///
    /// The expectation is that rendering code arranges for `hydrated = false` to be logged for
    /// each LIR node when a dataflow is first created. Then `hydrated = true` should be logged as
    /// operators become hydrated.
    pub fn log(&self, lir_id: LirId, hydrated: bool) {
        for &export_id in &self.export_ids {
            let event = HydrationEvent {
                export_id,
                lir_id,
                hydrated,
            };
            if self.tx.send(event).is_err() {
                error!("hydration event receiver dropped unexpectely");
            }
        }
    }
}

/// Describes flavor of arrangement: local or imported trace.
#[derive(Clone)]
pub enum ArrangementFlavor<S: Scope, T = mz_repr::Timestamp>
where
    T: MzTimestamp,
    S::Timestamp: MzTimestamp + Refines<T>,
{
    /// A dataflow-local arrangement.
    Local(
        Arranged<S, RowRowAgent<S::Timestamp, Diff>>,
        Arranged<S, ErrAgent<S::Timestamp, Diff>>,
    ),
    /// An imported trace from outside the dataflow.
    ///
    /// The `GlobalId` identifier exists so that exports of this same trace
    /// can refer back to and depend on the original instance.
    Trace(
        GlobalId,
        Arranged<S, RowRowEnter<T, Diff, S::Timestamp>>,
        Arranged<S, ErrEnter<T, S::Timestamp>>,
    ),
}

impl<S: Scope, T> ArrangementFlavor<S, T>
where
    T: MzTimestamp,
    S::Timestamp: MzTimestamp + Refines<T>,
{
    /// Presents `self` as a stream of updates.
    ///
    /// Deprecated: This function is not fueled and hence risks flattening the whole arrangement.
    ///
    /// This method presents the contents as they are, without further computation.
    /// If you have logic that could be applied to each record, consider using the
    /// `flat_map` methods which allows this and can reduce the work done.
    #[deprecated(note = "Use `flat_map` instead.")]
    pub fn as_collection(&self) -> (Collection<S, Row, Diff>, Collection<S, DataflowError, Diff>) {
        let mut datums = DatumVec::new();
        let logic = move |k: DatumSeq, v: DatumSeq| {
            let mut datums_borrow = datums.borrow();
            datums_borrow.extend(k);
            datums_borrow.extend(v);
            SharedRow::pack(&**datums_borrow)
        };
        match &self {
            ArrangementFlavor::Local(oks, errs) => (
                oks.as_collection(logic),
                errs.as_collection(|k, &()| k.clone()),
            ),
            ArrangementFlavor::Trace(_, oks, errs) => (
                oks.as_collection(logic),
                errs.as_collection(|k, &()| k.clone()),
            ),
        }
    }

    /// Constructs and applies logic to elements of `self` and returns the results.
    ///
    /// The `logic` receives a vector of datums, a timestamp, and a diff, and produces
    /// an iterator of `(D, S::Timestamp, Diff)` updates.
    ///
    /// If `key` is set, this is a promise that `logic` will produce no results on
    /// records for which the key does not evaluate to the value. This is used to
    /// leap directly to exactly those records.
    ///
    /// The `max_demand` parameter limits the number of columns decoded from the
    /// input. Only the first `max_demand` columns are decoded. Pass `usize::MAX` to
    /// decode all columns.
    pub fn flat_map<D, I, L>(
        &self,
        key: Option<&Row>,
        max_demand: usize,
        mut logic: L,
    ) -> (Stream<S, I::Item>, Collection<S, DataflowError, Diff>)
    where
        I: IntoIterator<Item = (D, S::Timestamp, Diff)>,
        D: Data,
        L: for<'a, 'b> FnMut(&'a mut DatumVecBorrow<'b>, S::Timestamp, Diff) -> I + 'static,
    {
        // Set a number of tuples after which the operator should yield.
        // This allows us to remain responsive even when enumerating a substantial
        // arrangement, as well as provides time to accumulate our produced output.
        let refuel = 1000000;

        let mut datums = DatumVec::new();
        let logic = move |k: DatumSeq, v: DatumSeq, t, d| {
            let mut datums_borrow = datums.borrow();
            datums_borrow.extend(k.to_datum_iter().take(max_demand));
            let max_demand = max_demand.saturating_sub(datums_borrow.len());
            datums_borrow.extend(v.to_datum_iter().take(max_demand));
            logic(&mut datums_borrow, t, d)
        };

        match &self {
            ArrangementFlavor::Local(oks, errs) => {
                let oks = CollectionBundle::<S, T>::flat_map_core(oks, key, logic, refuel);
                let errs = errs.as_collection(|k, &()| k.clone());
                (oks, errs)
            }
            ArrangementFlavor::Trace(_, oks, errs) => {
                let oks = CollectionBundle::<S, T>::flat_map_core(oks, key, logic, refuel);
                let errs = errs.as_collection(|k, &()| k.clone());
                (oks, errs)
            }
        }
    }
}
impl<S: Scope, T> ArrangementFlavor<S, T>
where
    T: MzTimestamp,
    S::Timestamp: MzTimestamp + Refines<T>,
{
    /// The scope containing the collection bundle.
    pub fn scope(&self) -> S {
        match self {
            ArrangementFlavor::Local(oks, _errs) => oks.stream.scope(),
            ArrangementFlavor::Trace(_gid, oks, _errs) => oks.stream.scope(),
        }
    }

    /// Brings the arrangement flavor into a region.
    pub fn enter_region<'a>(
        &self,
        region: &Child<'a, S, S::Timestamp>,
    ) -> ArrangementFlavor<Child<'a, S, S::Timestamp>, T> {
        match self {
            ArrangementFlavor::Local(oks, errs) => {
                ArrangementFlavor::Local(oks.enter_region(region), errs.enter_region(region))
            }
            ArrangementFlavor::Trace(gid, oks, errs) => {
                ArrangementFlavor::Trace(*gid, oks.enter_region(region), errs.enter_region(region))
            }
        }
    }
}
impl<'a, S: Scope, T> ArrangementFlavor<Child<'a, S, S::Timestamp>, T>
where
    T: MzTimestamp,
    S::Timestamp: MzTimestamp + Refines<T>,
{
    /// Extracts the arrangement flavor from a region.
    pub fn leave_region(&self) -> ArrangementFlavor<S, T> {
        match self {
            ArrangementFlavor::Local(oks, errs) => {
                ArrangementFlavor::Local(oks.leave_region(), errs.leave_region())
            }
            ArrangementFlavor::Trace(gid, oks, errs) => {
                ArrangementFlavor::Trace(*gid, oks.leave_region(), errs.leave_region())
            }
        }
    }
}

/// A bundle of the various ways a collection can be represented.
///
/// This type maintains the invariant that it does contain at least one valid
/// source of data, either a collection or at least one arrangement.
#[derive(Clone)]
pub struct CollectionBundle<S: Scope, T = mz_repr::Timestamp>
where
    T: MzTimestamp,
    S::Timestamp: MzTimestamp + Refines<T>,
{
    pub collection: Option<(Collection<S, Row, Diff>, Collection<S, DataflowError, Diff>)>,
    pub arranged: BTreeMap<Vec<MirScalarExpr>, ArrangementFlavor<S, T>>,
}

impl<S: Scope, T> CollectionBundle<S, T>
where
    T: MzTimestamp,
    S::Timestamp: MzTimestamp + Refines<T>,
{
    /// Construct a new collection bundle from update streams.
    pub fn from_collections(
        oks: Collection<S, Row, Diff>,
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
        arrangements: ArrangementFlavor<S, T>,
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
        arrangements: ArrangementFlavor<S, T>,
    ) -> Self {
        let mut keys = Vec::new();
        for column in columns {
            keys.push(MirScalarExpr::column(column));
        }
        Self::from_expressions(keys, arrangements)
    }

    /// The scope containing the collection bundle.
    pub fn scope(&self) -> S {
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
    pub fn enter_region<'a>(
        &self,
        region: &Child<'a, S, S::Timestamp>,
    ) -> CollectionBundle<Child<'a, S, S::Timestamp>, T> {
        CollectionBundle {
            collection: self
                .collection
                .as_ref()
                .map(|(oks, errs)| (oks.enter_region(region), errs.enter_region(region))),
            arranged: self
                .arranged
                .iter()
                .map(|(key, bundle)| (key.clone(), bundle.enter_region(region)))
                .collect(),
        }
    }
}

impl<'a, S: Scope, T> CollectionBundle<Child<'a, S, S::Timestamp>, T>
where
    T: MzTimestamp,
    S::Timestamp: MzTimestamp + Refines<T>,
{
    /// Extracts the collection bundle from a region.
    pub fn leave_region(&self) -> CollectionBundle<S, T> {
        CollectionBundle {
            collection: self
                .collection
                .as_ref()
                .map(|(oks, errs)| (oks.leave_region(), errs.leave_region())),
            arranged: self
                .arranged
                .iter()
                .map(|(key, bundle)| (key.clone(), bundle.leave_region()))
                .collect(),
        }
    }
}

impl<S: Scope, T> CollectionBundle<S, T>
where
    T: MzTimestamp,
    S::Timestamp: MzTimestamp + Refines<T>,
{
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
            Some(key) => {
                let arranged = self.arranged.get(key).unwrap_or_else(|| {
                    panic!("The collection arranged by {:?} doesn't exist.", key)
                });
                if ENABLE_COMPUTE_RENDER_FUELED_AS_SPECIFIC_COLLECTION.get(config_set) {
                    // Decode all columns, pass max_demand as usize::MAX.
                    let (ok, err) = arranged.flat_map(None, usize::MAX, |borrow, t, r| {
                        Some((SharedRow::pack(borrow.iter()), t, r))
                    });
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
    pub fn flat_map<D, I, L>(
        &self,
        key_val: Option<(Vec<MirScalarExpr>, Option<Row>)>,
        max_demand: usize,
        mut logic: L,
    ) -> (Stream<S, I::Item>, Collection<S, DataflowError, Diff>)
    where
        I: IntoIterator<Item = (D, S::Timestamp, Diff)>,
        D: Data,
        L: for<'a> FnMut(&'a mut DatumVecBorrow<'_>, S::Timestamp, Diff) -> I + 'static,
    {
        // If `key_val` is set, we should have to use the corresponding arrangement.
        // If there isn't one, that implies an error in the contract between
        // key-production and available arrangements.
        if let Some((key, val)) = key_val {
            self.arrangement(&key)
                .expect("Should have ensured during planning that this arrangement exists.")
                .flat_map(val.as_ref(), max_demand, logic)
        } else {
            use timely::dataflow::operators::Map;
            let (oks, errs) = self
                .collection
                .clone()
                .expect("Invariant violated: CollectionBundle contains no collection.");
            let mut datums = DatumVec::new();
            let oks = oks.inner.flat_map(move |(v, t, d)| {
                logic(&mut datums.borrow_with_limit(&v, max_demand), t, d)
            });
            (oks, errs)
        }
    }

    /// Factored out common logic for using literal keys in general traces.
    ///
    /// This logic is sufficiently interesting that we want to write it only
    /// once, and thereby avoid any skew in the two uses of the logic.
    ///
    /// The function presents the contents of the trace as `(key, value, time, delta)` tuples,
    /// where key and value are potentially specialized, but convertible into rows.
    fn flat_map_core<Tr, D, I, L>(
        trace: &Arranged<S, Tr>,
        key: Option<&Tr::KeyOwn>,
        mut logic: L,
        refuel: usize,
    ) -> Stream<S, I::Item>
    where
        Tr: for<'a> TraceReader<
                Key<'a>: ToDatumIter,
                KeyOwn: PartialEq,
                Val<'a>: ToDatumIter,
                Time = S::Timestamp,
                Diff = mz_repr::Diff,
            > + Clone
            + 'static,
        I: IntoIterator<Item = (D, Tr::Time, Tr::Diff)>,
        D: Data,
        L: FnMut(Tr::Key<'_>, Tr::Val<'_>, S::Timestamp, mz_repr::Diff) -> I + 'static,
    {
        use differential_dataflow::consolidation::ConsolidatingContainerBuilder as CB;

        let mut key_con = Tr::KeyContainer::with_capacity(1);
        if let Some(key) = &key {
            key_con.push_own(key);
        }
        let mode = if key.is_some() { "index" } else { "scan" };
        let name = format!("ArrangementFlatMap({})", mode);
        use timely::dataflow::operators::Operator;
        trace
            .stream
            .unary::<CB<_>, _, _, _>(Pipeline, &name, move |_, info| {
                // Acquire an activator to reschedule the operator when it has unfinished work.
                let activator = trace.stream.scope().activator_for(info.address);
                // Maintain a list of work to do, cursor to navigate and process.
                let mut todo = std::collections::VecDeque::new();
                move |input, output| {
                    let key = key_con.get(0);
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
                        todo.front_mut().unwrap().do_work(
                            key.as_ref(),
                            &mut logic,
                            &mut fuel,
                            output,
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
            })
    }

    /// Look up an arrangement by the expressions that form the key.
    ///
    /// The result may be `None` if no such arrangement exists, or it may be one of many
    /// "arrangement flavors" that represent the types of arranged data we might have.
    pub fn arrangement(&self, key: &[MirScalarExpr]) -> Option<ArrangementFlavor<S, T>> {
        self.arranged.get(key).map(|x| x.clone())
    }
}

impl<S, T> CollectionBundle<S, T>
where
    T: MzTimestamp,
    S: Scope,
    S::Timestamp: Refines<T> + RenderTimestamp,
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
        until: Antichain<mz_repr::Timestamp>,
        config_set: &ConfigSet,
    ) -> (
        Collection<S, mz_repr::Row, Diff>,
        Collection<S, DataflowError, Diff>,
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

        let max_demand = mfp.demand().iter().max().map(|x| *x + 1).unwrap_or(0);
        mfp.permute_fn(|c| c, max_demand);
        mfp.optimize();
        let mfp_plan = mfp.into_plan().unwrap();

        let mut datum_vec = DatumVec::new();
        // Wrap in an `Rc` so that lifetimes work out.
        let until = std::rc::Rc::new(until);

        let (stream, errors) = self.flat_map(key_val, max_demand, move |row_datums, time, diff| {
            let mut row_builder = SharedRow::get();
            let until = std::rc::Rc::clone(&until);
            let temp_storage = RowArena::new();
            let row_iter = row_datums.iter();
            let mut datums_local = datum_vec.borrow();
            datums_local.extend(row_iter);
            let time = time.clone();
            let event_time = time.event_time();
            mfp_plan
                .evaluate(
                    &mut datums_local,
                    &temp_storage,
                    event_time,
                    diff.clone(),
                    move |time| !until.less_equal(time),
                    &mut row_builder,
                )
                .map(move |x| match x {
                    Ok((row, event_time, diff)) => {
                        // Copy the whole time, and re-populate event time.
                        let mut time: S::Timestamp = time.clone();
                        *time.event_time_mut() = event_time;
                        (Ok(row), time, diff)
                    }
                    Err((e, event_time, diff)) => {
                        // Copy the whole time, and re-populate event time.
                        let mut time: S::Timestamp = time.clone();
                        *time.event_time_mut() = event_time;
                        (Err(e), time, diff)
                    }
                })
        });

        use differential_dataflow::AsCollection;
        let (oks, errs) = stream
            .as_collection()
            .map_fallible::<CapacityContainerBuilder<_>, CapacityContainerBuilder<_>, _, _, _>(
                "OkErr",
                |x| x,
            );

        (oks, errors.concat(&errs))
    }
    pub fn ensure_collections(
        mut self,
        collections: AvailableCollections,
        input_key: Option<Vec<MirScalarExpr>>,
        input_mfp: MapFilterProject,
        until: Antichain<mz_repr::Timestamp>,
        config_set: &ConfigSet,
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

        for (key, _, _) in collections.arranged.iter() {
            soft_assert_or_log!(
                !self.arranged.contains_key(key),
                "LIR ArrangeBy tried to create an existing arrangement"
            );
        }

        // We need the collection if either (1) it is explicitly demanded, or (2) we are going to render any arrangement
        let form_raw_collection = collections.raw
            || collections
                .arranged
                .iter()
                .any(|(key, _, _)| !self.arranged.contains_key(key));
        if form_raw_collection && self.collection.is_none() {
            self.collection = Some(self.as_collection_core(
                input_mfp,
                input_key.map(|k| (k, None)),
                until,
                config_set,
            ));
        }
        for (key, _, thinning) in collections.arranged {
            if !self.arranged.contains_key(&key) {
                // TODO: Consider allowing more expressive names.
                let name = format!("ArrangeBy[{:?}]", key);

                let (oks, errs) = self
                    .collection
                    .clone()
                    .expect("Collection constructed above");
                let (oks, errs_keyed) =
                    Self::arrange_collection(&name, oks, key.clone(), thinning.clone());
                let errs: KeyCollection<_, _, _> = errs.concat(&errs_keyed).into();
                let errs = errs.mz_arrange::<ErrBatcher<_, _>, ErrBuilder<_, _>, ErrSpine<_, _>>(
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
    fn arrange_collection(
        name: &String,
        oks: Collection<S, Row, Diff>,
        key: Vec<MirScalarExpr>,
        thinning: Vec<usize>,
    ) -> (
        Arranged<S, RowRowAgent<S::Timestamp, Diff>>,
        Collection<S, DataflowError, Diff>,
    ) {
        // The following `unary_fallible` implements a `map_fallible`, but produces columnar updates
        // for the ok stream. The `map_fallible` cannot be used here because the closure cannot
        // return references, which is what we need to push into columnar streams. Instead, we use
        // a bespoke operator that also optimizes reuse of allocations across individual updates.
        let (oks, errs) = oks
            .inner
            .unary_fallible::<ColumnBuilder<((Row, Row), S::Timestamp, Diff)>, _, _, _>(
                Pipeline,
                "FormArrangementKey",
                move |_, _| {
                    Box::new(move |input, ok, err| {
                        let mut key_buf = Row::default();
                        let mut val_buf = Row::default();
                        let mut datums = DatumVec::new();
                        let mut temp_storage = RowArena::new();
                        while let Some((time, data)) = input.next() {
                            let mut ok_session = ok.session_with_builder(&time);
                            let mut err_session = err.session(&time);
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
                        }
                    })
                },
            );
        let oks = oks
            .mz_arrange_core::<_, Col2ValBatcher<_, _,_, _>, RowRowBuilder<_, _>, RowRowSpine<_, _>>(
                ExchangeCore::<ColumnBuilder<_>, _>::new_core(columnar_exchange::<Row, Row, S::Timestamp, Diff>),name
            );
        (oks, errs.as_collection())
    }
}

struct PendingWork<C>
where
    C: Cursor,
{
    capability: Capability<C::Time>,
    cursor: C,
    batch: C::Storage,
}

impl<C> PendingWork<C>
where
    C: Cursor<KeyOwn: PartialEq + Sized>,
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
    fn do_work<I, D, L>(
        &mut self,
        key: Option<&C::Key<'_>>,
        logic: &mut L,
        fuel: &mut usize,
        output: &mut OutputHandleCore<
            '_,
            C::Time,
            ConsolidatingContainerBuilder<Vec<I::Item>>,
            timely::dataflow::channels::pushers::Tee<C::Time, Vec<I::Item>>,
        >,
    ) where
        I: IntoIterator<Item = (D, C::Time, C::Diff)>,
        D: Data,
        L: FnMut(C::Key<'_>, C::Val<'_>, C::Time, C::Diff) -> I + 'static,
    {
        use differential_dataflow::consolidation::consolidate;

        // Attempt to make progress on this batch.
        let mut work: usize = 0;
        let mut session = output.session_with_builder(&self.capability);
        let mut buffer = Vec::new();
        if let Some(key) = key {
            let key = C::KeyContainer::reborrow(*key);
            if self.cursor.get_key(&self.batch).map(|k| k == key) != Some(true) {
                self.cursor.seek_key(&self.batch, key);
            }
            if self.cursor.get_key(&self.batch).map(|k| k == key) == Some(true) {
                let key = self.cursor.key(&self.batch);
                while let Some(val) = self.cursor.get_val(&self.batch) {
                    self.cursor.map_times(&self.batch, |time, diff| {
                        buffer.push((C::owned_time(time), C::owned_diff(diff)));
                    });
                    consolidate(&mut buffer);
                    for (time, diff) in buffer.drain(..) {
                        for datum in logic(key, val, time, diff) {
                            session.give(datum);
                            work += 1;
                        }
                    }
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
                        buffer.push((C::owned_time(time), C::owned_diff(diff)));
                    });
                    consolidate(&mut buffer);
                    for (time, diff) in buffer.drain(..) {
                        for datum in logic(key, val, time, diff) {
                            session.give(datum);
                            work += 1;
                        }
                    }
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
