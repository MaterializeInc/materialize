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
use std::rc::Weak;
use std::sync::mpsc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use differential_dataflow::{Collection, Data};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::AvailableCollections;
use mz_expr::{Id, MapFilterProject, MirScalarExpr};
use mz_repr::fixed_length::{FromRowByTypes, IntoRowByTypes};
use mz_repr::{ColumnType, DatumVec, DatumVecBorrow, Diff, GlobalId, Row, RowArena, SharedRow};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_timely_util::operator::CollectionExt;
use timely::container::columnation::Columnation;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::Capability;
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, ScopeParent};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};
use tracing::error;

use crate::arrangement::manager::SpecializedTraceHandle;
use crate::compute_state::{ComputeState, HydrationEvent};
use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::render::errors::ErrorLogger;
use crate::render::join::LinearJoinSpec;
use crate::render::RenderTimestamp;
use crate::typedefs::{
    ErrAgent, ErrEnter, ErrSpine, RowAgent, RowEnter, RowRowAgent, RowRowEnter, RowRowSpine,
    RowSpine,
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
    T: Timestamp + Lattice + Columnation,
    S::Timestamp: Lattice + Refines<T> + Columnation,
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
    /// A token that operators can probe to know whether the dataflow is shutting down.
    pub(super) shutdown_token: ShutdownToken,
    /// A logger that operators can use to report hydration events.
    ///
    /// `None` if no hydration events should be logged in this context.
    pub(super) hydration_logger: Option<HydrationLogger>,
    /// Specification for rendering linear joins.
    pub(super) linear_join_spec: LinearJoinSpec,
    /// Whether to log operator hydration status.
    pub(super) enable_operator_hydration_status_logging: bool,
}

impl<S: Scope> Context<S>
where
    S::Timestamp: Lattice + Refines<mz_repr::Timestamp> + Columnation,
{
    /// Creates a new empty Context.
    pub fn for_dataflow_in<Plan>(
        dataflow: &DataflowDescription<Plan, CollectionMetadata>,
        scope: S,
        compute_state: &ComputeState,
    ) -> Self {
        use mz_ore::collections::CollectionExt as IteratorExt;
        let dataflow_id = scope.addr().into_first();
        let as_of_frontier = dataflow
            .as_of
            .clone()
            .unwrap_or_else(|| Antichain::from_elem(Timestamp::minimum()));

        // Skip operator hydration logging for transient dataflows. We do this to avoid overhead
        // for slow-path peeks, but it also affects subscribes. For now that seems fine, but we may
        // want to reconsider in the future.
        let hydration_logger = if dataflow.is_transient() {
            None
        } else {
            Some(HydrationLogger {
                export_ids: dataflow.export_ids().collect(),
                tx: compute_state.hydration_tx.clone(),
            })
        };

        Self {
            scope,
            debug_name: dataflow.debug_name.clone(),
            dataflow_id,
            as_of_frontier,
            until: dataflow.until.clone(),
            bindings: BTreeMap::new(),
            shutdown_token: Default::default(),
            hydration_logger,
            linear_join_spec: compute_state.linear_join_spec,
            enable_operator_hydration_status_logging: compute_state
                .enable_operator_hydration_status_logging,
        }
    }
}

impl<S: Scope, T> Context<S, T>
where
    T: Timestamp + Lattice + Columnation,
    S::Timestamp: Lattice + Refines<T> + Columnation,
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
        ErrorLogger::new(self.shutdown_token.clone(), self.debug_name.clone())
    }
}

/// Convenient wrapper around an optional `Weak` instance that can be used to check whether a
/// datalow is shutting down.
///
/// Instances created through the `Default` impl act as if the dataflow never shuts down.
/// Instances created through [`ShutdownToken::new`] defer to the wrapped token.
#[derive(Clone, Default)]
pub(super) struct ShutdownToken(Option<Weak<()>>);

impl ShutdownToken {
    /// Construct a `ShutdownToken` instance that defers to `token`.
    pub(super) fn new(token: Weak<()>) -> Self {
        Self(Some(token))
    }

    /// Probe the token for dataflow shutdown.
    ///
    /// This method is meant to be used with the `?` operator: It returns `None` if the dataflow is
    /// in the process of shutting down and `Some` otherwise.
    pub(super) fn probe(&self) -> Option<()> {
        match &self.0 {
            Some(t) => t.upgrade().map(|_| ()),
            None => Some(()),
        }
    }

    /// Returns whether the dataflow is in the process of shutting down.
    pub(super) fn in_shutdown(&self) -> bool {
        self.probe().is_none()
    }

    /// Returns a reference to the wrapped `Weak`.
    pub(crate) fn get_inner(&self) -> Option<&Weak<()>> {
        self.0.as_ref()
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
    pub fn log(&self, lir_id: u64, hydrated: bool) {
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

/// A representation of arrangements that are statically type-specialized.
/// Each variant of this `enum` covers a different supported specialization of
/// key and value types for arrangement flavors.
///
/// The specialization here is performed on the representation length, as opposed
/// to its constituent types. For fixed-length specializations, it thus becomes
/// necessary to keep track of the schema used, since datums are not used and thus
/// the representation is not tagged. A catch-all `RowRow` specialization without
/// schema information allows for covering the current approach of self-describing
/// variable-length keys and variable-length values.
#[derive(Clone)]
pub enum SpecializedArrangement<S: Scope>
where
    <S as ScopeParent>::Timestamp: Lattice + Columnation,
{
    RowUnit(Arranged<S, RowAgent<<S as ScopeParent>::Timestamp, Diff>>),
    RowRow(Arranged<S, RowRowAgent<<S as ScopeParent>::Timestamp, Diff>>),
}

impl<S: Scope> SpecializedArrangement<S>
where
    <S as ScopeParent>::Timestamp: Lattice + Columnation,
{
    /// The scope of the underlying arrangement's stream.
    pub fn scope(&self) -> S {
        match self {
            SpecializedArrangement::RowUnit(inner) => inner.stream.scope(),
            SpecializedArrangement::RowRow(inner) => inner.stream.scope(),
        }
    }

    /// Brings the underlying arrangement into a region.
    pub fn enter_region<'a>(
        &self,
        region: &Child<'a, S, S::Timestamp>,
    ) -> SpecializedArrangement<Child<'a, S, S::Timestamp>> {
        match self {
            SpecializedArrangement::RowUnit(inner) => {
                SpecializedArrangement::RowUnit(inner.enter_region(region))
            }
            SpecializedArrangement::RowRow(inner) => {
                SpecializedArrangement::RowRow(inner.enter_region(region))
            }
        }
    }

    /// Extracts the underlying arrangement as a stream of updates.
    pub fn as_collection<L>(&self, mut logic: L) -> Collection<S, Row, Diff>
    where
        L: for<'a, 'b> FnMut(&'a DatumVecBorrow<'b>) -> Row + 'static,
    {
        let mut datums = DatumVec::new();
        match self {
            SpecializedArrangement::RowUnit(inner) => inner.as_collection(move |k, v| {
                let mut datums_borrow = datums.borrow();
                datums_borrow.extend(k.into_datum_iter(None));
                datums_borrow.extend(v.into_datum_iter(Some(&[])));
                logic(&datums_borrow)
            }),
            SpecializedArrangement::RowRow(inner) => inner.as_collection(move |k, v| {
                let mut datums_borrow = datums.borrow();
                datums_borrow.extend(k.into_datum_iter(None));
                datums_borrow.extend(v.into_datum_iter(None));
                logic(&datums_borrow)
            }),
        }
    }

    /// Applies logic to elements of the underlying arrangement and returns the results.
    pub fn flat_map<I, L, T>(
        &self,
        key: Option<Row>,
        mut logic: L,
        refuel: usize,
    ) -> timely::dataflow::Stream<S, I::Item>
    where
        T: Timestamp + Lattice + Columnation,
        <S as ScopeParent>::Timestamp: Lattice + Refines<T>,
        I: IntoIterator,
        I::Item: Data,
        L: for<'a, 'b> FnMut(&'a mut DatumVecBorrow<'b>, &'a S::Timestamp, &'a Diff) -> I + 'static,
    {
        use differential_dataflow::operators::arrange::TraceAgent;
        let mut datums = DatumVec::new();
        match self {
            SpecializedArrangement::RowUnit(inner) => {
                CollectionBundle::<S, T>::flat_map_core::<TraceAgent<RowSpine<_, _>>, _, _>(
                    inner,
                    key,
                    move |k, v, t, d| {
                        let mut datums_borrow = datums.borrow();
                        datums_borrow.extend(k.into_datum_iter(None));
                        datums_borrow.extend(v.into_datum_iter(Some(&[])));
                        logic(&mut datums_borrow, t, d)
                    },
                    refuel,
                )
            }
            SpecializedArrangement::RowRow(inner) => {
                CollectionBundle::<S, T>::flat_map_core::<TraceAgent<RowRowSpine<_, _>>, _, _>(
                    inner,
                    key,
                    move |k, v, t, d| {
                        let mut datums_borrow = datums.borrow();
                        datums_borrow.extend(k.into_datum_iter(None));
                        datums_borrow.extend(v.into_datum_iter(None));
                        logic(&mut datums_borrow, t, d)
                    },
                    refuel,
                )
            }
        }
    }
}

impl<'a, S: Scope> SpecializedArrangement<Child<'a, S, S::Timestamp>>
where
    <S as ScopeParent>::Timestamp: Lattice + Columnation,
{
    /// Extracts the underlying arrangement flavor from a region.
    pub fn leave_region(&self) -> SpecializedArrangement<S> {
        match self {
            SpecializedArrangement::RowUnit(inner) => {
                SpecializedArrangement::RowUnit(inner.leave_region())
            }
            SpecializedArrangement::RowRow(inner) => {
                SpecializedArrangement::RowRow(inner.leave_region())
            }
        }
    }
}

impl<S: Scope> SpecializedArrangement<S>
where
    S: ScopeParent<Timestamp = mz_repr::Timestamp>,
{
    /// Obtains a `SpecializedTraceHandle` for the underlying arrangement.
    pub fn trace_handle(&self) -> SpecializedTraceHandle {
        match self {
            SpecializedArrangement::RowUnit(inner) => {
                SpecializedTraceHandle::RowUnit(inner.trace.clone())
            }
            SpecializedArrangement::RowRow(inner) => {
                SpecializedTraceHandle::RowRow(inner.trace.clone())
            }
        }
    }
}

/// Defines a statically type-specialized representation of arrangement imports,
/// similarly to `SpecializedArrangement`.
#[derive(Clone)]
pub enum SpecializedArrangementImport<S: Scope, T = mz_repr::Timestamp>
where
    T: Timestamp + Lattice + Columnation,
    <S as ScopeParent>::Timestamp: Lattice + Refines<T>,
{
    RowUnit(Arranged<S, RowEnter<T, Diff, <S as ScopeParent>::Timestamp>>),
    RowRow(Arranged<S, RowRowEnter<T, Diff, <S as ScopeParent>::Timestamp>>),
}

impl<S: Scope, T> SpecializedArrangementImport<S, T>
where
    T: Timestamp + Lattice + Columnation,
    <S as ScopeParent>::Timestamp: Lattice + Refines<T> + Columnation,
{
    /// The scope of the underlying trace's stream.
    pub fn scope(&self) -> S {
        match self {
            SpecializedArrangementImport::RowUnit(inner) => inner.stream.scope(),
            SpecializedArrangementImport::RowRow(inner) => inner.stream.scope(),
        }
    }

    /// Brings the underlying trace into a region.
    pub fn enter_region<'a>(
        &self,
        region: &Child<'a, S, S::Timestamp>,
    ) -> SpecializedArrangementImport<Child<'a, S, S::Timestamp>, T> {
        match self {
            SpecializedArrangementImport::RowUnit(inner) => {
                SpecializedArrangementImport::RowUnit(inner.enter_region(region))
            }
            SpecializedArrangementImport::RowRow(inner) => {
                SpecializedArrangementImport::RowRow(inner.enter_region(region))
            }
        }
    }

    /// Extracts the underlying trace as a stream of updates.
    pub fn as_collection<L>(&self, mut logic: L) -> Collection<S, Row, Diff>
    where
        L: for<'a, 'b> FnMut(&'a DatumVecBorrow<'b>) -> Row + 'static,
    {
        let mut datums = DatumVec::new();
        match self {
            SpecializedArrangementImport::RowUnit(inner) => inner.as_collection(move |k, v| {
                let mut datums_borrow = datums.borrow();
                datums_borrow.extend(k.into_datum_iter(None));
                datums_borrow.extend(v.into_datum_iter(Some(&[])));
                logic(&datums_borrow)
            }),
            SpecializedArrangementImport::RowRow(inner) => inner.as_collection(move |k, v| {
                let mut datums_borrow = datums.borrow();
                datums_borrow.extend(k.into_datum_iter(None));
                datums_borrow.extend(v.into_datum_iter(None));
                logic(&datums_borrow)
            }),
        }
    }

    /// Applies logic to elements of the underlying arrangement and returns the results.
    pub fn flat_map<I, L>(
        &self,
        key: Option<Row>,
        mut logic: L,
        refuel: usize,
    ) -> timely::dataflow::Stream<S, I::Item>
    where
        I: IntoIterator,
        I::Item: Data,
        L: for<'a, 'b> FnMut(&'a mut DatumVecBorrow<'b>, &'a S::Timestamp, &'a Diff) -> I + 'static,
    {
        let mut datums = DatumVec::new();
        match self {
            SpecializedArrangementImport::RowUnit(inner) => {
                CollectionBundle::<S, T>::flat_map_core::<RowEnter<T, Diff, S::Timestamp>, _, _>(
                    inner,
                    key,
                    move |k, v, t, d| {
                        let mut datums_borrow = datums.borrow();
                        datums_borrow.extend(k.into_datum_iter(None));
                        datums_borrow.extend(v.into_datum_iter(Some(&[])));
                        logic(&mut datums_borrow, t, d)
                    },
                    refuel,
                )
            }
            SpecializedArrangementImport::RowRow(inner) => {
                CollectionBundle::<S, T>::flat_map_core::<RowRowEnter<T, Diff, S::Timestamp>, _, _>(
                    inner,
                    key,
                    move |k, v, t, d| {
                        let mut datums_borrow = datums.borrow();
                        datums_borrow.extend(k.into_datum_iter(None));
                        datums_borrow.extend(v.into_datum_iter(None));
                        logic(&mut datums_borrow, t, d)
                    },
                    refuel,
                )
            }
        }
    }
}

impl<'a, S: Scope, T> SpecializedArrangementImport<Child<'a, S, S::Timestamp>, T>
where
    T: Timestamp + Lattice + Columnation,
    <S as ScopeParent>::Timestamp: Lattice + Refines<T>,
{
    /// Extracts the underlying arrangement flavor from a region.
    pub fn leave_region(&self) -> SpecializedArrangementImport<S, T> {
        match self {
            SpecializedArrangementImport::RowUnit(inner) => {
                SpecializedArrangementImport::RowUnit(inner.leave_region())
            }
            SpecializedArrangementImport::RowRow(inner) => {
                SpecializedArrangementImport::RowRow(inner.leave_region())
            }
        }
    }
}

/// Describes flavor of arrangement: local or imported trace.
#[derive(Clone)]
pub enum ArrangementFlavor<S: Scope, T = mz_repr::Timestamp>
where
    T: Timestamp + Lattice + Columnation,
    S::Timestamp: Lattice + Refines<T> + Columnation,
{
    /// A dataflow-local arrangement.
    Local(
        SpecializedArrangement<S>,
        Arranged<S, ErrAgent<<S as ScopeParent>::Timestamp, Diff>>,
    ),
    /// An imported trace from outside the dataflow.
    ///
    /// The `GlobalId` identifier exists so that exports of this same trace
    /// can refer back to and depend on the original instance.
    Trace(
        GlobalId,
        SpecializedArrangementImport<S, T>,
        Arranged<S, ErrEnter<T, <S as ScopeParent>::Timestamp>>,
    ),
}

impl<S: Scope, T> ArrangementFlavor<S, T>
where
    T: Timestamp + Lattice + Columnation,
    S::Timestamp: Lattice + Refines<T> + Columnation,
{
    /// Presents `self` as a stream of updates.
    ///
    /// This method presents the contents as they are, without further computation.
    /// If you have logic that could be applied to each record, consider using the
    /// `flat_map` methods which allows this and can reduce the work done.
    pub fn as_collection(&self) -> (Collection<S, Row, Diff>, Collection<S, DataflowError, Diff>) {
        match &self {
            ArrangementFlavor::Local(oks, errs) => (
                oks.as_collection(move |borrow| SharedRow::pack(&**borrow)),
                errs.as_collection(|k, &()| k.clone()),
            ),
            ArrangementFlavor::Trace(_, oks, errs) => (
                oks.as_collection(move |borrow| SharedRow::pack(&**borrow)),
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
        L: for<'a, 'b> FnMut(&'a mut DatumVecBorrow<'b>, &'a S::Timestamp, &'a Diff) -> I + 'static,
    {
        // Set a number of tuples after which the operator should yield.
        // This allows us to remain responsive even when enumerating a substantial
        // arrangement, as well as provides time to accumulate our produced output.
        let refuel = 1000000;

        match &self {
            ArrangementFlavor::Local(oks, errs) => {
                let logic = constructor();
                let oks = oks.flat_map(key, logic, refuel);
                let errs = errs.as_collection(|k, &()| k.clone());
                (oks, errs)
            }
            ArrangementFlavor::Trace(_, oks, errs) => {
                let logic = constructor();
                let oks = oks.flat_map(key, logic, refuel);
                let errs = errs.as_collection(|k, &()| k.clone());
                (oks, errs)
            }
        }
    }
}
impl<S: Scope, T> ArrangementFlavor<S, T>
where
    T: Timestamp + Lattice + Columnation,
    S::Timestamp: Lattice + Refines<T> + Columnation,
{
    /// The scope containing the collection bundle.
    pub fn scope(&self) -> S {
        match self {
            ArrangementFlavor::Local(oks, _errs) => oks.scope(),
            ArrangementFlavor::Trace(_gid, oks, _errs) => oks.scope(),
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
    T: Timestamp + Lattice + Columnation,
    S::Timestamp: Lattice + Refines<T> + Columnation,
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
    T: Timestamp + Lattice + Columnation,
    S::Timestamp: Lattice + Refines<T> + Columnation,
{
    pub collection: Option<(Collection<S, Row, Diff>, Collection<S, DataflowError, Diff>)>,
    pub arranged: BTreeMap<Vec<MirScalarExpr>, ArrangementFlavor<S, T>>,
}

impl<S: Scope, T: Lattice> CollectionBundle<S, T>
where
    T: Timestamp + Lattice + Columnation,
    S::Timestamp: Lattice + Refines<T> + Columnation,
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
            keys.push(MirScalarExpr::Column(column));
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
    T: Timestamp + Lattice + Columnation,
    S::Timestamp: Lattice + Refines<T> + Columnation,
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
    T: Timestamp + Lattice + Columnation,
    S::Timestamp: Lattice + Refines<T> + Columnation,
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
        L: for<'a, 'b> FnMut(&'a mut DatumVecBorrow<'b>, &'a S::Timestamp, &'a Diff) -> I + 'static,
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
            let mut datums = DatumVec::new();
            (
                oks.inner
                    .flat_map(move |(v, t, d)| logic(&mut datums.borrow_with(&v), &t, &d)),
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
    /// where key and value are potentially specialized, but convertible into rows.
    fn flat_map_core<Tr, I, L>(
        trace: &Arranged<S, Tr>,
        key: Option<Tr::KeyOwned>,
        mut logic: L,
        refuel: usize,
    ) -> timely::dataflow::Stream<S, I::Item>
    where
        for<'a> Tr::Key<'a>: IntoRowByTypes,
        for<'a> Tr::Val<'a>: IntoRowByTypes,
        Tr: TraceReader<Time = S::Timestamp, Diff = mz_repr::Diff> + Clone + 'static,
        I: IntoIterator,
        I::Item: Data,
        L: for<'a, 'b> FnMut(Tr::Key<'_>, Tr::Val<'_>, &'a S::Timestamp, &'a mz_repr::Diff) -> I
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
    pub fn arrangement(&self, key: &[MirScalarExpr]) -> Option<ArrangementFlavor<S, T>> {
        self.arranged.get(key).map(|x| x.clone())
    }
}

impl<S, T> CollectionBundle<S, T>
where
    T: timely::progress::Timestamp + Lattice + Columnation,
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
        until: Antichain<mz_repr::Timestamp>,
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
            let mut datum_vec = DatumVec::new();
            // Wrap in an `Rc` so that lifetimes work out.
            let until = std::rc::Rc::new(until);
            move |row_datums, time, diff| {
                let binding = SharedRow::get();
                let mut row_builder = binding.borrow_mut();
                let until = std::rc::Rc::clone(&until);
                let temp_storage = RowArena::new();
                let row_iter = row_datums.iter();
                let mut datums_local = datum_vec.borrow();
                datums_local.extend(row_iter);
                let time = time.clone();
                let event_time: mz_repr::Timestamp = *time.clone().event_time();
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
        until: Antichain<mz_repr::Timestamp>,
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
                Some(self.as_collection_core(input_mfp, input_key.map(|k| (k, None)), until));
        }
        for (key, _, thinning) in collections.arranged {
            if !self.arranged.contains_key(&key) {
                // TODO: Consider allowing more expressive names.
                let name = format!("ArrangeBy[{:?}]", key);

                let key_val_types =
                    derive_key_val_types(&key, &thinning, collections.types.as_ref());

                let (oks, errs) = self
                    .collection
                    .clone()
                    .expect("Collection constructed above");
                let (oks, errs_keyed) =
                    Self::specialized_arrange(&name, oks, &key, &thinning, key_val_types);
                let errs: KeyCollection<_, _, _> = errs.concat(&errs_keyed).into();
                let errs = errs.mz_arrange::<ErrSpine<_, _>>(&format!("{}-errors", name));
                self.arranged
                    .insert(key, ArrangementFlavor::Local(oks, errs));
            }
        }
        self
    }

    /// Builds a specialized arrangement to provided types. The specialization for key and
    /// value types of the arrangement is based on the bit length derived from the corresponding
    /// type descriptions.
    fn specialized_arrange(
        name: &String,
        oks: Collection<S, Row, i64>,
        key: &Vec<MirScalarExpr>,
        thinning: &Vec<usize>,
        key_val_types: Option<(Vec<ColumnType>, Vec<ColumnType>)>,
    ) -> (SpecializedArrangement<S>, Collection<S, DataflowError, i64>) {
        if let Some((_key_types, val_types)) = key_val_types {
            if val_types.is_empty() {
                // Emtpy value specialization.
                let (oks, errs) = oks.map_fallible(
                    "FormArrangementKey [val: empty]",
                    specialized_arrangement_key(key.clone(), thinning.clone(), None, Some(vec![])),
                );
                let name = &format!("{} [val: empty]", name);
                let oks = oks.mz_arrange::<RowSpine<_, _>>(name);
                return (SpecializedArrangement::RowUnit(oks), errs);
            }
        } else {
            // We expect to always have type information for arrangements, do getting here is
            // likely a bug.
            tracing::error!(%name, "missing types for arrangement");
        }

        // Catch-all: Just use RowRow.
        let (oks, errs) = oks.map_fallible(
            "FormArrangementKey",
            specialized_arrangement_key(key.clone(), thinning.clone(), None, None),
        );
        let oks = oks.mz_arrange::<RowRowSpine<_, _>>(name);
        (SpecializedArrangement::RowRow(oks), errs)
    }
}

/// Derives the column types of the key and values of an arrangement based on its column
/// permutation, value thinning, and column types describing the full row schema.
fn derive_key_val_types(
    key: &Vec<MirScalarExpr>,
    thinning: &Vec<usize>,
    types: Option<&Vec<ColumnType>>,
) -> Option<(Vec<ColumnType>, Vec<ColumnType>)> {
    if let Some(types) = types {
        let mut key_types = Vec::new();
        let mut val_types = Vec::new();

        for i in 0..key.len() {
            key_types.push(key[i].typ(types).clone());
        }
        for c in thinning.iter() {
            val_types.push(types[*c].clone());
        }
        Some((key_types, val_types))
    } else {
        None
    }
}

/// Obtains a function that maps input rows to (key, value) pairs according to
/// the given key and thinning expressions. This function allows for specialization
/// of key and value types and is intended to use to form arrangement keys.
fn specialized_arrangement_key<K, V>(
    key: Vec<MirScalarExpr>,
    thinning: Vec<usize>,
    key_types: Option<Vec<ColumnType>>,
    val_types: Option<Vec<ColumnType>>,
) -> impl FnMut(Row) -> Result<(K, V), DataflowError>
where
    K: Columnation + Data + FromRowByTypes,
    V: Columnation + Data + FromRowByTypes,
{
    let mut key_buf = K::default();
    let mut val_buf = V::default();
    let mut datums = DatumVec::new();
    move |row| {
        // TODO: Consider reusing the `row` allocation; probably in *next* invocation.
        let datums = datums.borrow_with(&row);
        let temp_storage = RowArena::new();
        let val_datum_iter = thinning.iter().map(|c| datums[*c]);
        Ok::<(K, V), DataflowError>((
            key_buf.try_from_datum_iter(
                key.iter().map(|k| k.eval(&datums, &temp_storage)),
                key_types.as_deref(),
            )?,
            val_buf.from_datum_iter(val_datum_iter, val_types.as_deref()),
        ))
    }
}

struct PendingWork<C>
where
    C: Cursor,
    C::Time: Timestamp,
{
    capability: Capability<C::Time>,
    cursor: C,
    batch: C::Storage,
}

impl<C> PendingWork<C>
where
    C: Cursor,
    C::KeyOwned: PartialEq + Sized,
    C::ValOwned: Sized,
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
        key: &Option<C::KeyOwned>,
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
        L: for<'a, 'b> FnMut(C::Key<'_>, C::Val<'b>, &'a C::Time, &'a C::Diff) -> I + 'static,
    {
        // Attempt to make progress on this batch.
        let mut work: usize = 0;
        let mut session = output.session(&self.capability);
        if let Some(key) = key {
            use differential_dataflow::trace::cursor::MyTrait;
            if self.cursor.get_key(&self.batch).map(|k| k.equals(key)) != Some(true) {
                self.cursor.seek_key_owned(&self.batch, key);
            }
            if self.cursor.get_key(&self.batch).map(|k| k.equals(key)) == Some(true) {
                let key = self.cursor.key(&self.batch);
                while let Some(val) = self.cursor.get_val(&self.batch) {
                    self.cursor.map_times(&self.batch, |time, diff| {
                        for datum in logic(key, val, time, diff) {
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
                        for datum in logic(key, val, time, diff) {
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
