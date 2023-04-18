// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and methods for building and shipping dataflow descriptions.
//!
//! Dataflows are buildable from the coordinator's `catalog` and `indexes`
//! members, which respectively describe the collection backing identifiers
//! and indicate which identifiers have arrangements available. This module
//! isolates that logic from the rest of the somewhat complicated coordinator.

use std::collections::{BTreeMap, BTreeSet};

use differential_dataflow::lattice::Lattice;
use timely::progress::Antichain;
use timely::PartialOrder;
use tracing::warn;

use mz_compute_client::controller::{ComputeInstanceId, ComputeInstanceRef};
use mz_compute_client::types::dataflows::{
    BuildDesc, DataflowDesc, DataflowDescription, IndexDesc,
};
use mz_compute_client::types::sinks::{
    ComputeSinkConnection, ComputeSinkDesc, PersistSinkConnection,
};
use mz_expr::visit::Visit;
use mz_expr::{
    CollectionPlan, Id, MapFilterProject, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr,
    UnmaterializableFunc, RECURSION_LIMIT,
};
use mz_ore::cast::ReinterpretCast;
use mz_ore::stack::{maybe_grow, CheckedRecursion, RecursionGuard, RecursionLimitError};
use mz_repr::adt::array::ArrayDimension;
use mz_repr::{Datum, GlobalId, Row, Timestamp};
use mz_sql::catalog::SessionCatalog;

use crate::catalog::{
    Catalog, CatalogItem, CatalogState, DataSourceDesc, MaterializedView, Source, View,
};
use crate::coord::ddl::CatalogTxn;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::{Coordinator, DEFAULT_LOGICAL_COMPACTION_WINDOW_TS};
use crate::session::{Session, SERVER_MAJOR_VERSION, SERVER_MINOR_VERSION};
use crate::util::{viewable_variables, ResultExt};
use crate::{rbac, AdapterError};

/// Borrows of catalog and indexes sufficient to build dataflow descriptions.
pub struct DataflowBuilder<'a, T> {
    pub catalog: &'a CatalogState,
    /// A handle to the compute abstraction, which describes indexes by identifier.
    ///
    /// This can also be used to grab a handle to the storage abstraction, through
    /// its `storage_mut()` method.
    pub compute: ComputeInstanceRef<'a, T>,
    recursion_guard: RecursionGuard,
}

/// The styles in which an expression can be prepared for use in a dataflow.
#[derive(Clone, Copy, Debug)]
pub enum ExprPrepStyle<'a> {
    /// The expression is being prepared for installation as a maintained index.
    Index,
    /// The expression is being prepared to run once at the specified logical
    /// time in the specified session.
    OneShot {
        logical_time: Option<mz_repr::Timestamp>,
        session: &'a Session,
    },
    /// The expression is being prepared for evaluation in an AS OF or UP TO clause.
    AsOfUpTo,
}

impl Coordinator {
    /// Creates a new dataflow builder from the catalog and indexes in `self`.
    pub fn dataflow_builder(
        &self,
        instance: ComputeInstanceId,
    ) -> DataflowBuilder<mz_repr::Timestamp> {
        let compute = self
            .controller
            .compute
            .instance_ref(instance)
            .expect("compute instance does not exist");
        DataflowBuilder {
            catalog: self.catalog().state(),
            compute,
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }

    /// Finalizes a dataflow and then broadcasts it to all workers.
    /// Utility method for the more general [`Self::must_ship_dataflows`]
    ///
    /// # Panics
    ///
    /// Panics if the dataflow fails to ship.
    pub(crate) async fn must_ship_dataflow(
        &mut self,
        dataflow: DataflowDesc,
        instance: ComputeInstanceId,
    ) {
        self.must_ship_dataflows(vec![dataflow], instance).await
    }

    /// Finalizes a list of dataflows and then broadcasts it to all workers.
    ///
    /// # Panics
    ///
    /// Panics if any of the dataflows fail to ship.
    async fn must_ship_dataflows(
        &mut self,
        dataflows: Vec<DataflowDesc>,
        instance: ComputeInstanceId,
    ) {
        self.ship_dataflows(dataflows, instance)
            .await
            .expect("failed to ship dataflows");
    }

    /// Finalizes a dataflow and then broadcasts it to all workers.
    /// Utility method for the more general [`Self::ship_dataflows`]
    ///
    /// Returns an error on failure. DO NOT call this for DDL. Instead, use the non-fallible version
    /// [`Self::must_ship_dataflow`].
    pub(crate) async fn ship_dataflow(
        &mut self,
        dataflow: DataflowDesc,
        instance: ComputeInstanceId,
    ) -> Result<(), AdapterError> {
        self.ship_dataflows(vec![dataflow], instance).await
    }

    /// Finalizes a list of dataflows and then broadcasts it to all workers.
    ///
    /// Returns an error on failure. DO NOT call this for DDL. Instead, use the non-fallible version
    /// [`Self::must_ship_dataflows`].
    async fn ship_dataflows(
        &mut self,
        dataflows: Vec<DataflowDesc>,
        instance: ComputeInstanceId,
    ) -> Result<(), AdapterError> {
        let mut output_ids = Vec::new();
        let mut dataflow_plans = Vec::with_capacity(dataflows.len());
        for dataflow in dataflows.into_iter() {
            output_ids.extend(dataflow.export_ids());
            let mut plan = self.finalize_dataflow(dataflow, instance)?;
            // If the only outputs of the dataflow are sinks, we might
            // be able to turn off the computation early, if they all
            // have non-trivial `up_to`s.
            if plan.index_exports.is_empty() {
                plan.until = Antichain::from_elem(Timestamp::MIN);
                for (_, sink) in &plan.sink_exports {
                    plan.until.join_assign(&sink.up_to);
                }
            }
            dataflow_plans.push(plan);
        }
        self.controller
            .active_compute()
            .create_dataflows(instance, dataflow_plans)
            .unwrap_or_terminate("dataflow creation cannot fail");
        self.initialize_compute_read_policies(
            output_ids,
            instance,
            Some(DEFAULT_LOGICAL_COMPACTION_WINDOW_TS),
        )
        .await;

        Ok(())
    }

    /// Finalizes a dataflow.
    ///
    /// Finalization includes optimization, but also validation of various
    /// invariants such as ensuring that the `as_of` frontier is in advance of
    /// the various `since` frontiers of participating data inputs.
    ///
    /// In particular, there are requirement on the `as_of` field for the dataflow
    /// and the `since` frontiers of created arrangements, as a function of the `since`
    /// frontiers of dataflow inputs (sources and imported arrangements).
    ///
    /// # Panics
    ///
    /// Panics if as_of is < the `since` frontiers.
    ///
    /// Panics if the dataflow descriptions contain an invalid plan.
    pub(crate) fn must_finalize_dataflow(
        &self,
        dataflow: DataflowDesc,
        compute_instance: ComputeInstanceId,
    ) -> DataflowDescription<mz_compute_client::plan::Plan> {
        // This function must succeed because catalog_transact has generally been run
        // before calling this function. We don't have plumbing yet to rollback catalog
        // operations if this function fails, and environmentd will be in an unsafe
        // state if we do not correctly clean up the catalog.
        self.finalize_dataflow(dataflow, compute_instance)
            .expect("Dataflow planning failed; unrecoverable error")
    }

    /// Finalizes a dataflow.
    ///
    /// Finalization includes optimization, but also validation of various
    /// invariants such as ensuring that the `as_of` frontier is in advance of
    /// the various `since` frontiers of participating data inputs.
    ///
    /// In particular, there are requirement on the `as_of` field for the dataflow
    /// and the `since` frontiers of created arrangements, as a function of the `since`
    /// frontiers of dataflow inputs (sources and imported arrangements).
    ///
    /// This method will return an error if the finalization fails. DO NOT call this
    /// method for DDL. Instead, use the non-fallible version [`Self::must_finalize_dataflow`].
    ///
    /// # Panics
    ///
    /// Panics if as_of is < the `since` frontiers.
    pub(crate) fn finalize_dataflow(
        &self,
        mut dataflow: DataflowDesc,
        compute_instance: ComputeInstanceId,
    ) -> Result<DataflowDescription<mz_compute_client::plan::Plan>, AdapterError> {
        let storage_ids = dataflow
            .source_imports
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        let compute_ids = dataflow
            .index_imports
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();

        let compute_ids = vec![(compute_instance, compute_ids)].into_iter().collect();
        let since = self.least_valid_read(&CollectionIdBundle {
            storage_ids,
            compute_ids,
        });

        // Ensure that the dataflow's `as_of` is at least `since`.
        if let Some(as_of) = &mut dataflow.as_of {
            // It should not be possible to request an invalid time. SINK doesn't support
            // AS OF. Subscribe and Peek check that their AS OF is >= since.
            assert!(
                PartialOrder::less_equal(&since, as_of),
                "Dataflow {} requested as_of ({:?}) not >= since ({:?})",
                dataflow.debug_name,
                as_of,
                since
            );
        } else {
            // Bind the since frontier to the dataflow description.
            dataflow.set_as_of(since);
        }

        // Ensure all expressions are normalized before finalizing.
        for build in dataflow.objects_to_build.iter_mut() {
            mz_transform::normalize_lets::normalize_lets(&mut build.plan.0)
                .unwrap_or_terminate("Normalize failed; unrecoverable error");
        }

        mz_compute_client::plan::Plan::finalize_dataflow(dataflow).map_err(AdapterError::Internal)
    }
}

impl CatalogTxn<'_, mz_repr::Timestamp> {
    /// Creates a new dataflow builder from an ongoing catalog transaction.
    pub fn dataflow_builder(
        &self,
        instance: ComputeInstanceId,
    ) -> DataflowBuilder<mz_repr::Timestamp> {
        let compute = self
            .dataflow_client
            .compute
            .instance_ref(instance)
            .expect("compute instance does not exist");
        DataflowBuilder {
            catalog: self.catalog,
            compute,
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl<'a> DataflowBuilder<'a, mz_repr::Timestamp> {
    /// Imports the view, source, or table with `id` into the provided
    /// dataflow description.
    fn import_into_dataflow(
        &mut self,
        id: &GlobalId,
        dataflow: &mut DataflowDesc,
    ) -> Result<(), AdapterError> {
        maybe_grow(|| {
            // Avoid importing the item redundantly.
            if dataflow.is_imported(id) {
                return Ok(());
            }

            // A valid index is any index on `id` that is known to index oracle.
            //
            // TODO: indexes should be imported after the optimization process,
            // and only those actually used by the optimized plan
            //
            // NOTE(benesch): is the above TODO still true? The dataflow layer
            // has gotten increasingly smart about index selection. Maybe it's
            // now fine to present all indexes?
            let index_oracle = self.index_oracle();
            let mut valid_indexes = index_oracle.indexes_on(*id).peekable();
            if valid_indexes.peek().is_some() {
                // Deduplicate indexes by keys, in case we have redundant indexes.
                let mut valid_indexes = valid_indexes.collect::<Vec<_>>();
                valid_indexes.sort_by_key(|(_, idx)| &idx.keys);
                valid_indexes.dedup_by_key(|(_, idx)| &idx.keys);
                for (index_id, idx) in valid_indexes {
                    let index_desc = IndexDesc {
                        on_id: *id,
                        key: idx.keys.to_vec(),
                    };
                    let entry = self.catalog.get_entry(id);
                    let desc = entry
                        .desc(
                            &self
                                .catalog
                                .resolve_full_name(entry.name(), entry.conn_id()),
                        )
                        .expect("indexes can only be built on items with descs");
                    let monotonic = self.monotonic_view(*id);
                    dataflow.import_index(index_id, index_desc, desc.typ().clone(), monotonic);
                }
            } else {
                drop(valid_indexes);
                let entry = self.catalog.get_entry(id);
                match entry.item() {
                    CatalogItem::Table(table) => {
                        dataflow.import_source(*id, table.desc.typ().clone(), false);
                    }
                    CatalogItem::Source(source) => {
                        dataflow.import_source(
                            *id,
                            source.desc.typ().clone(),
                            self.monotonic_source(source),
                        );
                    }
                    CatalogItem::View(view) => {
                        let expr = view.optimized_expr.clone();
                        self.import_view_into_dataflow(id, &expr, dataflow)?;
                    }
                    CatalogItem::MaterializedView(mview) => {
                        let monotonic = self.monotonic_view(*id);
                        dataflow.import_source(*id, mview.desc.typ().clone(), monotonic);
                    }
                    CatalogItem::Log(log) => {
                        dataflow.import_source(*id, log.variant.desc().typ().clone(), false);
                    }
                    _ => unreachable!(),
                }
            }
            Ok(())
        })
    }

    /// Imports the view with the specified ID and expression into the provided
    /// dataflow description.
    ///
    /// You should generally prefer calling
    /// [`DataflowBuilder::import_into_dataflow`], which can handle objects of
    /// any type as long as they exist in the catalog. This method exists for
    /// when the view does not exist in the catalog, e.g., because it is
    /// identified by a [`GlobalId::Transient`].
    pub fn import_view_into_dataflow(
        &mut self,
        view_id: &GlobalId,
        view: &OptimizedMirRelationExpr,
        dataflow: &mut DataflowDesc,
    ) -> Result<(), AdapterError> {
        for get_id in view.depends_on() {
            self.import_into_dataflow(&get_id, dataflow)?;
        }
        dataflow.insert_plan(*view_id, view.clone());
        Ok(())
    }

    /// Builds a dataflow description for the index with the specified ID.
    pub fn build_index_dataflow(&mut self, id: GlobalId) -> Result<DataflowDesc, AdapterError> {
        let index_entry = self.catalog.get_entry(&id);
        let index = match index_entry.item() {
            CatalogItem::Index(index) => index,
            _ => unreachable!("cannot create index dataflow on non-index"),
        };
        let on_entry = self.catalog.get_entry(&index.on);
        let on_type = on_entry
            .desc(
                &self
                    .catalog
                    .resolve_full_name(on_entry.name(), on_entry.conn_id()),
            )
            .expect("can only create indexes on items with a valid description")
            .typ()
            .clone();
        let name = index_entry.name().to_string();
        let mut dataflow = DataflowDesc::new(name);
        self.import_into_dataflow(&index.on, &mut dataflow)?;
        for BuildDesc { plan, .. } in &mut dataflow.objects_to_build {
            prep_relation_expr(self.catalog, plan, ExprPrepStyle::Index)?;
        }
        let mut index_description = IndexDesc {
            on_id: index.on,
            key: index.keys.clone(),
        };
        for key in &mut index_description.key {
            prep_scalar_expr(self.catalog, key, ExprPrepStyle::Index)?;
        }
        dataflow.export_index(id, index_description, on_type);

        // Optimize the dataflow across views, and any other ways that appeal.
        mz_transform::optimize_dataflow(&mut dataflow, &self.index_oracle())?;

        Ok(dataflow)
    }

    /// Builds a dataflow description for the sink with the specified name,
    /// ID, source, and output connection.
    ///
    /// For as long as this dataflow is active, `id` can be used to reference
    /// the sink (primarily to drop it, at the moment).
    pub fn build_sink_dataflow(
        &mut self,
        name: String,
        id: GlobalId,
        sink_description: ComputeSinkDesc,
    ) -> Result<DataflowDesc, AdapterError> {
        let mut dataflow = DataflowDesc::new(name);
        self.build_sink_dataflow_into(&mut dataflow, id, sink_description)?;
        Ok(dataflow)
    }

    /// Like `build_sink_dataflow`, but builds the sink dataflow into the
    /// existing dataflow description instead of creating one from scratch.
    pub fn build_sink_dataflow_into(
        &mut self,
        dataflow: &mut DataflowDesc,
        id: GlobalId,
        sink_description: ComputeSinkDesc,
    ) -> Result<(), AdapterError> {
        self.import_into_dataflow(&sink_description.from, dataflow)?;
        for BuildDesc { plan, .. } in &mut dataflow.objects_to_build {
            prep_relation_expr(self.catalog, plan, ExprPrepStyle::Index)?;
        }
        dataflow.export_sink(id, sink_description);

        // Optimize the dataflow across views, and any other ways that appeal.
        mz_transform::optimize_dataflow(dataflow, &self.index_oracle())?;

        Ok(())
    }

    /// Builds a dataflow description for the materialized view specified by `id`.
    ///
    /// For this, we first build a dataflow for the view expression, then we
    /// add a sink that writes that dataflow's output to storage.
    /// `internal_view_id` is the ID we assign to the view dataflow internally,
    /// so we can connect the sink to it.
    pub fn build_materialized_view_dataflow(
        &mut self,
        id: GlobalId,
        internal_view_id: GlobalId,
    ) -> Result<DataflowDesc, AdapterError> {
        let mview_entry = self.catalog.get_entry(&id);
        let mview = match mview_entry.item() {
            CatalogItem::MaterializedView(mv) => mv,
            _ => unreachable!(
                "cannot create materialized view dataflow on something that is not a materialized view"
            ),
        };

        let name = mview_entry.name().to_string();
        let mut dataflow = DataflowDesc::new(name);

        self.import_view_into_dataflow(&internal_view_id, &mview.optimized_expr, &mut dataflow)?;
        for BuildDesc { plan, .. } in &mut dataflow.objects_to_build {
            prep_relation_expr(self.catalog, plan, ExprPrepStyle::Index)?;
        }

        let sink_description = ComputeSinkDesc {
            from: internal_view_id,
            from_desc: mview.desc.clone(),
            connection: ComputeSinkConnection::Persist(PersistSinkConnection {
                value_desc: mview.desc.clone(),
                storage_metadata: (),
            }),
            with_snapshot: true,
            up_to: Antichain::default(),
        };
        self.build_sink_dataflow_into(&mut dataflow, id, sink_description)?;

        Ok(dataflow)
    }

    /// Determine the given source's monotonicity.
    fn monotonic_source(&self, source: &Source) -> bool {
        // TODO(petrosagg): store an inverse mapping of subsource -> source in the catalog so that
        // we can retrieve monotonicity information from the parent source.
        match &source.data_source {
            DataSourceDesc::Ingestion(ingestion) => ingestion.desc.monotonic(),
            DataSourceDesc::Introspection(_)
            | DataSourceDesc::Progress
            | DataSourceDesc::Source => false,
        }
    }

    /// Determine the given view's monotonicity.
    ///
    /// This recursively traverses the expressions of all (materialized) views involved in the
    /// given view's query expression. If this becomes a performance problem, we could add the
    /// monotonicity information of views into the catalog instead.
    fn monotonic_view(&self, id: GlobalId) -> bool {
        self.monotonic_view_inner(id, &mut BTreeMap::new())
            .unwrap_or_else(|e| {
                warn!("Error inspecting view {id} for monotonicity: {e}");
                false
            })
    }

    fn monotonic_view_inner(
        &self,
        id: GlobalId,
        memo: &mut BTreeMap<GlobalId, bool>,
    ) -> Result<bool, RecursionLimitError> {
        self.checked_recur(|_| {
            match self.catalog.get_entry(&id).item() {
                CatalogItem::Source(source) => Ok(self.monotonic_source(source)),
                CatalogItem::View(View { optimized_expr, .. })
                | CatalogItem::MaterializedView(MaterializedView { optimized_expr, .. }) => {
                    let mut view_expr = optimized_expr.clone().into_inner();

                    // Inspect global ids that occur in the Gets in view_expr, and collect the ids
                    // of monotonic (materialized) views and sources (but not indexes).
                    let mut monotonic_ids = BTreeSet::new();
                    let recursion_result: Result<(), RecursionLimitError> = view_expr
                        .try_visit_post(&mut |e| {
                            if let MirRelationExpr::Get {
                                id: Id::Global(got_id),
                                ..
                            } = e
                            {
                                let got_id = *got_id;

                                // A view might be reached multiple times. If we already computed
                                // the monotonicity of the gid, then use that. If not, then compute
                                // it now.
                                let monotonic = match memo.get(&got_id) {
                                    Some(monotonic) => *monotonic,
                                    None => {
                                        let monotonic = self.monotonic_view_inner(got_id, memo)?;
                                        memo.insert(got_id, monotonic);
                                        monotonic
                                    }
                                };
                                if monotonic {
                                    monotonic_ids.insert(got_id);
                                }
                            }
                            Ok(())
                        });
                    if let Err(error) = recursion_result {
                        // We still might have got some of the IDs, so just log and continue. Now
                        // the subsequent monotonicity analysis can have false negatives.
                        warn!("Error inspecting view {id} for monotonicity: {error}");
                    }

                    // Use `monotonic_ids` as a starting point for propagating monotonicity info.
                    mz_transform::monotonic::MonotonicFlag::default().apply(
                        &mut view_expr,
                        &monotonic_ids,
                        &mut BTreeSet::new(),
                    )
                }
                CatalogItem::Secret(_)
                | CatalogItem::Type(_)
                | CatalogItem::Connection(_)
                | CatalogItem::Table(_)
                | CatalogItem::Log(_)
                | CatalogItem::Index(_)
                | CatalogItem::Sink(_)
                | CatalogItem::Func(_) => Ok(false),
            }
        })
    }
}

impl<'a, T> CheckedRecursion for DataflowBuilder<'a, T> {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

/// Prepares a relation expression for dataflow execution by preparing all
/// contained scalar expressions (see `prep_scalar_expr`) in the specified
/// style.
pub fn prep_relation_expr(
    catalog: &CatalogState,
    expr: &mut OptimizedMirRelationExpr,
    style: ExprPrepStyle,
) -> Result<(), AdapterError> {
    match style {
        ExprPrepStyle::Index => {
            expr.0.try_visit_mut_post(&mut |e| {
                // Carefully test filter expressions, which may represent temporal filters.
                if let MirRelationExpr::Filter { input, predicates } = &*e {
                    let mfp =
                        MapFilterProject::new(input.arity()).filter(predicates.iter().cloned());
                    match mfp.into_plan() {
                        Err(e) => coord_bail!("{:?}", e),
                        Ok(mut mfp) => {
                            for s in mfp.iter_nontemporal_exprs() {
                                prep_scalar_expr(catalog, s, style)?;
                            }
                            Ok(())
                        }
                    }
                } else {
                    e.try_visit_scalars_mut1(&mut |s| prep_scalar_expr(catalog, s, style))
                }
            })
        }
        ExprPrepStyle::OneShot { .. } | ExprPrepStyle::AsOfUpTo => expr
            .0
            .try_visit_scalars_mut(&mut |s| prep_scalar_expr(catalog, s, style)),
    }
}

/// Prepares a scalar expression for execution by handling unmaterializable
/// functions.
///
/// Specifically, calls to unmaterializable functions are replaced if
/// `style` is `OneShot`. If `style` is `Index`, then an error is produced
/// if a call to a unmaterializable function is encountered.
pub fn prep_scalar_expr(
    state: &CatalogState,
    expr: &mut MirScalarExpr,
    style: ExprPrepStyle,
) -> Result<(), AdapterError> {
    match style {
        // Evaluate each unmaterializable function and replace the
        // invocation with the result.
        ExprPrepStyle::OneShot {
            logical_time,
            session,
        } => expr.try_visit_mut_post(&mut |e| {
            if let MirScalarExpr::CallUnmaterializable(f) = e {
                *e = eval_unmaterializable_func(state, f, logical_time, session)?;
            }
            Ok(())
        }),

        // Reject the query if it contains any unmaterializable function calls.
        ExprPrepStyle::Index | ExprPrepStyle::AsOfUpTo => {
            let mut last_observed_unmaterializable_func = None;
            expr.visit_mut_post(&mut |e| {
                if let MirScalarExpr::CallUnmaterializable(f) = e {
                    last_observed_unmaterializable_func = Some(f.clone());
                }
            })?;

            if let Some(f) = last_observed_unmaterializable_func {
                let err = match style {
                    ExprPrepStyle::Index => AdapterError::UnmaterializableFunction(f),
                    ExprPrepStyle::AsOfUpTo => AdapterError::UncallableFunction {
                        func: f,
                        context: "AS OF or UP TO",
                    },
                    _ => unreachable!(),
                };
                return Err(err);
            }
            Ok(())
        }
    }
}

fn eval_unmaterializable_func(
    state: &CatalogState,
    f: &UnmaterializableFunc,
    logical_time: Option<mz_repr::Timestamp>,
    session: &Session,
) -> Result<MirScalarExpr, AdapterError> {
    let pack_1d_array = |datums: Vec<Datum>| {
        let mut row = Row::default();
        row.packer()
            .push_array(
                &[ArrayDimension {
                    lower_bound: 1,
                    length: datums.len(),
                }],
                datums,
            )
            .expect("known to be a valid array");
        Ok(MirScalarExpr::Literal(Ok(row), f.output_type()))
    };
    let pack_dict = |mut datums: Vec<(String, String)>| {
        datums.sort();
        let mut row = Row::default();
        row.packer().push_dict(
            datums
                .iter()
                .map(|(key, value)| (key.as_str(), Datum::from(value.as_str()))),
        );
        Ok(MirScalarExpr::Literal(Ok(row), f.output_type()))
    };
    let pack = |datum| {
        Ok(MirScalarExpr::literal_ok(
            datum,
            f.output_type().scalar_type,
        ))
    };

    match f {
        UnmaterializableFunc::CurrentDatabase => pack(Datum::from(session.vars().database())),
        UnmaterializableFunc::CurrentSchemasWithSystem => {
            let catalog = Catalog::for_session_state(state, session);
            let search_path = catalog.effective_search_path(false);
            pack_1d_array(
                search_path
                    .into_iter()
                    .map(|(db, schema)| {
                        let schema = state.get_schema(&db, &schema, session.conn_id());
                        Datum::String(&schema.name.schema)
                    })
                    .collect(),
            )
        }
        UnmaterializableFunc::CurrentSchemasWithoutSystem => {
            let catalog = Catalog::for_session_state(state, session);
            let search_path = catalog.search_path();
            pack_1d_array(
                search_path
                    .into_iter()
                    .map(|(db, schema)| {
                        let schema = state.get_schema(db, schema, session.conn_id());
                        Datum::String(&schema.name.schema)
                    })
                    .collect(),
            )
        }
        UnmaterializableFunc::ViewableVariables => pack_dict(
            viewable_variables(state, session)
                .map(|var| (var.name().to_lowercase(), var.value()))
                .collect(),
        ),
        UnmaterializableFunc::CurrentTimestamp => {
            let t: Datum = session.pcx().wall_time.try_into()?;
            pack(t)
        }
        UnmaterializableFunc::CurrentUser => pack(Datum::from(&*session.user().name)),
        UnmaterializableFunc::IsRbacEnabled => pack(Datum::from(
            rbac::is_rbac_enabled_for_session(state.system_config(), session),
        )),
        UnmaterializableFunc::MzEnvironmentId => {
            pack(Datum::from(&*state.config().environment_id.to_string()))
        }
        UnmaterializableFunc::MzNow => match logical_time {
            None => coord_bail!("cannot call mz_now in this context"),
            Some(logical_time) => pack(Datum::MzTimestamp(logical_time)),
        },
        UnmaterializableFunc::MzSessionId => pack(Datum::from(state.config().session_id)),
        UnmaterializableFunc::MzUptime => {
            let uptime = state.config().start_instant.elapsed();
            let uptime = chrono::Duration::from_std(uptime).map_or(Datum::Null, Datum::from);
            pack(uptime)
        }
        UnmaterializableFunc::MzVersion => {
            pack(Datum::from(&*state.config().build_info.human_version()))
        }
        UnmaterializableFunc::MzVersionNum => {
            pack(Datum::Int32(state.config().build_info.version_num()))
        }
        UnmaterializableFunc::PgBackendPid => {
            pack(Datum::Int32(i32::reinterpret_cast(session.conn_id())))
        }
        UnmaterializableFunc::PgPostmasterStartTime => {
            let t: Datum = state.config().start_time.try_into()?;
            pack(t)
        }
        UnmaterializableFunc::Version => {
            let build_info = state.config().build_info;
            let version = format!(
                "PostgreSQL {}.{} on {} (Materialize {})",
                SERVER_MAJOR_VERSION,
                SERVER_MINOR_VERSION,
                mz_build_info::TARGET_TRIPLE,
                build_info.version,
            );
            pack(Datum::from(&*version))
        }
    }
}

#[cfg(test)]
impl Coordinator {
    #[allow(dead_code)]
    async fn verify_ship_dataflow_no_error(&mut self) {
        // must_ship_dataflow, must_ship_dataflows, and must_finalize_dataflow are not allowed
        // to have a `Result` return because these functions are called after
        // `catalog_transact`, after which no errors are allowed. This test exists to
        // prevent us from incorrectly teaching those functions how to return errors
        // (which has happened twice and is the motivation for this test).

        // An arbitrary compute instance ID to satisfy the function calls below. Note that
        // this only works because this function will never run.
        let compute_instance = ComputeInstanceId::User(1);

        let df = DataflowDesc::new("".into());
        let _: () = self.must_ship_dataflow(df.clone(), compute_instance).await;
        let _: () = self
            .must_ship_dataflows(vec![df.clone()], compute_instance)
            .await;
        let _: DataflowDescription<mz_compute_client::plan::Plan> =
            self.must_finalize_dataflow(df, compute_instance);
    }
}
