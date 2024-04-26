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

use chrono::{DateTime, Utc};
use maplit::{btreemap, btreeset};

use mz_catalog::memory::objects::{CatalogItem, DataSourceDesc, MaterializedView, Source, View};
use mz_compute_client::controller::error::InstanceMissing;
use mz_compute_types::dataflows::{DataflowDesc, DataflowDescription, IndexDesc};

use mz_compute_types::ComputeInstanceId;
use mz_controller::Controller;
use mz_expr::visit::Visit;
use mz_expr::{
    CollectionPlan, Id, MapFilterProject, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr,
    UnmaterializableFunc, RECURSION_LIMIT,
};
use mz_ore::cast::ReinterpretCast;
use mz_ore::stack::{maybe_grow, CheckedRecursion, RecursionGuard, RecursionLimitError};
use mz_repr::adt::array::ArrayDimension;
use mz_repr::explain::trace_plan;
use mz_repr::role_id::RoleId;
use mz_repr::{Datum, GlobalId, Row};
use mz_sql::catalog::CatalogRole;
use mz_sql::rbac;
use mz_sql::session::metadata::SessionMetadata;
use tracing::warn;

use crate::catalog::CatalogState;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::optimize::{view, Optimize, OptimizerConfig, OptimizerError};
use crate::session::{SERVER_MAJOR_VERSION, SERVER_MINOR_VERSION};
use crate::util::viewable_variables;

/// A reference-less snapshot of a compute instance. There is no guarantee `instance_id` continues
/// to exist after this has been made.
#[derive(Debug, Clone)]
pub struct ComputeInstanceSnapshot {
    instance_id: ComputeInstanceId,
    collections: BTreeSet<GlobalId>,
}

impl ComputeInstanceSnapshot {
    pub fn new(controller: &Controller, id: ComputeInstanceId) -> Result<Self, InstanceMissing> {
        controller
            .compute
            .instance_ref(id)
            .map(|instance| ComputeInstanceSnapshot {
                instance_id: id,
                collections: BTreeSet::from_iter(instance.collections().map(|(id, _state)| *id)),
            })
    }

    /// Return the ID of this compute instance.
    pub fn instance_id(&self) -> ComputeInstanceId {
        self.instance_id
    }

    /// Reports whether the instance contains the indicated collection.
    pub fn contains_collection(&self, id: &GlobalId) -> bool {
        self.collections.contains(id)
    }

    /// Inserts the given collection into the snapshot.
    pub fn insert_collection(&mut self, id: GlobalId) {
        self.collections.insert(id);
    }
}

/// Borrows of catalog and indexes sufficient to build dataflow descriptions.
#[derive(Debug)]
pub struct DataflowBuilder<'a> {
    pub catalog: &'a CatalogState,
    /// A handle to the compute abstraction, which describes indexes by identifier.
    ///
    /// This can also be used to grab a handle to the storage abstraction, through
    /// its `storage_mut()` method.
    pub compute: ComputeInstanceSnapshot,
    /// If set, indicates that the `DataflowBuilder` operates in "replan" mode
    /// and should consider only catalog items that are strictly less than the
    /// given [`GlobalId`].
    ///
    /// In particular, indexes with higher [`GlobalId`] that are present in the
    /// catalog will be ignored.
    ///
    /// Bound from [`OptimizerConfig::replan`].
    pub replan: Option<GlobalId>,
    /// A guard for recursive operations in this [`DataflowBuilder`] instance.
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
        logical_time: EvalTime,
        session: &'a dyn SessionMetadata,
        catalog_state: &'a CatalogState,
    },
    /// The expression is being prepared for evaluation in an AS OF or UP TO clause.
    AsOfUpTo,
    /// The expression is being prepared for evaluation in a CHECK expression of a webhook source.
    WebhookValidation {
        /// Time at which this expression is being evaluated.
        now: DateTime<Utc>,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum EvalTime {
    Time(mz_repr::Timestamp),
    /// Skips mz_now() calls.
    Deferred,
    /// Errors on mz_now() calls.
    NotAvailable,
}

/// Returns an ID bundle with the given dataflows imports.
pub fn dataflow_import_id_bundle<P>(
    dataflow: &DataflowDescription<P>,
    compute_instance: ComputeInstanceId,
) -> CollectionIdBundle {
    let storage_ids = dataflow.source_imports.keys().copied().collect();
    let compute_ids = dataflow.index_imports.keys().copied().collect();
    CollectionIdBundle {
        storage_ids,
        compute_ids: btreemap! {compute_instance => compute_ids},
    }
}

impl<'a> DataflowBuilder<'a> {
    pub fn new(catalog: &'a CatalogState, compute: ComputeInstanceSnapshot) -> Self {
        Self {
            catalog,
            compute,
            replan: None,
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }

    // TODO(aalexandrov): strictly speaking it should be better if we can make
    // `config: &OptimizerConfig` a field in the enclosing builder. However,
    // before we can do that we should make sure that nobody outside of the
    // optimizer is using a DataflowBuilder instance.
    pub(super) fn with_config(mut self, config: &OptimizerConfig) -> Self {
        self.replan = config.replan;
        self
    }

    /// Imports the view, source, or table with `id` into the provided
    /// dataflow description.
    pub fn import_into_dataflow(
        &mut self,
        id: &GlobalId,
        dataflow: &mut DataflowDesc,
    ) -> Result<(), OptimizerError> {
        maybe_grow(|| {
            // Avoid importing the item redundantly.
            if dataflow.is_imported(id) {
                return Ok(());
            }

            // A valid index is any index on `id` that is known to index oracle.
            // Here, we import all indexes that belong to all imported collections. Later,
            // `prune_and_annotate_dataflow_index_imports` runs at the end of the MIR
            // pipeline, and removes unneeded index imports based on the optimized plan.
            let mut valid_indexes = self.indexes_on(*id).peekable();
            if valid_indexes.peek().is_some() {
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
    ) -> Result<(), OptimizerError> {
        for get_id in view.depends_on() {
            self.import_into_dataflow(&get_id, dataflow)?;
        }
        dataflow.insert_plan(*view_id, view.clone());
        Ok(())
    }

    // Re-optimize the imported view plans using the current optimizer
    // configuration if reoptimization is requested.
    pub fn maybe_reoptimize_imported_views(
        &self,
        df_desc: &mut DataflowDesc,
        config: &OptimizerConfig,
    ) -> Result<(), OptimizerError> {
        if !config.features.reoptimize_imported_views {
            return Ok(()); // Do nothing if not explicitly requested.
        }

        let mut view_optimizer = view::Optimizer::new(config.clone(), None);
        for desc in df_desc.objects_to_build.iter_mut().rev() {
            if matches!(desc.id, GlobalId::Explain | GlobalId::Transient(_)) {
                continue; // Skip descriptions that do not reference proper views.
            }
            if let CatalogItem::View(view) = &self.catalog.get_entry(&desc.id).item {
                let _span = tracing::span!(
                    target: "optimizer",
                    tracing::Level::DEBUG,
                    "view",
                    path.segment = desc.id.to_string()
                )
                .entered();

                // Reoptimize the view and update the resulting `desc.plan`.
                desc.plan = view_optimizer.optimize(view.raw_expr.clone())?;

                // Report the optimized plan under this span.
                trace_plan(desc.plan.as_inner());
            }
        }

        Ok(())
    }

    /// Determine the given source's monotonicity.
    fn monotonic_source(&self, source: &Source) -> bool {
        // TODO(petrosagg): store an inverse mapping of subsource -> source in the catalog so that
        // we can retrieve monotonicity information from the parent source.
        match &source.data_source {
            DataSourceDesc::Ingestion(ingestion) => ingestion.desc.monotonic(),
            DataSourceDesc::IngestionExport { .. }
            | DataSourceDesc::Introspection(_)
            | DataSourceDesc::Progress
            | DataSourceDesc::Webhook { .. }
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

impl<'a> CheckedRecursion for DataflowBuilder<'a> {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

/// Prepares a relation expression for dataflow execution by preparing all
/// contained scalar expressions (see `prep_scalar_expr`) in the specified
/// style.
pub fn prep_relation_expr(
    expr: &mut OptimizedMirRelationExpr,
    style: ExprPrepStyle,
) -> Result<(), OptimizerError> {
    match style {
        ExprPrepStyle::Index => {
            expr.0.try_visit_mut_post(&mut |e| {
                // Carefully test filter expressions, which may represent temporal filters.
                if let MirRelationExpr::Filter { input, predicates } = &*e {
                    let mfp =
                        MapFilterProject::new(input.arity()).filter(predicates.iter().cloned());
                    match mfp.into_plan() {
                        Err(e) => Err(OptimizerError::Internal(e)),
                        Ok(mut mfp) => {
                            for s in mfp.iter_nontemporal_exprs() {
                                prep_scalar_expr(s, style)?;
                            }
                            Ok(())
                        }
                    }
                } else {
                    e.try_visit_scalars_mut1(&mut |s| prep_scalar_expr(s, style))
                }
            })
        }
        ExprPrepStyle::OneShot { .. }
        | ExprPrepStyle::AsOfUpTo
        | ExprPrepStyle::WebhookValidation { .. } => expr
            .0
            .try_visit_scalars_mut(&mut |s| prep_scalar_expr(s, style)),
    }
}

/// Prepares a scalar expression for execution by handling unmaterializable
/// functions.
///
/// How we prepare the scalar expression depends on which `style` is specificed.
///
/// * `OneShot`: Calls to all unmaterializable functions are replaced.
/// * `Index`: An error is produced if a call to an unmaterializable function is encountered.
/// * `AsOfUpTo`: An error is produced if a call to an unmaterializable function is encountered.
/// * `WebhookValidation`: Only calls to `UnmaterializableFunc::CurrentTimestamp` are replaced,
///   others are left untouched.
///
pub fn prep_scalar_expr(
    expr: &mut MirScalarExpr,
    style: ExprPrepStyle,
) -> Result<(), OptimizerError> {
    match style {
        // Evaluate each unmaterializable function and replace the
        // invocation with the result.
        ExprPrepStyle::OneShot {
            logical_time,
            session,
            catalog_state,
        } => expr.try_visit_mut_post(&mut |e| {
            if let MirScalarExpr::CallUnmaterializable(f) = e {
                *e = eval_unmaterializable_func(catalog_state, f, logical_time, session)?;
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
                    ExprPrepStyle::Index => OptimizerError::UnmaterializableFunction(f),
                    ExprPrepStyle::AsOfUpTo => OptimizerError::UncallableFunction {
                        func: f,
                        context: "AS OF or UP TO",
                    },
                    _ => unreachable!(),
                };
                return Err(err);
            }
            Ok(())
        }

        ExprPrepStyle::WebhookValidation { now } => {
            expr.try_visit_mut_post(&mut |e| {
                if let MirScalarExpr::CallUnmaterializable(
                    f @ UnmaterializableFunc::CurrentTimestamp,
                ) = e
                {
                    let now: Datum = now.try_into()?;
                    let const_expr = MirScalarExpr::literal_ok(now, f.output_type().scalar_type);
                    *e = const_expr;
                }
                Ok::<_, anyhow::Error>(())
            })?;
            Ok(())
        }
    }
}

fn eval_unmaterializable_func(
    state: &CatalogState,
    f: &UnmaterializableFunc,
    logical_time: EvalTime,
    session: &dyn SessionMetadata,
) -> Result<MirScalarExpr, OptimizerError> {
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
        UnmaterializableFunc::CurrentDatabase => pack(Datum::from(session.database())),
        UnmaterializableFunc::CurrentSchema => {
            let search_path = state.resolve_search_path(session);
            let schema = search_path
                .first()
                .map(|(db, schema)| &*state.get_schema(db, schema, session.conn_id()).name.schema);
            pack(Datum::from(schema))
        }
        UnmaterializableFunc::CurrentSchemasWithSystem => {
            let search_path = state.resolve_search_path(session);
            let search_path = state.effective_search_path(&search_path, false);
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
            let search_path = state.resolve_search_path(session);
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
        UnmaterializableFunc::ViewableVariables => pack_dict(
            viewable_variables(state, session)
                .map(|var| (var.name().to_lowercase(), var.value()))
                .collect(),
        ),
        UnmaterializableFunc::CurrentTimestamp => {
            let t: Datum = session.pcx().wall_time.try_into()?;
            pack(t)
        }
        UnmaterializableFunc::CurrentUser => pack(Datum::from(
            state.get_role(session.current_role_id()).name(),
        )),
        UnmaterializableFunc::SessionUser => pack(Datum::from(
            state.get_role(session.session_role_id()).name(),
        )),
        UnmaterializableFunc::IsRbacEnabled => pack(Datum::from(
            rbac::is_rbac_enabled_for_session(state.system_config(), session),
        )),
        UnmaterializableFunc::MzEnvironmentId => {
            pack(Datum::from(&*state.config().environment_id.to_string()))
        }
        UnmaterializableFunc::MzIsSuperuser => pack(Datum::from(session.is_superuser())),
        UnmaterializableFunc::MzNow => match logical_time {
            EvalTime::Time(logical_time) => pack(Datum::MzTimestamp(logical_time)),
            EvalTime::Deferred => Ok(MirScalarExpr::CallUnmaterializable(f.clone())),
            EvalTime::NotAvailable => Err(OptimizerError::UncallableFunction {
                func: UnmaterializableFunc::MzNow,
                context: "this",
            }),
        },
        UnmaterializableFunc::MzRoleOidMemberships => {
            let role_memberships = role_oid_memberships(state);
            let mut role_memberships: Vec<(_, Vec<_>)> = role_memberships
                .into_iter()
                .map(|(role_id, role_membership)| {
                    (
                        role_id.to_string(),
                        role_membership
                            .into_iter()
                            .map(|role_id| role_id.to_string())
                            .collect(),
                    )
                })
                .collect();
            role_memberships.sort();
            let mut row = Row::default();
            row.packer().push_dict_with(|row| {
                for (role_id, role_membership) in &role_memberships {
                    row.push(Datum::from(role_id.as_str()));
                    row.push_array(
                        &[ArrayDimension {
                            lower_bound: 1,
                            length: role_membership.len(),
                        }],
                        role_membership.iter().map(|role_id| Datum::from(role_id.as_str())),
                    ).expect("role_membership is 1 dimensional, and its length is used for the array length");
                }
            });
            Ok(MirScalarExpr::Literal(Ok(row), f.output_type()))
        }
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
        UnmaterializableFunc::PgBackendPid => pack(Datum::Int32(i32::reinterpret_cast(
            session.conn_id().unhandled(),
        ))),
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

fn role_oid_memberships<'a>(catalog: &'a CatalogState) -> BTreeMap<u32, BTreeSet<u32>> {
    let mut role_memberships = BTreeMap::new();
    for role_id in catalog.get_roles() {
        let role = catalog.get_role(role_id);
        if !role_memberships.contains_key(&role.oid) {
            role_oid_memberships_inner(catalog, role_id, &mut role_memberships);
        }
    }
    role_memberships
}

fn role_oid_memberships_inner<'a>(
    catalog: &'a CatalogState,
    role_id: &RoleId,
    role_memberships: &mut BTreeMap<u32, BTreeSet<u32>>,
) {
    let role = catalog.get_role(role_id);
    role_memberships.insert(role.oid, btreeset! {role.oid});
    for parent_role_id in role.membership.map.keys() {
        let parent_role = catalog.get_role(parent_role_id);
        if !role_memberships.contains_key(&parent_role.oid) {
            role_oid_memberships_inner(catalog, parent_role_id, role_memberships);
        }
        let parent_membership: BTreeSet<_> = role_memberships
            .get(&parent_role.oid)
            .expect("inserted in recursive call above")
            .into_iter()
            .cloned()
            .collect();
        role_memberships
            .get_mut(&role.oid)
            .expect("inserted above")
            .extend(parent_membership);
    }
}
