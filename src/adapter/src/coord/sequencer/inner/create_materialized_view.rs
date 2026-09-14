// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use differential_dataflow::lattice::Lattice;
use maplit::btreemap;
use mz_catalog::durable::objects::MaintainedReadRequirement;
use mz_catalog::memory::error::ErrorKind;
use mz_catalog::memory::objects::{CatalogItem, MaterializedView};
use mz_expr::{CollectionPlan, ResultSpec};
use mz_ore::collections::CollectionExt;
use mz_ore::instrument;
use mz_ore::soft_panic_or_log;
use mz_repr::explain::{ExprHumanizerExt, TransientItem};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::optimize::OverrideFrom;
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{
    CatalogItemId, Datum, GlobalId, RelationVersion, Row, Timestamp, VersionedRelationDesc,
};
use mz_sql::ast::ExplainStage;
use mz_sql::catalog::CatalogError;
use mz_sql::names::ResolvedIds;
use mz_sql::plan;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql_parser::ast;
use mz_sql_parser::ast::display::AstDisplay;
use mz_transform::notice::OptimizerNoticeApi;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use timely::progress::Antichain;
use tracing::Span;

use crate::ReadHolds;
use crate::catalog::CatalogState;
use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{
    Coordinator, CreateMaterializedViewExplain, CreateMaterializedViewFinish,
    CreateMaterializedViewOptimize, CreateMaterializedViewStage, ExplainContext,
    ExplainPlanContext, Message, PlanValidity, StageResult, Staged,
};
use crate::error::AdapterError;
use crate::explain::explain_dataflow;
use crate::explain::explain_plan;
use crate::explain::optimizer_trace::OptimizerTrace;
use crate::optimize::dataflows::{ComputeInstanceSnapshot, dataflow_import_id_bundle};
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::{AdapterNotice, CollectionIdBundle, ExecuteContext, TimestampProvider, catalog};

/// Reuses a compatible candidate, or replaces both global plans and their raw
/// notices using an optimizer restricted to `indexes`. The caller must protect
/// the final imports at its frozen timestamp before writing the candidate.
fn ensure_materialized_view_access_paths(
    indexes: &BTreeSet<GlobalId>,
    local_mir: &optimize::materialized_view::LocalMirPlan,
    global_mir: &mut optimize::materialized_view::GlobalMirPlan,
    global_lir: &mut optimize::materialized_view::GlobalLirPlan,
    optimizer: impl FnOnce() -> optimize::materialized_view::Optimizer,
) -> Result<(), AdapterError> {
    if global_lir
        .df_desc()
        .index_imports
        .keys()
        .chain(global_mir.df_desc().index_imports.keys())
        .all(|id| indexes.contains(id))
    {
        return Ok(());
    }
    let mut optimizer = optimizer();
    let mir = optimizer.catch_unwind_optimize(local_mir.clone())?;
    let lir = optimizer.catch_unwind_optimize(mir.clone())?;
    if lir.desc() != global_lir.desc() {
        return Err(AdapterError::internal(
            "create materialized view",
            "access-path planning changed the output schema",
        ));
    }
    *global_mir = mir;
    *global_lir = lir;
    Ok(())
}

impl Staged for CreateMaterializedViewStage {
    type Ctx = ExecuteContext;

    fn validity(&mut self) -> &mut PlanValidity {
        match self {
            Self::Optimize(stage) => &mut stage.validity,
            Self::Finish(stage) => &mut stage.validity,
            Self::Explain(stage) => &mut stage.validity,
        }
    }

    async fn stage(
        self,
        coord: &mut Coordinator,
        ctx: &mut ExecuteContext,
    ) -> Result<StageResult<Box<Self>>, AdapterError> {
        match self {
            CreateMaterializedViewStage::Optimize(stage) => {
                coord.create_materialized_view_optimize(stage).await
            }
            CreateMaterializedViewStage::Finish(stage) => {
                coord.create_materialized_view_finish(ctx, stage).await
            }
            CreateMaterializedViewStage::Explain(stage) => {
                coord
                    .create_materialized_view_explain(ctx.session(), stage)
                    .await
            }
        }
    }

    fn message(self, ctx: ExecuteContext, span: Span) -> Message {
        Message::CreateMaterializedViewStageReady {
            ctx,
            span,
            stage: self,
        }
    }

    fn cancel_enabled(&self) -> bool {
        true
    }
}

impl Coordinator {
    /// Returns the committed input permission for MV admission, not a read hold.
    pub(crate) fn materialized_view_input_permission(
        &self,
        ids: impl IntoIterator<Item = GlobalId>,
    ) -> Result<Antichain<Timestamp>, AdapterError> {
        let mut permission = Antichain::from_elem(Timestamp::MIN);
        if self.catalog().state().catalog_read_protection_enabled() {
            for id in ids {
                let bound = self
                    .catalog()
                    .state()
                    .storage_metadata()
                    .compaction_bounds
                    .get(&id)
                    .ok_or_else(|| {
                        AdapterError::internal(
                            "create materialized view",
                            format!("logical input {id} has no committed compaction bound"),
                        )
                    })?;
                permission.join_assign(bound);
            }
        }
        Ok(permission)
    }

    /// Discovers storage inputs required to reconstruct an MV from its definition.
    pub(crate) fn materialized_view_logical_inputs(
        &self,
        ids: impl IntoIterator<Item = GlobalId>,
    ) -> Result<CollectionIdBundle, AdapterError> {
        let inputs = self.catalog().state().logical_collection_inputs(
            ids.into_iter()
                .filter(|id| self.catalog().get_entry_by_global_id(id).is_relation()),
        );
        let log_names: Vec<_> = inputs
            .iter()
            .map(|id| self.catalog().get_entry_by_global_id(id))
            .filter(|entry| matches!(entry.item(), CatalogItem::Log(_)))
            .map(|entry| entry.name().item.clone())
            .collect();
        if !log_names.is_empty() {
            return Err(AdapterError::InvalidLogDependency {
                object_type: "materialized view".into(),
                log_names,
            });
        }
        Ok(CollectionIdBundle {
            storage_ids: inputs,
            compute_ids: BTreeMap::new(),
        })
    }

    #[instrument]
    pub(crate) async fn sequence_create_materialized_view(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CreateMaterializedViewPlan,
        resolved_ids: ResolvedIds,
    ) {
        let stage = return_if_err!(
            self.create_materialized_view_validate(
                ctx.session(),
                plan,
                resolved_ids,
                ExplainContext::None
            ),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    pub(crate) async fn explain_create_materialized_view(
        &mut self,
        ctx: ExecuteContext,
        plan::ExplainPlanPlan {
            stage,
            format,
            config,
            explainee,
        }: plan::ExplainPlanPlan,
    ) {
        let plan::Explainee::Statement(stmt) = explainee else {
            // This is currently asserted in the `sequence_explain_plan` code that
            // calls this method.
            unreachable!()
        };
        let plan::ExplaineeStatement::CreateMaterializedView { broken, plan } = stmt else {
            // This is currently asserted in the `sequence_explain_plan` code that
            // calls this method.
            unreachable!()
        };

        // Create an OptimizerTrace instance to collect plans emitted when
        // executing the optimizer pipeline.
        let optimizer_trace = OptimizerTrace::new(stage.paths());

        // Not used in the EXPLAIN path so it's OK to generate a dummy value.
        let resolved_ids = ResolvedIds::empty();

        let explain_ctx = ExplainContext::Plan(ExplainPlanContext {
            broken,
            config,
            format,
            stage,
            replan: None,
            desc: None,
            optimizer_trace,
        });
        let stage = return_if_err!(
            self.create_materialized_view_validate(ctx.session(), plan, resolved_ids, explain_ctx),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    pub(crate) async fn explain_replan_materialized_view(
        &mut self,
        ctx: ExecuteContext,
        plan::ExplainPlanPlan {
            stage,
            format,
            config,
            explainee,
        }: plan::ExplainPlanPlan,
    ) {
        let plan::Explainee::ReplanMaterializedView(id) = explainee else {
            unreachable!() // Asserted in `sequence_explain_plan`.
        };
        let CatalogItem::MaterializedView(item) = self.catalog().get_entry(&id).item() else {
            unreachable!() // Asserted in `plan_explain_plan`.
        };
        let gid = item.global_id_writes();

        let create_sql = item.create_sql.clone();
        let plan_result = self
            .catalog_mut()
            .deserialize_plan_with_enable_for_item_parsing(&create_sql, true);
        let (plan, resolved_ids) = return_if_err!(plan_result, ctx);

        let plan::Plan::CreateMaterializedView(plan) = plan else {
            unreachable!() // We are parsing the `create_sql` of a `MaterializedView` item.
        };

        // It is safe to assume that query optimization will always succeed, so
        // for now we statically assume `broken = false`.
        let broken = false;

        // Create an OptimizerTrace instance to collect plans emitted when
        // executing the optimizer pipeline.
        let optimizer_trace = OptimizerTrace::new(stage.paths());

        let explain_ctx = ExplainContext::Plan(ExplainPlanContext {
            broken,
            config,
            format,
            stage,
            replan: Some(gid),
            desc: None,
            optimizer_trace,
        });
        let stage = return_if_err!(
            self.create_materialized_view_validate(ctx.session(), plan, resolved_ids, explain_ctx,),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    pub(super) fn explain_materialized_view(
        &self,
        ctx: &ExecuteContext,
        plan::ExplainPlanPlan {
            stage,
            format,
            config,
            explainee,
        }: plan::ExplainPlanPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::Explainee::MaterializedView(id) = explainee else {
            unreachable!() // Asserted in `sequence_explain_plan`.
        };
        let CatalogItem::MaterializedView(view) = self.catalog().get_entry(&id).item() else {
            unreachable!() // Asserted in `plan_explain_plan`.
        };
        let gid = view.global_id_writes();

        let Some(dataflow_metainfo) = self.catalog().try_get_dataflow_metainfo(&gid) else {
            if !id.is_system() {
                tracing::error!(
                    "cannot find dataflow metainformation for materialized view {id} in catalog"
                );
            }
            coord_bail!(
                "cannot find dataflow metainformation for materialized view {id} in catalog"
            );
        };

        let target_cluster = self.catalog().get_cluster(view.cluster_id);

        let features = OptimizerFeatures::from(self.catalog().system_config())
            .override_from(&target_cluster.config.features())
            .override_from(&self.cluster_scoped_optimizer_overrides(view.cluster_id))
            .override_from(&config.features);

        let cardinality_stats = BTreeMap::new();

        let explain = match stage {
            ExplainStage::RawPlan => explain_plan(
                view.raw_expr.as_ref().clone(),
                format,
                &config,
                &features,
                &self.catalog().for_session(ctx.session()),
                cardinality_stats,
                Some(target_cluster.name.as_str()),
            )?,
            ExplainStage::LocalPlan => explain_plan(
                view.locally_optimized_expr.as_inner().clone(),
                format,
                &config,
                &features,
                &self.catalog().for_session(ctx.session()),
                cardinality_stats,
                Some(target_cluster.name.as_str()),
            )?,
            ExplainStage::GlobalPlan => {
                let Some(plan) = self.catalog().try_get_optimized_plan(&gid).cloned() else {
                    tracing::error!("cannot find {stage} for materialized view {id} in catalog");
                    coord_bail!("cannot find {stage} for materialized view in catalog");
                };
                explain_dataflow(
                    plan,
                    format,
                    &config,
                    &features,
                    &self.catalog().for_session(ctx.session()),
                    cardinality_stats,
                    Some(target_cluster.name.as_str()),
                    dataflow_metainfo,
                )?
            }
            ExplainStage::PhysicalPlan => {
                let Some(plan) = self.catalog().try_get_physical_plan(&gid).cloned() else {
                    tracing::error!("cannot find {stage} for materialized view {id} in catalog",);
                    coord_bail!("cannot find {stage} for materialized view in catalog");
                };
                explain_dataflow(
                    plan,
                    format,
                    &config,
                    &features,
                    &self.catalog().for_session(ctx.session()),
                    cardinality_stats,
                    Some(target_cluster.name.as_str()),
                    dataflow_metainfo,
                )?
            }
            _ => {
                coord_bail!("cannot EXPLAIN {} FOR MATERIALIZED VIEW", stage);
            }
        };

        let row = Row::pack_slice(&[Datum::from(explain.as_str())]);

        Ok(Self::send_immediate_rows(row))
    }

    #[instrument]
    fn create_materialized_view_validate(
        &self,
        session: &Session,
        plan: plan::CreateMaterializedViewPlan,
        resolved_ids: ResolvedIds,
        // An optional context set iff the state machine is initiated from
        // sequencing an EXPLAIN for this statement.
        explain_ctx: ExplainContext,
    ) -> Result<CreateMaterializedViewStage, AdapterError> {
        let plan::CreateMaterializedViewPlan {
            materialized_view:
                plan::MaterializedView {
                    expr,
                    query_ids,
                    cluster_id,
                    target_replica,
                    refresh_schedule,
                    ..
                },
            ambiguous_columns,
            ..
        } = &plan;

        // Validate any references in the materialized view's expression. We do
        // this on the unoptimized plan to better reflect what the user typed.
        // We want to reject queries that depend on log sources, for example,
        // even if we can *technically* optimize that reference away.
        let expr_depends_on = expr.depends_on();
        self.catalog()
            .validate_timeline_context(expr_depends_on.iter().copied())?;
        self.validate_system_column_references(*ambiguous_columns, &expr_depends_on)?;
        // Materialized views are not allowed to depend on log sources, as replicas
        // are not producing the same definite collection for these.
        let log_names = expr_depends_on
            .iter()
            .map(|gid| self.catalog.resolve_item_id(gid))
            .flat_map(|item_id| self.catalog().introspection_dependencies(item_id))
            .map(|item_id| self.catalog().get_entry(&item_id).name().item.clone())
            .collect::<Vec<_>>();
        if !log_names.is_empty() {
            return Err(AdapterError::InvalidLogDependency {
                object_type: "materialized view".into(),
                log_names,
            });
        }

        // Track the target cluster/replica and resolved dependencies so that
        // concurrent drops (e.g. `ALTER CLUSTER ... SET (REPLICATION FACTOR
        // ...)` racing with the off-thread optimizer) are caught between
        // stages instead of panicking later when the persisted SQL is
        // re-parsed during catalog application.
        let validity = PlanValidity::new(
            self.catalog(),
            resolved_ids.items().copied().collect(),
            Some(*cluster_id),
            *target_replica,
            session.role_metadata().clone(),
        );

        // Check whether we can read all inputs at all the REFRESH AT times.
        if let Some(refresh_schedule) = refresh_schedule {
            if !refresh_schedule.ats.is_empty() && matches!(explain_ctx, ExplainContext::None) {
                // Purification has acquired the earliest possible read holds if there are any
                // REFRESH options.
                let read_holds = self
                    .txn_read_holds
                    .get(session.conn_id())
                    .expect("purification acquired read holds if there are REFRESH ATs");
                let least_valid_read = read_holds.least_valid_read();
                for refresh_at_ts in &refresh_schedule.ats {
                    if !least_valid_read.less_equal(refresh_at_ts) {
                        return Err(AdapterError::InputNotReadableAtRefreshAtTime(
                            *refresh_at_ts,
                            least_valid_read,
                        ));
                    }
                }
                // Purification must cover the admission inputs. In protected
                // mode these are logical dependencies, independent of indexes.
                let ids = if self.catalog().state().catalog_read_protection_enabled() {
                    self.materialized_view_logical_inputs(query_ids.collections().copied())?
                } else {
                    self.index_oracle(*cluster_id)
                        .sufficient_collections(query_ids.collections().copied())
                };
                if !ids.difference(&read_holds.id_bundle()).is_empty() {
                    return Err(AdapterError::ChangedPlan(
                        "the set of possible inputs changed during the creation of the \
                         materialized view"
                            .to_string(),
                    ));
                }
            }
        }

        Ok(CreateMaterializedViewStage::Optimize(
            CreateMaterializedViewOptimize {
                validity,
                plan,
                resolved_ids,
                explain_ctx,
            },
        ))
    }

    #[instrument]
    async fn create_materialized_view_optimize(
        &mut self,
        CreateMaterializedViewOptimize {
            validity,
            plan,
            resolved_ids,
            explain_ctx,
        }: CreateMaterializedViewOptimize,
    ) -> Result<StageResult<Box<CreateMaterializedViewStage>>, AdapterError> {
        let plan::CreateMaterializedViewPlan {
            name,
            materialized_view:
                plan::MaterializedView {
                    column_names,
                    cluster_id,
                    non_null_assertions,
                    refresh_schedule,
                    ..
                },
            ..
        } = &plan;

        // Collect optimizer parameters.
        let compute_instance = self
            .candidate_instance_snapshot(*cluster_id)
            .expect("compute instance does not exist");
        let (item_id, global_id) = if let ExplainContext::None = explain_ctx {
            self.allocate_user_id().await?
        } else {
            self.allocate_transient_id()
        };

        let (_, view_id) = self.allocate_transient_id();
        let debug_name = self.catalog().resolve_full_name(name, None).to_string();
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&self.catalog.get_cluster(*cluster_id).config.features())
            .override_from(&self.cluster_scoped_optimizer_overrides(*cluster_id))
            .override_from(&explain_ctx);
        let optimizer_features = optimizer_config.features.clone();

        // Build an optimizer for this MATERIALIZED VIEW.
        let mut optimizer = optimize::materialized_view::Optimizer::new(
            self.owned_catalog().as_optimizer_catalog(),
            compute_instance,
            global_id,
            view_id,
            column_names.clone(),
            non_null_assertions.clone(),
            refresh_schedule.clone(),
            debug_name,
            optimizer_config,
            self.optimizer_metrics(),
        );

        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize create materialized view",
            move || {
                span.in_scope(|| {
                    let mut pipeline = || -> Result<(
                        optimize::materialized_view::LocalMirPlan,
                        optimize::materialized_view::GlobalMirPlan,
                        optimize::materialized_view::GlobalLirPlan,
                    ), AdapterError> {
                        let _dispatch_guard = explain_ctx.dispatch_guard();

                        let raw_expr = plan.materialized_view.expr.clone();

                        // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local and global)
                        let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr)?;
                        let global_mir_plan =
                            optimizer.catch_unwind_optimize(local_mir_plan.clone())?;
                        // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                        let global_lir_plan =
                            optimizer.catch_unwind_optimize(global_mir_plan.clone())?;

                        Ok((local_mir_plan, global_mir_plan, global_lir_plan))
                    };

                    let stage = match pipeline() {
                        Ok((local_mir_plan, global_mir_plan, global_lir_plan)) => {
                            if let ExplainContext::Plan(explain_ctx) = explain_ctx {
                                let (_, df_meta) = global_lir_plan.unapply();
                                CreateMaterializedViewStage::Explain(
                                    CreateMaterializedViewExplain {
                                        validity,
                                        global_id,
                                        plan,
                                        df_meta,
                                        explain_ctx,
                                    },
                                )
                            } else {
                                CreateMaterializedViewStage::Finish(CreateMaterializedViewFinish {
                                    item_id,
                                    global_id,
                                    validity,
                                    plan,
                                    resolved_ids,
                                    local_mir_plan,
                                    global_mir_plan,
                                    global_lir_plan,
                                    optimizer_features,
                                })
                            }
                        }
                        // Internal optimizer errors are handled differently
                        // depending on the caller.
                        Err(err) => {
                            let ExplainContext::Plan(explain_ctx) = explain_ctx else {
                                // In `sequence_~` contexts, immediately return the error.
                                return Err(err);
                            };

                            if explain_ctx.broken {
                                // In `EXPLAIN BROKEN` contexts, just log the error
                                // and move to the next stage with default
                                // parameters.
                                tracing::error!("error while handling EXPLAIN statement: {}", err);
                                CreateMaterializedViewStage::Explain(
                                    CreateMaterializedViewExplain {
                                        global_id,
                                        validity,
                                        plan,
                                        df_meta: Default::default(),
                                        explain_ctx,
                                    },
                                )
                            } else {
                                // In regular `EXPLAIN` contexts, immediately return the error.
                                return Err(err);
                            }
                        }
                    };

                    Ok(Box::new(stage))
                })
            },
        )))
    }

    #[instrument]
    async fn create_materialized_view_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        stage: CreateMaterializedViewFinish,
    ) -> Result<StageResult<Box<CreateMaterializedViewStage>>, AdapterError> {
        let CreateMaterializedViewFinish {
            item_id,
            global_id,
            plan:
                plan::CreateMaterializedViewPlan {
                    name,
                    materialized_view:
                        plan::MaterializedView {
                            mut create_sql,
                            query_ids,
                            expr: raw_expr,
                            column_names,
                            dependencies,
                            replacement_target,
                            cluster_id,
                            target_replica,
                            non_null_assertions,
                            compaction_window,
                            refresh_schedule,
                            ..
                        },
                    drop_ids,
                    if_not_exists,
                    ..
                },
            resolved_ids,
            local_mir_plan,
            mut global_mir_plan,
            mut global_lir_plan,
            optimizer_features,
            ..
        } = stage;

        // Validate the replacement target, if one is given.
        if let Some(target_id) = replacement_target {
            let Some(target) = self.catalog().get_entry(&target_id).materialized_view() else {
                return Err(AdapterError::internal(
                    "create materialized view",
                    "replacement target not a materialized view",
                ));
            };

            // For now, we don't support schema evolution for materialized views.
            let schema_diff = target.desc.latest().diff(global_lir_plan.desc());
            if !schema_diff.is_empty() {
                return Err(AdapterError::ReplacementSchemaMismatch(schema_diff));
            }
        }

        // Timestamp selection
        let id_bundle = dataflow_import_id_bundle(global_lir_plan.df_desc(), cluster_id);
        let logical_inputs = self.materialized_view_logical_inputs(
            query_ids
                .collections()
                .copied()
                .chain(raw_expr.depends_on()),
        )?;
        // Admission promises logical input history, not the availability of a
        // candidate index. Physical paths are selected at that timestamp below.
        let id_bundle = if self.catalog().state().catalog_read_protection_enabled() {
            logical_inputs.clone()
        } else {
            id_bundle
        };

        let read_holds = if let Some(txn_reads) = self.txn_read_holds.get(ctx.session().conn_id()) {
            // In some cases, for example when REFRESH is used, the preparatory
            // stages will already have acquired ReadHolds, we can re-use those.

            txn_reads.clone()
        } else {
            // No one has acquired holds, make sure we can determine an as_of
            // and commit a readable creation frontier.
            self.acquire_query_read_holds(&id_bundle).await?
        };

        // Reuse purification's holds, whose timestamps may already be named by
        // REFRESH AT. Planning can introduce reads absent from name resolution.
        let mut additional_inputs = id_bundle.clone();
        additional_inputs.extend(&logical_inputs);
        let additional_read_holds = self
            .acquire_query_read_holds(&additional_inputs.difference(&read_holds.id_bundle()))
            .await?;
        let (dataflow_as_of, storage_as_of, until) = self
            .select_timestamps(
                id_bundle,
                refresh_schedule.as_ref(),
                &read_holds,
                &additional_read_holds,
                &logical_inputs,
            )
            .await?;

        tracing::info!(
            dataflow_as_of = ?dataflow_as_of,
            storage_as_of = ?storage_as_of,
            until = ?until,
            "materialized view timestamp selection",
        );

        let initial_as_of = storage_as_of.clone();

        // Update the `create_sql` with the selected `as_of`. This is how we make sure the `as_of`
        // is persisted to the catalog and can be relied on during bootstrapping.
        // This has to be the `storage_as_of`, because bootstrapping uses this in
        // `bootstrap_storage_collections`.
        if let Some(storage_as_of_ts) = storage_as_of.as_option() {
            let stmt = mz_sql::parse::parse(&create_sql)
                .map_err(|_| {
                    AdapterError::internal(
                        "create materialized view",
                        "original SQL should roundtrip",
                    )
                })?
                .into_element()
                .ast;
            let ast::Statement::CreateMaterializedView(mut stmt) = stmt else {
                panic!("unexpected statement type");
            };
            stmt.as_of = Some(storage_as_of_ts.into());
            create_sql = stmt.to_ast_string_stable();
        }

        let desc = VersionedRelationDesc::new(global_lir_plan.desc().clone());
        let collections = [(RelationVersion::root(), global_id)].into_iter().collect();

        let local_mir_for_cache = local_mir_plan.expr();

        let mut ops = vec![
            catalog::Op::DropObjects(
                drop_ids
                    .into_iter()
                    .map(catalog::DropObjectInfo::Item)
                    .collect(),
            ),
            catalog::Op::CreateItem {
                id: item_id,
                name: name.clone(),
                item: CatalogItem::MaterializedView(MaterializedView {
                    create_sql,
                    raw_expr: raw_expr.into(),
                    locally_optimized_expr: local_mir_plan.expr().into(),
                    desc,
                    collections,
                    resolved_ids,
                    query_ids,
                    dependencies,
                    replacement_target,
                    cluster_id,
                    target_replica,
                    non_null_assertions,
                    custom_logical_compaction_window: compaction_window,
                    refresh_schedule: refresh_schedule.clone(),
                    initial_as_of: Some(initial_as_of.clone()),
                    optimized_plan: None,
                    physical_plan: None,
                    dataflow_metainfo: None,
                }),
                owner_id: *ctx.session().current_role_id(),
            },
        ];
        if self.catalog().state().catalog_read_protection_enabled() {
            ops.push(catalog::Op::SetReadProtection {
                requirements: vec![MaintainedReadRequirement {
                    id: global_id,
                    inputs: logical_inputs.storage_ids,
                    frontier: dataflow_as_of.as_option().copied(),
                }],
                bounds: vec![],
            });
        }

        // Physical protection bridges writing the candidate and installation's
        // acquisition of execution holds in catalog implications. Logical holds
        // alone cannot preserve an index trace at the chosen historical AS OF.
        let physical_read_holds = if let Some(client) = self.query_client.clone() {
            let planning_revision = self.catalog().transient_revision();
            let (candidate, _) = match self
                .catalog()
                .transact_incremental_dry_run(
                    self.catalog().state(),
                    ops.clone(),
                    None,
                    None,
                    initial_as_of.as_option().copied().unwrap_or(Timestamp::MIN),
                )
                .await
            {
                Ok(candidate) => candidate,
                Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                    kind: ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
                })) if if_not_exists => {
                    ctx.session()
                        .add_notice(AdapterNotice::ObjectAlreadyExists {
                            name: name.item,
                            ty: "materialized view",
                        });
                    return Ok(StageResult::Response(
                        ExecuteResponse::CreatedMaterializedView,
                    ));
                }
                Err(error) => return Err(error),
            };
            let candidate = Arc::new(candidate);
            let mv = candidate
                .get_entry(&item_id)
                .materialized_view()
                .expect("created MV");
            let read_ts = *dataflow_as_of.as_option().expect("readable MV timestamp");
            let replicas = client.replica_clients(cluster_id, target_replica);
            // A catalog declaration or bound does not establish that an index
            // trace exists at this timestamp. Missing native observations make
            // that path ineligible, not the logical CREATE inadmissible.
            let mut indexes: BTreeSet<_> = candidate
                .get_entries()
                .filter_map(|(_, entry)| {
                    let CatalogItem::Index(index) = entry.item() else {
                        return None;
                    };
                    if index.cluster_id != cluster_id {
                        return None;
                    }
                    let id = index.global_id();
                    if candidate
                        .collection_compaction_bounds()
                        .get(&id)
                        .is_some_and(|bound| !bound.less_equal(&read_ts))
                    {
                        return None;
                    }
                    // Maintained dataflows install on every selected replica, not
                    // just one readable replica as a peek can.
                    (!replicas.is_empty()
                        && replicas.iter().all(|replica| {
                            replica
                                .collection_frontiers(id)
                                .ok()
                                .flatten()
                                .and_then(|frontiers| frontiers.read_frontier)
                                .is_some_and(|since| since.less_equal(&read_ts))
                        }))
                    .then_some(id)
                })
                .collect();
            let mut optimizer_config = optimize::OptimizerConfig::from(candidate.system_config());
            // Keep the features selected for this statement, including session
            // and cluster overrides, when only its access paths change.
            optimizer_config.features = optimizer_features.clone();
            loop {
                ensure_materialized_view_access_paths(
                    &indexes,
                    &local_mir_plan,
                    &mut global_mir_plan,
                    &mut global_lir_plan,
                    || {
                        let (_, view_id) = self.allocate_transient_id();
                        optimize::materialized_view::Optimizer::new(
                            Arc::<CatalogState>::clone(&candidate),
                            ComputeInstanceSnapshot::new_from_parts(cluster_id, indexes.clone()),
                            global_id,
                            view_id,
                            column_names.clone(),
                            mv.non_null_assertions.clone(),
                            refresh_schedule.clone(),
                            candidate.resolve_full_name(&name, None).to_string(),
                            optimizer_config.clone(),
                            self.optimizer_metrics(),
                        )
                    },
                )?;
                let bundle = dataflow_import_id_bundle(global_lir_plan.df_desc(), cluster_id);
                let prepared = client
                    .prepare_read(self.catalog(), &bundle, |_| Ok(Some(read_ts)))
                    .await?;
                let incompatible: BTreeSet<_> = prepared
                    .frontiers
                    .iter()
                    .filter_map(|(id, since)| (*since > read_ts).then_some(*id))
                    .collect();
                if !incompatible.is_empty() {
                    if incompatible.iter().any(|id| !indexes.contains(id)) {
                        return Err(AdapterError::internal(
                            "create materialized view",
                            "logical input protection does not cover the written plan",
                        ));
                    }
                    indexes.retain(|id| !incompatible.contains(id));
                    continue;
                }
                let (holds, _) = self
                    .acquire_client_read_protection(client.protection.incarnation(), bundle, |_| {
                        Ok(Some(read_ts))
                    })
                    .await?;
                if self.catalog().transient_revision() != planning_revision {
                    return Err(AdapterError::DDLTransactionRace);
                }
                if holds.least_valid_read().less_equal(&read_ts) {
                    break Some(holds);
                }
                // Acquisition resamples native frontiers and permission. If
                // either advanced, reselect paths without advancing AS OF.
                if holds
                    .storage_holds
                    .values()
                    .any(|hold| !hold.since().less_equal(&read_ts))
                {
                    return Err(AdapterError::internal(
                        "create materialized view",
                        "logical input protection does not cover the written plan",
                    ));
                }
                for ((_, id), hold) in &holds.compute_holds {
                    if !hold.since().less_equal(&read_ts) {
                        indexes.remove(id);
                    }
                }
            }
        } else {
            None
        };

        // Pre-allocate a vector of transient GlobalIds for each notice.
        let notice_ids = std::iter::repeat_with(|| self.allocate_transient_id())
            .map(|(_item_id, global_id)| global_id)
            .take(global_lir_plan.df_meta().optimizer_notices.len())
            .collect::<Vec<_>>();

        // Render optimizer notices before the catalog transaction. We wrap
        // the system-session humanizer with an `ExprHumanizerExt` so that
        // references to the to-be-created materialized view's own
        // `global_id` in the persisted notice text resolve to its intended
        // human-readable name.
        //
        // We keep `raw_df_meta` live so that on success we can emit its raw
        // notices to the user session (rendered against the user's
        // session-aware humanizer). We deliberately do NOT emit to the user
        // here, so that if the catalog transaction below fails the user
        // isn't shown confusing notices about an item that wasn't actually
        // created.
        let (df_desc, mut raw_df_meta) = global_lir_plan.unapply();
        let df_meta = {
            let system_catalog = self.catalog().for_system_session();
            let full_name = self.catalog().resolve_full_name(&name, None);
            let transient_items = btreemap! {
                global_id => TransientItem::new(
                    Some(full_name.into_parts()),
                    Some(column_names.iter().map(|c| c.to_string()).collect()),
                )
            };
            let humanizer = ExprHumanizerExt::new(transient_items, &system_catalog);
            CatalogState::render_notices_core(
                &humanizer,
                (self.catalog().config().now)(),
                &raw_df_meta,
                notice_ids,
                Some(global_id),
            )
        };

        // Write the plan before committing the object and its selection together.
        let selection = self
            .catalog()
            .prepare_item_plan(
                global_id,
                Some(local_mir_for_cache),
                global_mir_plan.df_desc().clone(),
                df_desc.clone(),
                df_meta.clone(),
                optimizer_features,
            )
            .await?;
        ops.extend(selection);

        let transact_result = self
            .catalog_transact_with_context(None, Some(ctx), ops)
            .await;
        drop(physical_read_holds);

        match transact_result {
            Ok(_) => {
                // Only emit optimizer notices to the user now that the
                // catalog transaction has succeeded. If the transaction had
                // failed, emitting notices would confuse the user with
                // information about an item that wasn't actually created.
                // A cache rejection may reflect an optimizer-only dependency dropped in this batch.
                raw_df_meta.optimizer_notices.retain(|notice| {
                    notice.dependencies().iter().all(|id| {
                        self.catalog().try_get_entry_by_global_id(id).is_some()
                    })
                });
                self.emit_raw_optimizer_notices_to_user(ctx, &raw_df_meta.optimizer_notices);
                Ok(ExecuteResponse::CreatedMaterializedView)
            }
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(
                        CatalogError::ItemAlreadyExists(_, _),
                    ),
            })) if if_not_exists => {
                ctx.session()
                    .add_notice(AdapterNotice::ObjectAlreadyExists {
                        name: name.item,
                        ty: "materialized view",
                    });
                Ok(ExecuteResponse::CreatedMaterializedView)
            }
            Err(err) => Err(err),
        }
        .map(StageResult::Response)
    }

    /// Select the initial `dataflow_as_of`, `storage_as_of`, and `until` frontiers for a
    /// materialized view.
    async fn select_timestamps(
        &self,
        id_bundle: CollectionIdBundle,
        refresh_schedule: Option<&RefreshSchedule>,
        read_holds: &ReadHolds,
        additional_read_holds: &ReadHolds,
        logical_inputs: &CollectionIdBundle,
    ) -> Result<
        (
            Antichain<mz_repr::Timestamp>,
            Antichain<mz_repr::Timestamp>,
            Antichain<mz_repr::Timestamp>,
        ),
        AdapterError,
    > {
        assert!(
            id_bundle
                .difference(&read_holds.id_bundle())
                .difference(&additional_read_holds.id_bundle())
                .is_empty(),
            "we must have read holds for all involved collections"
        );

        // For non-REFRESH MVs both the `dataflow_as_of` and the `storage_as_of` should be simply
        // `least_valid_read`.
        let mut least_valid_read = read_holds
            .least_valid_read()
            .join(&additional_read_holds.least_valid_read());
        // Physical compaction may lag permission. Admission cannot rely on
        // that extra history, even for inputs eliminated by optimization.
        least_valid_read.join_assign(
            &self.materialized_view_input_permission(logical_inputs.storage_ids.iter().copied())?,
        );
        let mut dataflow_as_of = least_valid_read.clone();
        let mut storage_as_of = least_valid_read.clone();

        // For MVs with non-trivial REFRESH schedules:
        // 1. it's important to set the `storage_as_of` to the first refresh. This is because we'd
        // like queries on the MV to block until the first refresh (rather than to show an empty
        // MV).
        // 2. We move the `dataflow_as_of` forward to the minimum of `greatest_available_read` and
        // the first refresh time. There is no point in processing the times before
        // `greatest_available_read`, because the first time for which results will be exposed is
        // the first refresh time. Also note that simply moving the `dataflow_as_of` forward to the
        // first refresh time would prevent warmup before the first refresh.
        if let Some(refresh_schedule) = &refresh_schedule {
            // Planning can introduce logical reads absent from name resolution.
            // Do not let rounding skip a requested refresh on those inputs.
            for refresh_at_ts in &refresh_schedule.ats {
                if !least_valid_read.less_equal(refresh_at_ts) {
                    return Err(AdapterError::InputNotReadableAtRefreshAtTime(
                        *refresh_at_ts,
                        least_valid_read,
                    ));
                }
            }
            if let Some(least_valid_read_ts) = least_valid_read.as_option() {
                if let Some(first_refresh_ts) =
                    refresh_schedule.round_up_timestamp(*least_valid_read_ts)
                {
                    storage_as_of = Antichain::from_elem(first_refresh_ts);
                    let greatest_available = if let Some(client) = self.query_client.as_ref() {
                        client
                            .write_frontier(self.catalog(), &id_bundle)
                            .await?
                            .iter()
                            .map(|time| time.step_back().unwrap_or(*time))
                            .collect()
                    } else {
                        self.greatest_available_read(&id_bundle)
                    };
                    dataflow_as_of.join_assign(&greatest_available.meet(&storage_as_of));
                } else {
                    let last_refresh = refresh_schedule.last_refresh().expect(
                        "if round_up_timestamp returned None, then there should be a last refresh",
                    );

                    return Err(AdapterError::MaterializedViewWouldNeverRefresh(
                        last_refresh,
                        *least_valid_read_ts,
                    ));
                }
            } else {
                // The `as_of` should never be empty, because then the MV would be unreadable.
                soft_panic_or_log!("creating a materialized view with an empty `as_of`");
            }
        }

        // If we have a refresh schedule that has a last refresh, then set the `until` to the last refresh.
        // (If the `try_step_forward` fails, then no need to set an `until`, because it's not possible to get any data
        // beyond that last refresh time, because there are no times beyond that time.)
        let until_ts = refresh_schedule
            .and_then(|s| s.last_refresh())
            .and_then(|r| r.try_step_forward());
        let until = Antichain::from_iter(until_ts);

        if self.catalog().state().catalog_read_protection_enabled() && storage_as_of.is_empty() {
            return Err(AdapterError::internal(
                "create materialized view",
                "no readable timestamp for materialized view inputs",
            ));
        }
        Ok((dataflow_as_of, storage_as_of, until))
    }

    #[instrument]
    async fn create_materialized_view_explain(
        &self,
        session: &Session,
        CreateMaterializedViewExplain {
            global_id,
            plan:
                plan::CreateMaterializedViewPlan {
                    name,
                    materialized_view:
                        plan::MaterializedView {
                            column_names,
                            cluster_id,
                            ..
                        },
                    ..
                },
            df_meta,
            explain_ctx:
                ExplainPlanContext {
                    config,
                    format,
                    stage,
                    optimizer_trace,
                    ..
                },
            ..
        }: CreateMaterializedViewExplain,
    ) -> Result<StageResult<Box<CreateMaterializedViewStage>>, AdapterError> {
        let session_catalog = self.catalog().for_session(session);
        let expr_humanizer = {
            let full_name = self.catalog().resolve_full_name(&name, None);
            let transient_items = btreemap! {
                global_id => TransientItem::new(
                    Some(full_name.into_parts()),
                    Some(column_names.iter().map(|c| c.to_string()).collect()),
                )
            };
            ExprHumanizerExt::new(transient_items, &session_catalog)
        };

        let target_cluster = self.catalog().get_cluster(cluster_id);

        let features = OptimizerFeatures::from(self.catalog().system_config())
            .override_from(&target_cluster.config.features())
            .override_from(&self.cluster_scoped_optimizer_overrides(cluster_id))
            .override_from(&config.features);

        let rows = optimizer_trace
            .into_rows(
                format,
                &config,
                &features,
                &expr_humanizer,
                None,
                Some(target_cluster),
                df_meta,
                stage,
                plan::ExplaineeStatementKind::CreateMaterializedView,
                None,
            )
            .await?;

        Ok(StageResult::Response(Self::send_immediate_rows(rows)))
    }

    pub(crate) async fn explain_pushdown_materialized_view(
        &mut self,
        ctx: ExecuteContext,
        item_id: CatalogItemId,
    ) {
        let CatalogItem::MaterializedView(mview) = self.catalog().get_entry(&item_id).item() else {
            unreachable!() // Asserted in `sequence_explain_pushdown`.
        };
        let gid = mview.global_id_writes();
        let mview = mview.clone();

        let Some(plan) = self.catalog().try_get_physical_plan(&gid).cloned() else {
            let msg = format!("cannot find plan for materialized view {item_id} in catalog");
            tracing::error!("{msg}");
            ctx.retire(Err(anyhow!("{msg}").into()));
            return;
        };

        // We don't have any way to "duplicate" the read hold of the actual collection, which we
        // obtain below... but the current implementation of read holds guarantees that the storage
        // holds we obtain here will not be any greater than the hold we actually want.
        //
        // We hold only the plan's storage imports, whose persist part stats the pushdown
        // explanation reads. We must not acquire holds on the plan's compute imports: an index
        // that the materialized view was planned against can be dropped while the view keeps
        // running, and a dropped index no longer has a compute collection to hold.
        let id_bundle = CollectionIdBundle {
            storage_ids: plan.source_imports.keys().copied().collect(),
            compute_ids: BTreeMap::new(),
        };
        let read_holds = match self.acquire_query_read_holds(&id_bundle).await {
            Ok(holds) => Some(holds),
            Err(error) => {
                ctx.retire(Err(error));
                return;
            }
        };

        let frontiers = self
            .controller
            .compute
            .collection_frontiers(gid, Some(mview.cluster_id))
            .expect("materialized view exists");

        let as_of = frontiers.read_frontier.to_owned();

        let until = mview
            .refresh_schedule
            .as_ref()
            .and_then(|s| s.last_refresh())
            .unwrap_or(mz_repr::Timestamp::MAX);

        let mz_now = match as_of.as_option() {
            Some(&as_of) => {
                ResultSpec::value_between(Datum::MzTimestamp(as_of), Datum::MzTimestamp(until))
            }
            None => ResultSpec::value_all(),
        };

        self.execute_explain_pushdown_with_read_holds(
            ctx,
            as_of,
            mz_now,
            read_holds,
            plan.source_imports
                .into_iter()
                .filter_map(|(id, import)| import.desc.arguments.operators.map(|mfp| (id, mfp))),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_catalog::SYSTEM_CONN_ID;
    use mz_ore::metrics::MetricsRegistry;
    use mz_sql::catalog::CatalogDatabase;
    use mz_sql::names::{ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier};
    use mz_sql::optimizer_metrics::OptimizerMetrics;
    use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;

    #[mz_ore::test(tokio::test)]
    async fn historical_materialized_view_access_paths() {
        catalog::Catalog::with_debug(|catalog| async move {
            let database = catalog.resolve_database(crate::session::DEFAULT_DATABASE_NAME).expect("default database");
            let database_spec = ResolvedDatabaseSpecifier::Id(database.id());
            let schema = catalog.resolve_schema_in_database(
                &database_spec, mz_sql::DEFAULT_SCHEMA, &SYSTEM_CONN_ID,
            ).expect("default schema");
            let qualifiers = ItemQualifiers { database_spec, schema_spec: schema.id.clone() };
            let prefix = format!("{}.{}", database.name, schema.name.schema);
            let birth = catalog.current_upper().await;
            let mut state = catalog.state().clone();
            let mut snapshot = None;
            let mut ids = BTreeMap::new();
            for (name, sql) in [
                ("input", format!("CREATE TABLE {prefix}.input (a int)")),
                ("v", format!("CREATE VIEW {prefix}.v AS SELECT * FROM {prefix}.input")),
                ("early", format!("CREATE INDEX early IN CLUSTER quickstart ON {prefix}.v (a)")),
                ("late", format!("CREATE INDEX late IN CLUSTER quickstart ON {prefix}.v (a)")),
                ("mv", format!("CREATE MATERIALIZED VIEW {prefix}.mv IN CLUSTER quickstart AS SELECT * FROM {prefix}.v")),
            ] {
                let (id, gid) = catalog.allocate_user_id_for_test().await.expect("allocate fixture IDs");
                let (item, _) = state.with_enable_for_item_parsing(|state| state.parse_item_inner(
                    gid, &sql, &BTreeMap::new(), None, false, None,
                    None, None,
                )).expect("parse fixture definition");
                ids.insert(name, (id, gid));
                let (next, next_snapshot) = catalog.transact_incremental_dry_run(
                    &state,
                    vec![catalog::Op::CreateItem {
                        id,
                        name: QualifiedItemName {
                            qualifiers: qualifiers.clone(),
                            item: name.into(),
                        },
                        item, owner_id: MZ_SYSTEM_ROLE_ID,
                    }],
                    None, snapshot, birth,
                ).await.expect("apply fixture definition");
                state = next;
                snapshot = Some(next_snapshot);
            }
            let state = Arc::new(state);
            let mv = state.get_entry(&ids["mv"].0).materialized_view().expect("fixture MV");
            let metrics = OptimizerMetrics::register_into(
                &MetricsRegistry::new(), std::time::Duration::ZERO,
            );
            let optimizer = |indexes: BTreeSet<_>| optimize::materialized_view::Optimizer::new(
                Arc::<CatalogState>::clone(&state),
                ComputeInstanceSnapshot::new_from_parts(mv.cluster_id, indexes),
                ids["mv"].1, GlobalId::Transient(1),
                mv.desc.latest().iter_names().cloned().collect(),
                mv.non_null_assertions.clone(), mv.refresh_schedule.clone(),
                "historical MV".into(), optimize::OptimizerConfig::from(state.system_config()), metrics.clone(),
            );
            // The initial optimization chose a trace that will be excluded at
            // the historical timestamp. Exercise the real global planner, not
            // hand-constructed MIR/LIR import maps.
            let late = BTreeSet::from([ids["late"].1]);
            let mut initial = optimizer(late.clone());
            let local = initial.optimize(mv.raw_expr.as_ref().clone()).expect("optimize local MIR");
            let mut mir = initial.optimize(local.clone()).expect("optimize global MIR");
            let mut lir = initial.optimize(mir.clone()).expect("optimize physical plan");
            assert_eq!(lir.df_desc().index_imports.keys().copied().collect::<BTreeSet<_>>(), late);
            ensure_materialized_view_access_paths(&late, &local, &mut mir, &mut lir, || panic!("compatible plan must be reused")).expect("reuse compatible plan");

            let early = BTreeSet::from([ids["early"].1]);
            ensure_materialized_view_access_paths(&early, &local, &mut mir, &mut lir, || optimizer(early.clone())).expect("select readable index");
            assert_eq!(lir.df_desc().index_imports.keys().copied().collect::<BTreeSet<_>>(), early);
            assert_eq!(mir.df_desc().index_imports.keys().copied().collect::<BTreeSet<_>>(), early);
            assert_eq!(
                lir.df_meta().index_usage_types.keys().copied().collect::<BTreeSet<_>>(),
                early,
            );

            // Pin the writer boundary too: the immutable candidate must contain
            // this optimization's MIR, LIR, and rendered metadata together.
            let written = mz_catalog::expr_cache::GlobalExpressions {
                global_mir: mir.df_desc().clone(),
                physical_plan: lir.df_desc().clone(),
                dataflow_metainfos: CatalogState::render_notices_core(
                    &state.for_system_session(), 0, lir.df_meta(),
                    (100u64..).map(GlobalId::Transient)
                        .take(lir.df_meta().optimizer_notices.len()).collect(),
                    Some(ids["mv"].1),
                ),
                optimizer_features: optimize::OptimizerConfig::from(state.system_config()).features,
                item_version: RelationVersion::root(),
            };
            let selections = catalog.write_plans(BTreeMap::from([(ids["mv"].1, written.clone())])).await.expect("write immutable plan");
            let [catalog::Op::SetWrittenPlan {
                revision: Some(revision), imports, ..
            }] = selections.as_slice() else {
                panic!("expected one immutable plan selection");
            };
            assert_eq!(imports, &early);
            let stored = catalog.read_written_plans(vec![(ids["mv"].1, *revision)]).await.expect("read immutable plan");
            assert_eq!(stored[&ids["mv"].1], written);

            ensure_materialized_view_access_paths(&BTreeSet::new(), &local, &mut mir, &mut lir, || optimizer(BTreeSet::new())).expect("fall back to logical inputs");
            assert!(lir.df_desc().index_imports.is_empty());
            assert!(mir.df_desc().index_imports.is_empty());
            assert!(lir.df_meta().index_usage_types.is_empty());
            assert!(lir.df_desc().source_imports.contains_key(&ids["input"].1));
            assert_eq!(lir.desc(), &mv.desc.latest());
        }).await;
    }
}
