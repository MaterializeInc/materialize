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
use maplit::btreeset;
use mz_adapter_types::compaction::CompactionWindow;
use mz_catalog::memory::objects::{CatalogItem, MaterializedView};
use mz_expr::{CollectionPlan, ResultSpec};
use mz_ore::collections::CollectionExt;
use mz_ore::instrument;
use mz_ore::soft_panic_or_log;
use mz_repr::explain::{ExprHumanizerExt, TransientItem};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::optimize::OverrideFrom;
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{CatalogItemId, Datum, RelationVersion, Row, VersionedRelationDesc};
use mz_sql::ast::ExplainStage;
use mz_sql::catalog::CatalogError;
use mz_sql::names::ResolvedIds;
use mz_sql::plan;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql_parser::ast;
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage_client::controller::CollectionDescription;
use std::collections::BTreeMap;
use timely::progress::Antichain;
use tracing::Span;

use crate::ReadHolds;
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
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::util::ResultExt;
use crate::{AdapterNotice, CollectionIdBundle, ExecuteContext, TimestampProvider, catalog};

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
                view.optimized_expr.as_inner().clone(),
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
                    cluster_id,
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

        let validity =
            PlanValidity::require_transient_revision(self.catalog().transient_revision());

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
                // Also check that no new id has appeared in `sufficient_collections` (e.g. a new
                // index), otherwise we might be missing some read holds.
                let ids = self
                    .index_oracle(*cluster_id)
                    .sufficient_collections(resolved_ids.collections().copied());
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
            .instance_snapshot(*cluster_id)
            .expect("compute instance does not exist");
        let (item_id, global_id) = if let ExplainContext::None = explain_ctx {
            let id_ts = self.get_catalog_write_ts().await;
            self.catalog().allocate_user_id(id_ts).await?
        } else {
            self.allocate_transient_id()
        };

        let (_, view_id) = self.allocate_transient_id();
        let debug_name = self.catalog().resolve_full_name(name, None).to_string();
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&self.catalog.get_cluster(*cluster_id).config.features())
            .override_from(&explain_ctx);
        let force_non_monotonic = Default::default();

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
            force_non_monotonic,
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
                        let global_mir_plan = optimizer.catch_unwind_optimize(local_mir_plan.clone())?;
                        // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                        let global_lir_plan = optimizer.catch_unwind_optimize(global_mir_plan.clone())?;

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
                            expr: raw_expr,
                            dependencies,
                            replacement_target,
                            cluster_id,
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
            global_mir_plan,
            global_lir_plan,
            ..
        } = stage;

        // Validate the replacement target, if one is given.
        // TODO(alter-mv): Could we do this already in planning?
        if let Some(target_id) = replacement_target {
            let Some(target) = self.catalog().get_entry(&target_id).materialized_view() else {
                return Err(AdapterError::internal(
                    "create materialized view",
                    "replacement target not a materialized view",
                ));
            };

            // For now, we don't support schema evolution for materialized views.
            if &target.desc.latest() != global_lir_plan.desc() {
                return Err(AdapterError::Unstructured(anyhow!("incompatible schemas")));
            }
        }

        // Timestamp selection
        let id_bundle = dataflow_import_id_bundle(global_lir_plan.df_desc(), cluster_id);

        let read_holds_owned;
        let read_holds = if let Some(txn_reads) = self.txn_read_holds.get(ctx.session().conn_id()) {
            // In some cases, for example when REFRESH is used, the preparatory
            // stages will already have acquired ReadHolds, we can re-use those.

            txn_reads
        } else {
            // No one has acquired holds, make sure we can determine an as_of
            // and render our dataflow below.
            read_holds_owned = self.acquire_read_holds(&id_bundle);
            &read_holds_owned
        };

        let (dataflow_as_of, storage_as_of, until) =
            self.select_timestamps(id_bundle, refresh_schedule.as_ref(), read_holds)?;

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

        let ops = vec![
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
                    optimized_expr: local_mir_plan.expr().into(),
                    desc,
                    collections,
                    resolved_ids,
                    dependencies,
                    replacement_target,
                    cluster_id,
                    non_null_assertions,
                    custom_logical_compaction_window: compaction_window,
                    refresh_schedule: refresh_schedule.clone(),
                    initial_as_of: Some(initial_as_of.clone()),
                }),
                owner_id: *ctx.session().current_role_id(),
            },
        ];

        // Pre-allocate a vector of transient GlobalIds for each notice.
        let notice_ids = std::iter::repeat_with(|| self.allocate_transient_id())
            .map(|(_item_id, global_id)| global_id)
            .take(global_lir_plan.df_meta().optimizer_notices.len())
            .collect::<Vec<_>>();

        let transact_result = self
            .catalog_transact_with_side_effects(Some(ctx), ops, move |coord, ctx| {
                Box::pin(async move {
                    let output_desc = global_lir_plan.desc().clone();
                    let (mut df_desc, df_meta) = global_lir_plan.unapply();

                    // Save plan structures.
                    coord
                        .catalog_mut()
                        .set_optimized_plan(global_id, global_mir_plan.df_desc().clone());
                    coord
                        .catalog_mut()
                        .set_physical_plan(global_id, df_desc.clone());

                    let notice_builtin_updates_fut = coord
                        .process_dataflow_metainfo(df_meta, global_id, ctx, notice_ids)
                        .await;

                    df_desc.set_as_of(dataflow_as_of.clone());
                    df_desc.set_initial_as_of(initial_as_of);
                    df_desc.until = until;

                    let storage_metadata = coord.catalog.state().storage_metadata();

                    let mut collection_desc =
                        CollectionDescription::for_other(output_desc, Some(storage_as_of));
                    let mut allow_writes = true;

                    // If this MV is intended to replace another one, we need to start it in
                    // read-only mode, targeting the shard of the replacement target.
                    if let Some(target_id) = replacement_target {
                        let target_gid = coord.catalog.get_entry(&target_id).latest_global_id();
                        collection_desc.primary = Some(target_gid);
                        allow_writes = false;
                    }

                    // Announce the creation of the materialized view source.
                    coord
                        .controller
                        .storage
                        .create_collections(
                            storage_metadata,
                            None,
                            vec![(global_id, collection_desc)],
                        )
                        .await
                        .unwrap_or_terminate("cannot fail to append");

                    coord
                        .initialize_storage_read_policies(
                            btreeset![item_id],
                            compaction_window.unwrap_or(CompactionWindow::Default),
                        )
                        .await;

                    coord
                        .ship_dataflow_and_notice_builtin_table_updates(
                            df_desc,
                            cluster_id,
                            notice_builtin_updates_fut,
                        )
                        .await;

                    if allow_writes {
                        coord.allow_writes(cluster_id, global_id);
                    }
                })
            })
            .await;

        match transact_result {
            Ok(_) => Ok(ExecuteResponse::CreatedMaterializedView),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
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
    fn select_timestamps(
        &self,
        id_bundle: CollectionIdBundle,
        refresh_schedule: Option<&RefreshSchedule>,
        read_holds: &ReadHolds<mz_repr::Timestamp>,
    ) -> Result<
        (
            Antichain<mz_repr::Timestamp>,
            Antichain<mz_repr::Timestamp>,
            Antichain<mz_repr::Timestamp>,
        ),
        AdapterError,
    > {
        assert!(
            id_bundle.difference(&read_holds.id_bundle()).is_empty(),
            "we must have read holds for all involved collections"
        );

        // For non-REFRESH MVs both the `dataflow_as_of` and the `storage_as_of` should be simply
        // `least_valid_read`.
        let least_valid_read = read_holds.least_valid_read();
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
            if let Some(least_valid_read_ts) = least_valid_read.as_option() {
                if let Some(first_refresh_ts) =
                    refresh_schedule.round_up_timestamp(*least_valid_read_ts)
                {
                    storage_as_of = Antichain::from_elem(first_refresh_ts);
                    dataflow_as_of.join_assign(
                        &self
                            .greatest_available_read(&id_bundle)
                            .meet(&storage_as_of),
                    );
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
        &self,
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
        let read_holds =
            Some(self.acquire_read_holds(&dataflow_import_id_bundle(&plan, mview.cluster_id)));

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
                .filter_map(|(id, (source, _, _upper))| {
                    source.arguments.operators.map(|mfp| (id, mfp))
                }),
        )
        .await
    }
}
