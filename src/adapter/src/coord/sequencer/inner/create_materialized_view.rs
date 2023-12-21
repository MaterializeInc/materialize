// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_adapter_types::compaction::CompactionWindow;
use mz_catalog::memory::objects::{CatalogItem, MaterializedView};
use mz_expr::CollectionPlan;
use mz_ore::tracing::OpenTelemetryContext;
use mz_sql::catalog::CatalogError;
use mz_sql::names::{ObjectId, ResolvedIds};
use mz_sql::plan;
use mz_storage_client::controller::{CollectionDescription, DataSource, DataSourceOther};

use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{
    Coordinator, CreateMaterializedViewFinish, CreateMaterializedViewOptimize,
    CreateMaterializedViewStage, CreateMaterializedViewValidate, Message, PlanValidity,
};
use crate::error::AdapterError;
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::{catalog, AdapterNotice, ExecuteContext, TimestampProvider};

use crate::util::ResultExt;

impl Coordinator {
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_create_materialized_view(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CreateMaterializedViewPlan,
        resolved_ids: ResolvedIds,
    ) {
        self.sequence_create_materialized_view_stage(
            ctx,
            CreateMaterializedViewStage::Validate(CreateMaterializedViewValidate {
                plan,
                resolved_ids,
            }),
            OpenTelemetryContext::obtain(),
        )
        .await;
    }

    /// Processes as many peek stages as possible.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn sequence_create_materialized_view_stage(
        &mut self,
        mut ctx: ExecuteContext,
        mut stage: CreateMaterializedViewStage,
        otel_ctx: OpenTelemetryContext,
    ) {
        use CreateMaterializedViewStage::*;

        // Process the current stage and allow for processing the next.
        loop {
            // Always verify peek validity. This is cheap, and prevents programming errors
            // if we move any stages off thread.
            if let Some(validity) = stage.validity() {
                return_if_err!(validity.check(self.catalog()), ctx);
            }

            (ctx, stage) = match stage {
                Validate(stage) => {
                    let next = return_if_err!(
                        self.create_materialized_view_validate(ctx.session(), stage),
                        ctx
                    );
                    (ctx, CreateMaterializedViewStage::Optimize(next))
                }
                Optimize(stage) => {
                    self.create_materialized_view_optimize(ctx, stage, otel_ctx)
                        .await;
                    return;
                }
                Finish(stage) => {
                    let result = self.create_materialized_view_finish(&mut ctx, stage).await;
                    ctx.retire(result);
                    return;
                }
            }
        }
    }

    fn create_materialized_view_validate(
        &mut self,
        session: &Session,
        CreateMaterializedViewValidate { plan, resolved_ids }: CreateMaterializedViewValidate,
    ) -> Result<CreateMaterializedViewOptimize, AdapterError> {
        let plan::CreateMaterializedViewPlan {
            name,
            materialized_view:
                plan::MaterializedView {
                    expr, cluster_id, ..
                },
            ambiguous_columns,
            ..
        } = &plan;

        self.ensure_cluster_can_host_compute_item(name, *cluster_id)?;

        // Validate any references in the materialized view's expression. We do
        // this on the unoptimized plan to better reflect what the user typed.
        // We want to reject queries that depend on log sources, for example,
        // even if we can *technically* optimize that reference away.
        let expr_depends_on = expr.depends_on();
        self.validate_timeline_context(expr_depends_on.iter().cloned())?;
        self.validate_system_column_references(*ambiguous_columns, &expr_depends_on)?;
        // Materialized views are not allowed to depend on log sources, as replicas
        // are not producing the same definite collection for these.
        // TODO(teskje): Remove this check once arrangement-based log sources
        // are replaced with persist-based ones.
        let log_names = expr_depends_on
            .iter()
            .flat_map(|id| self.catalog().introspection_dependencies(*id))
            .map(|id| self.catalog().get_entry(&id).name().item.clone())
            .collect::<Vec<_>>();
        if !log_names.is_empty() {
            return Err(AdapterError::InvalidLogDependency {
                object_type: "materialized view".into(),
                log_names,
            });
        }

        let validity = PlanValidity {
            transient_revision: self.catalog().transient_revision(),
            dependency_ids: expr_depends_on.clone(),
            cluster_id: Some(*cluster_id),
            replica_id: None,
            role_metadata: session.role_metadata().clone(),
        };

        Ok(CreateMaterializedViewOptimize {
            validity,
            plan,
            resolved_ids,
        })
    }

    async fn create_materialized_view_optimize(
        &mut self,
        ctx: ExecuteContext,
        CreateMaterializedViewOptimize {
            validity,
            plan,
            resolved_ids,
        }: CreateMaterializedViewOptimize,
        otel_ctx: OpenTelemetryContext,
    ) {
        let plan::CreateMaterializedViewPlan {
            name,
            materialized_view:
                plan::MaterializedView {
                    column_names,
                    cluster_id,
                    non_null_assertions,
                    ..
                },
            ..
        } = &plan;

        // Generate data structures that can be moved to another task where we will perform possibly
        // expensive optimizations.
        let internal_cmd_tx = self.internal_cmd_tx.clone();

        // Collect optimizer parameters.
        let compute_instance = self
            .instance_snapshot(*cluster_id)
            .expect("compute instance does not exist");
        let id = return_if_err!(self.catalog_mut().allocate_user_id().await, ctx);
        let internal_view_id = return_if_err!(self.allocate_transient_id(), ctx);
        let debug_name = self.catalog().resolve_full_name(name, None).to_string();
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

        // Build an optimizer for this MATERIALIZED VIEW.
        let mut optimizer = optimize::materialized_view::Optimizer::new(
            self.owned_catalog(),
            compute_instance,
            id,
            internal_view_id,
            column_names.clone(),
            non_null_assertions.clone(),
            debug_name,
            optimizer_config,
        );

        let span = tracing::debug_span!("optimize create materielized view task");

        mz_ore::task::spawn_blocking(
            || "optimize create materielized view",
            move || {
                let _guard = span.enter();

                // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local and global)
                let raw_expr = plan.materialized_view.expr.clone();
                let local_mir_plan = return_if_err!(optimizer.optimize(raw_expr), ctx);
                let global_mir_plan =
                    return_if_err!(optimizer.optimize(local_mir_plan.clone()), ctx);
                // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                let global_lir_plan =
                    return_if_err!(optimizer.optimize(global_mir_plan.clone()), ctx);

                let stage = CreateMaterializedViewStage::Finish(CreateMaterializedViewFinish {
                    validity,
                    id,
                    plan,
                    resolved_ids,
                    local_mir_plan,
                    global_mir_plan,
                    global_lir_plan,
                });

                let _ = internal_cmd_tx.send(Message::CreateMaterializedViewStageReady {
                    ctx,
                    otel_ctx,
                    stage,
                });
            },
        );
    }

    async fn create_materialized_view_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        CreateMaterializedViewFinish {
            id,
            plan:
                plan::CreateMaterializedViewPlan {
                    name,
                    materialized_view:
                        plan::MaterializedView {
                            create_sql,
                            expr: raw_expr,
                            cluster_id,
                            non_null_assertions,
                            compaction_window,
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
        }: CreateMaterializedViewFinish,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut ops = Vec::new();
        ops.extend(
            drop_ids
                .into_iter()
                .map(|id| catalog::Op::DropObject(ObjectId::Item(id))),
        );
        ops.push(catalog::Op::CreateItem {
            id,
            oid: self.catalog_mut().allocate_oid()?,
            name: name.clone(),
            item: CatalogItem::MaterializedView(MaterializedView {
                create_sql,
                raw_expr,
                optimized_expr: local_mir_plan.expr(),
                desc: global_lir_plan.desc().clone(),
                resolved_ids,
                cluster_id,
                non_null_assertions,
                custom_logical_compaction_window: compaction_window,
            }),
            owner_id: *ctx.session().current_role_id(),
        });

        let transact_result = self
            .catalog_transact_with_side_effects(Some(ctx.session()), ops, |coord| async {
                // Save plan structures.
                coord
                    .catalog_mut()
                    .set_optimized_plan(id, global_mir_plan.df_desc().clone());
                coord
                    .catalog_mut()
                    .set_physical_plan(id, global_lir_plan.df_desc().clone());

                let output_desc = global_lir_plan.desc().clone();
                let (mut df_desc, df_meta) = global_lir_plan.unapply();

                // Timestamp selection
                let id_bundle = dataflow_import_id_bundle(&df_desc, cluster_id);
                let since = coord.least_valid_read(&id_bundle);
                df_desc.set_as_of(since.clone());

                // Emit notices.
                coord.emit_optimizer_notices(ctx.session(), &df_meta.optimizer_notices);

                // Notices rendering
                let df_meta = coord.catalog().render_notices(df_meta, Some(id));
                coord
                    .catalog_mut()
                    .set_dataflow_metainfo(id, df_meta.clone());

                // Announce the creation of the materialized view source.
                coord
                    .controller
                    .storage
                    .create_collections(
                        None,
                        vec![(
                            id,
                            CollectionDescription {
                                desc: output_desc,
                                data_source: DataSource::Other(DataSourceOther::Compute),
                                since: Some(since),
                                status_collection_id: None,
                            },
                        )],
                    )
                    .await
                    .unwrap_or_terminate("cannot fail to append");

                coord
                    .initialize_storage_read_policies(
                        vec![id],
                        compaction_window.unwrap_or(CompactionWindow::Default),
                    )
                    .await;

                if coord.catalog().state().system_config().enable_mz_notices() {
                    // Initialize a container for builtin table updates.
                    let mut builtin_table_updates =
                        Vec::with_capacity(df_meta.optimizer_notices.len());
                    // Collect optimization hint updates.
                    coord.catalog().pack_optimizer_notices(
                        &mut builtin_table_updates,
                        df_meta.optimizer_notices.iter(),
                        1,
                    );
                    // Write collected optimization hints to the builtin tables.
                    let builtin_updates_fut = coord
                        .builtin_table_update()
                        .execute(builtin_table_updates)
                        .await;

                    let ship_dataflow_fut = coord.ship_dataflow(df_desc, cluster_id);

                    let ((), ()) =
                        futures::future::join(builtin_updates_fut, ship_dataflow_fut).await;
                } else {
                    coord.ship_dataflow(df_desc, cluster_id).await;
                }
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
    }
}
