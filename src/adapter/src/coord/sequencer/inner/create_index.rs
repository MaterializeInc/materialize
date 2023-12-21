// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_catalog::memory::objects::{CatalogItem, Index};
use mz_ore::tracing::OpenTelemetryContext;
use mz_sql::catalog::CatalogError;
use mz_sql::names::ResolvedIds;
use mz_sql::plan;

use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{
    Coordinator, CreateIndexFinish, CreateIndexOptimize, CreateIndexStage, CreateIndexValidate,
    Message, PlanValidity,
};
use crate::error::AdapterError;
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::{catalog, AdapterNotice, ExecuteContext, TimestampProvider};

impl Coordinator {
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_create_index(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CreateIndexPlan,
        resolved_ids: ResolvedIds,
    ) {
        self.sequence_create_index_stage(
            ctx,
            CreateIndexStage::Validate(CreateIndexValidate { plan, resolved_ids }),
            OpenTelemetryContext::obtain(),
        )
        .await;
    }

    /// Processes as many `create index` stages as possible.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn sequence_create_index_stage(
        &mut self,
        mut ctx: ExecuteContext,
        mut stage: CreateIndexStage,
        otel_ctx: OpenTelemetryContext,
    ) {
        use CreateIndexStage::*;

        // Process the current stage and allow for processing the next.
        loop {
            // Always verify plan validity. This is cheap, and prevents programming errors
            // if we move any stages off thread.
            if let Some(validity) = stage.validity() {
                return_if_err!(validity.check(self.catalog()), ctx);
            }

            (ctx, stage) = match stage {
                Validate(stage) => {
                    let next =
                        return_if_err!(self.create_index_validate(ctx.session(), stage), ctx);
                    (ctx, CreateIndexStage::Optimize(next))
                }
                Optimize(stage) => {
                    self.create_index_optimize(ctx, stage, otel_ctx).await;
                    return;
                }
                Finish(stage) => {
                    let result = self.create_index_finish(&mut ctx, stage).await;
                    ctx.retire(result);
                    return;
                }
            }
        }
    }

    fn create_index_validate(
        &mut self,
        session: &Session,
        CreateIndexValidate { plan, resolved_ids }: CreateIndexValidate,
    ) -> Result<CreateIndexOptimize, AdapterError> {
        let plan::CreateIndexPlan {
            name,
            index: plan::Index { on, cluster_id, .. },
            ..
        } = &plan;

        self.ensure_cluster_can_host_compute_item(name, *cluster_id)?;

        let validity = PlanValidity {
            transient_revision: self.catalog().transient_revision(),
            dependency_ids: BTreeSet::from_iter(std::iter::once(*on)),
            cluster_id: Some(*cluster_id),
            replica_id: None,
            role_metadata: session.role_metadata().clone(),
        };

        Ok(CreateIndexOptimize {
            validity,
            plan,
            resolved_ids,
        })
    }

    async fn create_index_optimize(
        &mut self,
        ctx: ExecuteContext,
        CreateIndexOptimize {
            validity,
            plan,
            resolved_ids,
        }: CreateIndexOptimize,
        otel_ctx: OpenTelemetryContext,
    ) {
        let plan::CreateIndexPlan {
            index: plan::Index { cluster_id, .. },
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
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

        // Build an optimizer for this INDEX.
        let mut optimizer = optimize::index::Optimizer::new(
            self.owned_catalog(),
            compute_instance,
            id,
            optimizer_config,
        );

        let span = tracing::debug_span!("optimize create index task");

        mz_ore::task::spawn_blocking(
            || "optimize create index",
            move || {
                let _guard = span.enter();

                // MIR ⇒ MIR optimization (global)
                let index_plan =
                    optimize::index::Index::new(&plan.name, &plan.index.on, &plan.index.keys);
                let global_mir_plan = return_if_err!(optimizer.optimize(index_plan), ctx);
                // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                let global_lir_plan =
                    return_if_err!(optimizer.optimize(global_mir_plan.clone()), ctx);

                let stage = CreateIndexStage::Finish(CreateIndexFinish {
                    validity,
                    id,
                    plan,
                    resolved_ids,
                    global_mir_plan,
                    global_lir_plan,
                });

                let _ = internal_cmd_tx.send(Message::CreateIndexStageReady {
                    ctx,
                    otel_ctx,
                    stage,
                });
            },
        );
    }

    async fn create_index_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        CreateIndexFinish {
            id,
            plan:
                plan::CreateIndexPlan {
                    name,
                    index:
                        plan::Index {
                            create_sql,
                            on,
                            keys,
                            cluster_id,
                        },
                    options,
                    if_not_exists,
                },
            resolved_ids,
            global_mir_plan,
            global_lir_plan,
            ..
        }: CreateIndexFinish,
    ) -> Result<ExecuteResponse, AdapterError> {
        let ops = vec![catalog::Op::CreateItem {
            id,
            oid: self.catalog_mut().allocate_oid()?,
            name: name.clone(),
            item: CatalogItem::Index(Index {
                create_sql,
                keys,
                on,
                conn_id: None,
                resolved_ids,
                cluster_id,
                is_retained_metrics_object: false,
                custom_logical_compaction_window: None,
            }),
            owner_id: *self.catalog().get_entry(&on).owner_id(),
        }];

        let transact_result = self
            .catalog_transact_with_side_effects(Some(ctx.session()), ops, |coord| async {
                // Save plan structures.
                coord
                    .catalog_mut()
                    .set_optimized_plan(id, global_mir_plan.df_desc().clone());
                coord
                    .catalog_mut()
                    .set_physical_plan(id, global_lir_plan.df_desc().clone());

                let (mut df_desc, df_meta) = global_lir_plan.unapply();

                // Timestamp selection
                let id_bundle = dataflow_import_id_bundle(&df_desc, cluster_id);
                let since = coord.least_valid_read(&id_bundle);
                df_desc.set_as_of(since);

                // Emit notices.
                coord.emit_optimizer_notices(ctx.session(), &df_meta.optimizer_notices);

                // Notices rendering
                let df_meta = coord.catalog().render_notices(df_meta, Some(id));
                coord
                    .catalog_mut()
                    .set_dataflow_metainfo(id, df_meta.clone());

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

                    futures::future::join(builtin_updates_fut, ship_dataflow_fut).await;
                } else {
                    coord.ship_dataflow(df_desc, cluster_id).await;
                }

                coord.set_index_options(id, options).expect("index enabled");
            })
            .await;

        match transact_result {
            Ok(_) => Ok(ExecuteResponse::CreatedIndex),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                ctx.session()
                    .add_notice(AdapterNotice::ObjectAlreadyExists {
                        name: name.item,
                        ty: "index",
                    });
                Ok(ExecuteResponse::CreatedIndex)
            }
            Err(err) => Err(err),
        }
    }
}
