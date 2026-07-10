// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_catalog::memory::error::ErrorKind;
use mz_catalog::memory::objects::{CatalogItem, MetricSink};
use mz_ore::instrument;
use mz_repr::optimize::OverrideFrom;
use mz_sql::catalog::CatalogError;
use mz_sql::names::ResolvedIds;
use mz_sql::plan;
use mz_sql::session::metadata::SessionMetadata;
use tracing::Span;

use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{
    Coordinator, CreateMetricSinkFinish, CreateMetricSinkOptimize, CreateMetricSinkStage, Message,
    PlanValidity, StageResult, Staged,
};
use crate::error::AdapterError;
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::{AdapterNotice, ExecuteContext, catalog};

impl Staged for CreateMetricSinkStage {
    type Ctx = ExecuteContext;

    fn validity(&mut self) -> &mut PlanValidity {
        match self {
            Self::Optimize(stage) => &mut stage.validity,
            Self::Finish(stage) => &mut stage.validity,
        }
    }

    async fn stage(
        self,
        coord: &mut Coordinator,
        ctx: &mut ExecuteContext,
    ) -> Result<StageResult<Box<Self>>, AdapterError> {
        match self {
            CreateMetricSinkStage::Optimize(stage) => {
                coord.create_metric_sink_optimize(stage).await
            }
            CreateMetricSinkStage::Finish(stage) => {
                coord.create_metric_sink_finish(ctx, stage).await
            }
        }
    }

    fn message(self, ctx: ExecuteContext, span: Span) -> Message {
        Message::CreateMetricSinkStageReady {
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
    pub(crate) async fn sequence_create_metric_sink(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CreateMetricSinkPlan,
        resolved_ids: ResolvedIds,
    ) {
        let stage = return_if_err!(
            self.create_metric_sink_validate(ctx.session(), plan, resolved_ids),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    fn create_metric_sink_validate(
        &self,
        session: &Session,
        plan: plan::CreateMetricSinkPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<CreateMetricSinkStage, AdapterError> {
        // Track the target cluster and resolved dependencies so concurrent drops are caught
        // between stages instead of panicking later when the dataflow is shipped.
        let validity = PlanValidity::new(
            self.catalog(),
            resolved_ids.items().copied().collect(),
            Some(plan.in_cluster),
            None,
            session.role_metadata().clone(),
        );
        Ok(CreateMetricSinkStage::Optimize(CreateMetricSinkOptimize {
            validity,
            plan,
            resolved_ids,
        }))
    }

    #[instrument]
    async fn create_metric_sink_optimize(
        &mut self,
        CreateMetricSinkOptimize {
            validity,
            plan,
            resolved_ids,
        }: CreateMetricSinkOptimize,
    ) -> Result<StageResult<Box<CreateMetricSinkStage>>, AdapterError> {
        let cluster_id = plan.in_cluster;

        // Collect optimizer parameters.
        let compute_instance = self
            .instance_snapshot(cluster_id)
            .expect("compute instance does not exist");
        let (item_id, global_id) = self.allocate_user_id().await?;
        // A transient id for the view the optimizer builds over `from` to shape its rows (see
        // `optimize::metric_sink::shape_metric_sink_source`); scoped to this dataflow, not durable.
        let (_, view_id) = self.allocate_transient_id();

        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&self.catalog.get_cluster(cluster_id).config.features())
            .override_from(&self.cluster_scoped_optimizer_overrides(cluster_id));

        // Build an optimizer for this METRIC SINK.
        let mut optimizer = optimize::metric_sink::Optimizer::new(
            self.owned_catalog(),
            compute_instance,
            view_id,
            global_id,
            optimizer_config,
            self.optimizer_metrics(),
        );
        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize create metric sink",
            move || {
                span.in_scope(|| {
                    let metric_sink = optimize::metric_sink::MetricSink::new(
                        plan.name.clone(),
                        plan.metric_sink.from,
                    );

                    // MIR ⇒ MIR optimization (global)
                    let global_mir_plan = optimizer.catch_unwind_optimize(metric_sink)?;
                    // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                    let global_lir_plan =
                        optimizer.catch_unwind_optimize(global_mir_plan.clone())?;

                    let stage = CreateMetricSinkStage::Finish(CreateMetricSinkFinish {
                        validity,
                        item_id,
                        global_id,
                        plan,
                        resolved_ids,
                        global_mir_plan,
                        global_lir_plan,
                    });
                    Ok(Box::new(stage))
                })
            },
        )))
    }

    #[instrument]
    async fn create_metric_sink_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        stage: CreateMetricSinkFinish,
    ) -> Result<StageResult<Box<CreateMetricSinkStage>>, AdapterError> {
        let CreateMetricSinkFinish {
            item_id,
            global_id,
            plan:
                plan::CreateMetricSinkPlan {
                    name,
                    metric_sink,
                    if_not_exists,
                    in_cluster,
                },
            resolved_ids,
            global_mir_plan,
            global_lir_plan,
            ..
        } = stage;
        let id_bundle = dataflow_import_id_bundle(global_lir_plan.df_desc(), in_cluster);

        let owner_id = *ctx.session().current_role_id();
        let ops = vec![catalog::Op::CreateItem {
            id: item_id,
            name: name.clone(),
            item: CatalogItem::MetricSink(MetricSink {
                create_sql: metric_sink.create_sql,
                global_id,
                from: metric_sink.from,
                resolved_ids,
                cluster_id: metric_sink.cluster_id,
                optimized_plan: None,
                physical_plan: None,
                dataflow_metainfo: None,
            }),
            owner_id,
        }];

        // Render optimizer notices before the catalog transaction: this way notice text resolves
        // the new sink's own `global_id` to its intended human-readable name rather than a bare
        // transient id.
        let (df_desc, raw_df_meta) = global_lir_plan.unapply();
        let from_entry = self.catalog().get_entry_by_global_id(&metric_sink.from);
        let from_desc = from_entry
            .relation_desc()
            .expect("can only create a metric sink on items with a valid description");
        let df_meta = self.render_create_item_notices(&name, global_id, &from_desc, &raw_df_meta);

        let transact_result = self
            .catalog_transact_with_side_effects(Some(ctx), ops, move |coord, _ctx| {
                Box::pin(async move {
                    // Save plan structures.
                    coord
                        .catalog_mut()
                        .set_optimized_plan(global_id, global_mir_plan.df_desc().clone());
                    coord
                        .catalog_mut()
                        .set_physical_plan(global_id, df_desc.clone());

                    let notice_builtin_updates_fut =
                        coord.persist_dataflow_metainfo(df_meta, global_id).await;

                    // `ship_new_dataflow` puts in place a read hold across shipping, so that
                    // update_read_capabilities can successfully act on `id_bundle`: otherwise the
                    // since of dependencies might move along concurrently, pulling the rug from
                    // under us.
                    coord
                        .ship_new_dataflow(
                            &id_bundle,
                            df_desc,
                            in_cluster,
                            notice_builtin_updates_fut,
                        )
                        .await;
                    // No `allow_writes` here: metric sinks write to the in-process metrics
                    // registry, not to external/persist state.
                })
            })
            .await;

        match transact_result {
            Ok(_) => {
                self.emit_raw_optimizer_notices_to_user(ctx, &raw_df_meta.optimizer_notices);
                Ok(StageResult::Response(ExecuteResponse::CreatedMetricSink))
            }
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind: ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                ctx.session()
                    .add_notice(AdapterNotice::ObjectAlreadyExists {
                        name: name.item,
                        ty: "metric sink",
                    });
                Ok(StageResult::Response(ExecuteResponse::CreatedMetricSink))
            }
            Err(err) => Err(err),
        }
    }
}
