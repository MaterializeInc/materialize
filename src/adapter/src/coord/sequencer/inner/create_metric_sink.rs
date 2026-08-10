// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `CREATE METRIC SINK` sequencing.
//!
//! Staged like `CREATE INDEX`: optimization runs off the coordinator thread, then the finish stage
//! writes the durable catalog item and ships the dataflow inside one catalog transaction.

use anyhow::anyhow;
use mz_catalog::memory::error::ErrorKind;
use mz_catalog::memory::objects::{CatalogItem, MetricSink};
use mz_controller_types::ClusterId;
use mz_ore::instrument;
use mz_repr::optimize::OverrideFrom;
use mz_sql::catalog::CatalogError;
use mz_sql::names::{QualifiedItemName, ResolvedIds};
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
            Some(plan.metric_sink.cluster_id),
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
        let cluster_id = plan.metric_sink.cluster_id;

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
        let optimizer_features = optimizer_config.features.clone();
        let debug_name = self
            .catalog()
            .resolve_full_name(&plan.name, None)
            .to_string();

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
                        debug_name,
                        optimize::metric_sink::MetricSinkFrom::Id(plan.metric_sink.from),
                        plan.metric_sink.prefix.clone(),
                        None,
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
                        optimizer_features,
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
                },
            resolved_ids,
            global_mir_plan,
            global_lir_plan,
            optimizer_features,
            ..
        } = stage;
        let cluster_id = metric_sink.cluster_id;
        let id_bundle = dataflow_import_id_bundle(global_lir_plan.df_desc(), cluster_id);

        // Run the authoritative prefix-free check here in the finish stage, not in optimize:
        // optimize runs off the coordinator thread, so another sink could commit between the two
        // stages. See `ensure_metric_sink_prefix_is_free`.
        self.ensure_metric_sink_prefix_is_free(&name, cluster_id, &metric_sink.prefix)?;

        let owner_id = *ctx.session().current_role_id();
        let ops = vec![catalog::Op::CreateItem {
            id: item_id,
            name: name.clone(),
            item: CatalogItem::MetricSink(MetricSink {
                create_sql: metric_sink.create_sql,
                global_id,
                from: metric_sink.from,
                resolved_ids,
                cluster_id,
                prefix: metric_sink.prefix,
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

        // Populate the durable expression cache before the catalog transaction and await the
        // write. This way any other envd (or a subsequent bootstrap here) will observe the cached
        // plans + rendered notices as soon as the item becomes visible. Metric sinks have no local
        // MIR (the pipeline starts from a `GlobalId`), so there is no local expression to cache.
        self.catalog()
            .cache_expressions(
                global_id,
                None,
                global_mir_plan.df_desc().clone(),
                df_desc.clone(),
                df_meta.clone(),
                optimizer_features,
            )
            .await;

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
                        coord.persist_dataflow_metainfo(df_meta, global_id);

                    coord
                        .ship_new_dataflow(
                            &id_bundle,
                            df_desc,
                            cluster_id,
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

    /// Rejects `prefix` if it is a prefix of, or has as a prefix, any metric sink already on
    /// `cluster_id`.
    ///
    /// Prefix-free, not just distinct: the published name is `prefix + metric_name`, so `a_`
    /// + `b_c` and `a_b_` + `c` both publish `a_b_c`, and Prometheus silently merges same-named
    /// families. Uniqueness only holds per cluster: the registry is process-local and every
    /// replica of a cluster runs the same sinks.
    ///
    /// This is the authoritative check, not the plan-time one. Planning is not serialized
    /// against catalog writes, so two creates can plan against the same state. The coordinator
    /// sequences one statement at a time, and nothing commits between here and
    /// `catalog_transact`.
    ///
    /// A sink already holding `name` is skipped: the create is then a no-op (`IF NOT EXISTS`)
    /// or an "already exists" error, neither of which publishes anything new.
    fn ensure_metric_sink_prefix_is_free(
        &self,
        name: &QualifiedItemName,
        cluster_id: ClusterId,
        prefix: &str,
    ) -> Result<(), AdapterError> {
        let cluster = self.catalog().get_cluster(cluster_id);
        for item_id in &cluster.bound_objects {
            let entry = self.catalog().get_entry(item_id);
            let CatalogItem::MetricSink(existing) = entry.item() else {
                continue;
            };
            if entry.name() == name {
                continue;
            }
            if existing.prefix.starts_with(prefix) || prefix.starts_with(&existing.prefix) {
                return Err(AdapterError::Unstructured(anyhow!(
                    "metric sink prefix {:?} conflicts with prefix {:?} of metric sink {} on \
                     cluster {}",
                    prefix,
                    existing.prefix,
                    entry.name().item,
                    cluster.name,
                )));
            }
        }
        Ok(())
    }
}
