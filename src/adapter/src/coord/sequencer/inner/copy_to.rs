// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::tracing::OpenTelemetryContext;
use mz_sql::plan::{self, QueryWhen};
use timely::progress::Antichain;
use tokio::sync::mpsc;

use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::{check_log_reads, return_if_err};
use crate::coord::{
    Coordinator, CopyToFinish, CopyToOptimizeLir, CopyToOptimizeMir, CopyToStage, CopyToTimestamp,
    CopyToValidate, Message, PlanValidity, TargetCluster,
};
use crate::error::AdapterError;
use crate::optimize::Optimize;
use crate::session::Session;
use crate::subscribe::ActiveSubscribe;
use crate::util::{ComputeSinkId, ResultExt};
use crate::{optimize, ExecuteContext, TimelineContext};

impl Coordinator {
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_copy_to(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CopyToPlan,
        target_cluster: TargetCluster,
    ) {
        self.sequence_copy_to_stage(
            ctx,
            CopyToStage::Validate(CopyToValidate {
                plan,
                target_cluster,
            }),
            OpenTelemetryContext::obtain(),
        )
        .await;
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn sequence_copy_to_stage(
        &mut self,
        mut ctx: ExecuteContext,
        mut stage: CopyToStage,
        otel_ctx: OpenTelemetryContext,
    ) {
        use CopyToStage::*;

        // Process the current stage and allow for processing the next.
        loop {
            // Always verify plan validity. This is cheap, and prevents programming errors
            // if we move any stages off thread.
            if let Some(validity) = stage.validity() {
                return_if_err!(validity.check(self.catalog()), ctx);
            }

            (ctx, stage) = match stage {
                Validate(stage) => {
                    let next = return_if_err!(self.copy_to_validate(ctx.session_mut(), stage), ctx);
                    (ctx, CopyToStage::OptimizeMir(next))
                }
                OptimizeMir(stage) => {
                    self.copy_to_optimize_mir(ctx, stage, otel_ctx);
                    return;
                }
                Timestamp(stage) => {
                    let next = return_if_err!(self.copy_to_timestamp(&mut ctx, stage).await, ctx);
                    (ctx, CopyToStage::OptimizeLir(next))
                }
                OptimizeLir(stage) => {
                    self.copy_to_optimize_lir(ctx, stage, otel_ctx);
                    return;
                }
                Finish(stage) => {
                    let result = self.copy_to_finish(&mut ctx, stage).await;
                    ctx.retire(result);
                    return;
                }
            }
        }
    }

    fn copy_to_validate(
        &mut self,
        session: &mut Session,
        CopyToValidate {
            plan,
            target_cluster,
        }: CopyToValidate,
    ) -> Result<CopyToOptimizeMir, AdapterError> {
        let plan::CopyToPlan { from, .. } = &plan;

        let cluster = self
            .catalog()
            .resolve_target_cluster(target_cluster, session)?;
        let cluster_id = cluster.id;

        let mut replica_id = session
            .vars()
            .cluster_replica()
            .map(|name| {
                cluster
                    .replica_id(name)
                    .ok_or(AdapterError::UnknownClusterReplica {
                        cluster_name: cluster.name.clone(),
                        replica_name: name.to_string(),
                    })
            })
            .transpose()?;

        let when = QueryWhen::Immediately;

        let depends_on = from.depends_on();

        // Run `check_log_reads` and emit notices.
        let notices = check_log_reads(
            self.catalog(),
            cluster,
            &depends_on,
            &mut replica_id,
            session.vars(),
        )?;
        session.add_notices(notices);

        // Determine timeline.
        let mut timeline = self.validate_timeline_context(depends_on.clone())?;
        if matches!(timeline, TimelineContext::TimestampIndependent) && from.contains_temporal() {
            // If the from IDs are timestamp independent but the query contains temporal functions
            // then the timeline context needs to be upgraded to timestamp dependent.
            timeline = TimelineContext::TimestampDependent;
        }

        let validity = PlanValidity {
            transient_revision: self.catalog().transient_revision(),
            dependency_ids: depends_on,
            cluster_id: Some(cluster_id),
            replica_id,
            role_metadata: session.role_metadata().clone(),
        };

        Ok(CopyToOptimizeMir {
            validity,
            plan,
            timeline,
        })
    }

    fn copy_to_optimize_mir(
        &mut self,
        mut ctx: ExecuteContext,
        CopyToOptimizeMir {
            validity,
            plan,
            timeline,
        }: CopyToOptimizeMir,
        otel_ctx: OpenTelemetryContext,
    ) {
        // Generate data structures that can be moved to another task where we will perform possibly
        // expensive optimizations.
        let internal_cmd_tx = self.internal_cmd_tx.clone();

        // Collect optimizer parameters.
        let compute_instance = self
            .instance_snapshot(validity.cluster_id.expect("cluser_id"))
            .expect("compute instance does not exist");
        let id = return_if_err!(self.allocate_transient_id(), ctx);
        let conn_id = ctx.session().conn_id().clone();
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

        // Build an optimizer for this COPY TO.
        let mut optimizer = optimize::copy_to::Optimizer::new(
            self.owned_catalog(),
            compute_instance,
            id,
            conn_id,
            optimizer_config,
        );

        let span = tracing::debug_span!("optimize subscribe task (mir)");

        mz_ore::task::spawn_blocking(
            || "optimize subscribe (mir)",
            move || {
                let _guard = span.enter();

                // MIR ⇒ MIR optimization (global)
                let global_mir_plan =
                    return_if_err!(optimizer.catch_unwind_optimize(plan.from.clone()), ctx);

                let stage = CopyToStage::Timestamp(CopyToTimestamp {
                    validity,
                    plan,
                    timeline,
                    optimizer,
                    global_mir_plan,
                });

                let _ = internal_cmd_tx.send(Message::SubscribeStageReady {
                    ctx,
                    otel_ctx,
                    stage,
                });
            },
        );
    }

    async fn copy_to_timestamp(
        &mut self,
        ctx: &mut ExecuteContext,
        CopyToTimestamp {
            validity,
            plan,
            timeline,
            optimizer,
            global_mir_plan,
        }: CopyToTimestamp,
    ) -> Result<CopyToOptimizeLir, AdapterError> {
        // Timestamp selection
        let oracle_read_ts = self.oracle_read_ts(&ctx.session, &timeline, when).await;
        let as_of = match self
            .determine_timestamp(
                ctx.session(),
                &global_mir_plan.id_bundle(optimizer.cluster_id()),
                &QueryWhen::Immediately,
                optimizer.cluster_id(),
                &timeline,
                oracle_read_ts,
                None,
            )
            .await
        {
            Ok(v) => v.timestamp_context.timestamp_or_default(),
            Err(e) => return Err(e),
        };
        if let Some(id) = ctx.extra().contents() {
            self.set_statement_execution_timestamp(id, as_of);
        }

        Ok(CopyToOptimizeLir {
            validity,
            plan,
            optimizer,
            global_mir_plan: global_mir_plan.resolve(Antichain::from_elem(as_of)),
        })
    }

    fn copy_to_optimize_lir(
        &mut self,
        ctx: ExecuteContext,
        CopyToOptimizeLir {
            validity,
            plan,
            mut optimizer,
            global_mir_plan,
        }: CopyToOptimizeLir,
        otel_ctx: OpenTelemetryContext,
    ) {
        // Generate data structures that can be moved to another task where we will perform possibly
        // expensive optimizations.
        let internal_cmd_tx = self.internal_cmd_tx.clone();

        let span = tracing::debug_span!("optimize subscribe task (lir)");

        mz_ore::task::spawn_blocking(
            || "optimize subscribe (lir)",
            move || {
                let _guard = span.enter();

                // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                let global_lir_plan = return_if_err!(
                    optimizer.catch_unwind_optimize(global_mir_plan.clone()),
                    ctx
                );

                let stage = CopyToStage::Finish(CopyToFinish {
                    validity,
                    cluster_id: optimizer.cluster_id(),
                    plan,
                    global_lir_plan,
                });

                let _ = internal_cmd_tx.send(Message::SubscribeStageReady {
                    ctx,
                    otel_ctx,
                    stage,
                });
            },
        );
    }

    async fn copy_to_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        CopyToFinish {
            validity,
            cluster_id,
            plan,
            global_lir_plan,
        }: CopyToFinish,
    ) -> Result<ExecuteResponse, AdapterError> {
        let sink_id = global_lir_plan.sink_id();

        todo!()
    }
}
