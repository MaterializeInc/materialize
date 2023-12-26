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
    Coordinator, Message, PlanValidity, SubscribeFinish, SubscribeOptimizeLir,
    SubscribeOptimizeMir, SubscribeStage, SubscribeTimestamp, SubscribeValidate, TargetCluster,
};
use crate::error::AdapterError;
use crate::optimize::Optimize;
use crate::session::{Session, TransactionOps};
use crate::subscribe::ActiveSubscribe;
use crate::util::{ComputeSinkId, ResultExt};
use crate::{optimize, AdapterNotice, ExecuteContext, TimelineContext};

impl Coordinator {
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_subscribe(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::SubscribePlan,
        target_cluster: TargetCluster,
    ) {
        self.sequence_subscribe_stage(
            ctx,
            SubscribeStage::Validate(SubscribeValidate {
                plan,
                target_cluster,
            }),
            OpenTelemetryContext::obtain(),
        )
        .await;
    }

    /// Processes as many `subscribe` stages as possible.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn sequence_subscribe_stage(
        &mut self,
        mut ctx: ExecuteContext,
        mut stage: SubscribeStage,
        otel_ctx: OpenTelemetryContext,
    ) {
        use SubscribeStage::*;

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
                        return_if_err!(self.subscribe_validate(ctx.session_mut(), stage), ctx);
                    (ctx, SubscribeStage::OptimizeMir(next))
                }
                OptimizeMir(stage) => {
                    self.subscribe_optimize_mir(ctx, stage, otel_ctx);
                    return;
                }
                Timestamp(stage) => {
                    let next = return_if_err!(self.subscribe_timestamp(&mut ctx, stage).await, ctx);
                    (ctx, SubscribeStage::OptimizeLir(next))
                }
                OptimizeLir(stage) => {
                    self.subscribe_optimize_lir(ctx, stage, otel_ctx);
                    return;
                }
                Finish(stage) => {
                    let result = self.subscribe_finish(&mut ctx, stage).await;
                    ctx.retire(result);
                    return;
                }
            }
        }
    }

    fn subscribe_validate(
        &mut self,
        session: &mut Session,
        SubscribeValidate {
            plan,
            target_cluster,
        }: SubscribeValidate,
    ) -> Result<SubscribeOptimizeMir, AdapterError> {
        let plan::SubscribePlan { from, when, .. } = &plan;

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

        // SUBSCRIBE AS OF, similar to peeks, doesn't need to worry about transaction
        // timestamp semantics.
        if when == &QueryWhen::Immediately {
            // If this isn't a SUBSCRIBE AS OF, the SUBSCRIBE can be in a transaction if it's the
            // only operation.
            session.add_transaction_ops(TransactionOps::Subscribe)?;
        }

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

        Ok(SubscribeOptimizeMir {
            validity,
            plan,
            timeline,
        })
    }

    fn subscribe_optimize_mir(
        &mut self,
        mut ctx: ExecuteContext,
        SubscribeOptimizeMir {
            validity,
            plan,
            timeline,
        }: SubscribeOptimizeMir,
        otel_ctx: OpenTelemetryContext,
    ) {
        let plan::SubscribePlan {
            with_snapshot,
            up_to,
            ..
        } = &plan;

        // Generate data structures that can be moved to another task where we will perform possibly
        // expensive optimizations.
        let internal_cmd_tx = self.internal_cmd_tx.clone();

        // Collect optimizer parameters.
        let compute_instance = self
            .instance_snapshot(validity.cluster_id.expect("cluser_id"))
            .expect("compute instance does not exist");
        let id = return_if_err!(self.allocate_transient_id(), ctx);
        let conn_id = ctx.session().conn_id().clone();
        let up_to = return_if_err!(
            up_to
                .as_ref()
                .map(|expr| Coordinator::evaluate_when(
                    self.catalog().state(),
                    expr.clone(),
                    ctx.session_mut()
                ))
                .transpose(),
            ctx
        );
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

        // Build an optimizer for this SUBSCRIBE.
        let mut optimizer = optimize::subscribe::Optimizer::new(
            self.owned_catalog(),
            compute_instance,
            id,
            conn_id,
            *with_snapshot,
            up_to,
            optimizer_config,
        );

        let span = tracing::debug_span!("optimize subscribe task (mir)");

        mz_ore::task::spawn_blocking(
            || "optimize subscribe (mir)",
            move || {
                let _guard = span.enter();

                // MIR ⇒ MIR optimization (global)
                let global_mir_plan = return_if_err!(optimizer.optimize(plan.from.clone()), ctx);

                let stage = SubscribeStage::Timestamp(SubscribeTimestamp {
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

    async fn subscribe_timestamp(
        &mut self,
        ctx: &mut ExecuteContext,
        SubscribeTimestamp {
            validity,
            plan,
            timeline,
            optimizer,
            global_mir_plan,
        }: SubscribeTimestamp,
    ) -> Result<SubscribeOptimizeLir, AdapterError> {
        let plan::SubscribePlan { when, .. } = &plan;

        // Timestamp selection
        let oracle_read_ts = self.oracle_read_ts(&ctx.session, &timeline, when).await;
        let as_of = match self
            .determine_timestamp(
                ctx.session(),
                &global_mir_plan.id_bundle(optimizer.cluster_id()),
                when,
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
        if let Some(up_to) = optimizer.up_to() {
            if as_of == up_to {
                ctx.session_mut()
                    .add_notice(AdapterNotice::EqualSubscribeBounds { bound: up_to });
            } else if as_of > up_to {
                return Err(AdapterError::AbsurdSubscribeBounds { as_of, up_to });
            }
        }

        Ok(SubscribeOptimizeLir {
            validity,
            plan,
            optimizer,
            global_mir_plan: global_mir_plan.resolve(Antichain::from_elem(as_of)),
        })
    }

    fn subscribe_optimize_lir(
        &mut self,
        ctx: ExecuteContext,
        SubscribeOptimizeLir {
            validity,
            plan,
            mut optimizer,
            global_mir_plan,
        }: SubscribeOptimizeLir,
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
                let global_lir_plan =
                    return_if_err!(optimizer.optimize(global_mir_plan.clone()), ctx);

                let stage = SubscribeStage::Finish(SubscribeFinish {
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

    async fn subscribe_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        SubscribeFinish {
            validity,
            cluster_id,
            plan:
                plan::SubscribePlan {
                    copy_to,
                    emit_progress,
                    output,
                    ..
                },
            global_lir_plan,
        }: SubscribeFinish,
    ) -> Result<ExecuteResponse, AdapterError> {
        let sink_id = global_lir_plan.sink_id();

        let (tx, rx) = mpsc::unbounded_channel();
        let active_subscribe = ActiveSubscribe {
            user: ctx.session().user().clone(),
            conn_id: ctx.session().conn_id().clone(),
            channel: tx,
            emit_progress,
            as_of: global_lir_plan
                .as_of()
                .expect("set to Some in an earlier stage"),
            arity: global_lir_plan.sink_desc().from_desc.arity(),
            cluster_id,
            depends_on: validity.dependency_ids,
            start_time: self.now(),
            dropping: false,
            output,
        };
        active_subscribe.initialize();

        let (df_desc, df_meta) = global_lir_plan.unapply();
        // Emit notices.
        self.emit_optimizer_notices(ctx.session(), &df_meta.optimizer_notices);

        // Add metadata for the new SUBSCRIBE.
        let write_notify_fut = self.add_active_subscribe(sink_id, active_subscribe).await;
        // Ship dataflow.
        let ship_dataflow_fut = self.ship_dataflow(df_desc, cluster_id);

        // Both adding metadata for the new SUBSCRIBE and shipping the underlying dataflow, send
        // requests to external services, which can take time, so we run them concurrently.
        let ((), ()) = futures::future::join(write_notify_fut, ship_dataflow_fut).await;

        if let Some(target) = validity.replica_id {
            self.controller
                .compute
                .set_subscribe_target_replica(cluster_id, sink_id, target)
                .unwrap_or_terminate("cannot fail to set subscribe target replica");
        }

        self.active_conns
            .get_mut(ctx.session().conn_id())
            .expect("must exist for active sessions")
            .drop_sinks
            .push(ComputeSinkId {
                cluster_id,
                global_id: sink_id,
            });

        let resp = ExecuteResponse::Subscribing {
            rx,
            ctx_extra: std::mem::take(ctx.extra_mut()),
        };
        match copy_to {
            None => Ok(resp),
            Some(format) => Ok(ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            }),
        }
    }
}
