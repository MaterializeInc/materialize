// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::instrument;
use mz_sql::plan::{self, QueryWhen};
use mz_sql::session::metadata::SessionMetadata;
use timely::progress::Antichain;
use tokio::sync::mpsc;
use tracing::Span;

use crate::active_compute_sink::{ActiveComputeSink, ActiveSubscribe};
use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::{check_log_reads, return_if_err};
use crate::coord::{
    Coordinator, Message, PlanValidity, StageResult, Staged, SubscribeFinish, SubscribeOptimizeMir,
    SubscribeStage, SubscribeTimestampOptimizeLir, TargetCluster,
};
use crate::error::AdapterError;
use crate::optimize::{Optimize, OverrideFrom};
use crate::session::{Session, TransactionOps};
use crate::util::ResultExt;
use crate::{optimize, AdapterNotice, ExecuteContext, TimelineContext};

impl Staged for SubscribeStage {
    fn validity(&mut self) -> &mut PlanValidity {
        match self {
            SubscribeStage::OptimizeMir(stage) => &mut stage.validity,
            SubscribeStage::TimestampOptimizeLir(stage) => &mut stage.validity,
            SubscribeStage::Finish(stage) => &mut stage.validity,
        }
    }

    async fn stage(
        self,
        coord: &mut Coordinator,
        ctx: &mut ExecuteContext,
    ) -> Result<StageResult<Box<Self>>, AdapterError> {
        match self {
            SubscribeStage::OptimizeMir(stage) => {
                coord.subscribe_optimize_mir(ctx.session(), stage)
            }
            SubscribeStage::TimestampOptimizeLir(stage) => {
                coord.subscribe_timestamp_optimize_lir(ctx, stage).await
            }
            SubscribeStage::Finish(stage) => coord.subscribe_finish(ctx, stage).await,
        }
    }

    fn message(self, ctx: ExecuteContext, span: Span) -> Message {
        Message::SubscribeStageReady {
            ctx,
            span,
            stage: self,
        }
    }
}

impl Coordinator {
    #[instrument]
    pub(crate) async fn sequence_subscribe(
        &mut self,
        mut ctx: ExecuteContext,
        plan: plan::SubscribePlan,
        target_cluster: TargetCluster,
    ) {
        let stage = return_if_err!(
            self.subscribe_validate(ctx.session_mut(), plan, target_cluster),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    fn subscribe_validate(
        &mut self,
        session: &mut Session,
        plan: plan::SubscribePlan,
        target_cluster: TargetCluster,
    ) -> Result<SubscribeStage, AdapterError> {
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

        Ok(SubscribeStage::OptimizeMir(SubscribeOptimizeMir {
            validity,
            plan,
            timeline,
        }))
    }

    #[instrument]
    fn subscribe_optimize_mir(
        &mut self,
        session: &Session,
        SubscribeOptimizeMir {
            mut validity,
            plan,
            timeline,
        }: SubscribeOptimizeMir,
    ) -> Result<StageResult<Box<SubscribeStage>>, AdapterError> {
        let plan::SubscribePlan {
            with_snapshot,
            up_to,
            ..
        } = &plan;

        // Collect optimizer parameters.
        let cluster_id = validity.cluster_id.expect("cluser_id");
        let compute_instance = self
            .instance_snapshot(cluster_id)
            .expect("compute instance does not exist");
        let id = self.allocate_transient_id()?;
        let conn_id = session.conn_id().clone();
        let up_to = up_to
            .as_ref()
            .map(|expr| Coordinator::evaluate_when(self.catalog().state(), expr.clone(), session))
            .transpose()?;
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&self.catalog.get_cluster(cluster_id).config.features());

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

        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize subscribe (mir)",
            move || {
                span.in_scope(|| {
                    // MIR ⇒ MIR optimization (global)
                    let global_mir_plan = optimizer.catch_unwind_optimize(plan.from.clone())?;
                    // Add introduced indexes as validity dependencies.
                    validity
                        .dependency_ids
                        .extend(global_mir_plan.id_bundle(optimizer.cluster_id()).iter());

                    let stage =
                        SubscribeStage::TimestampOptimizeLir(SubscribeTimestampOptimizeLir {
                            validity,
                            plan,
                            timeline,
                            optimizer,
                            global_mir_plan,
                        });
                    Ok(Box::new(stage))
                })
            },
        )))
    }

    #[instrument]
    async fn subscribe_timestamp_optimize_lir(
        &mut self,
        ctx: &ExecuteContext,
        SubscribeTimestampOptimizeLir {
            validity,
            plan,
            timeline,
            mut optimizer,
            global_mir_plan,
        }: SubscribeTimestampOptimizeLir,
    ) -> Result<StageResult<Box<SubscribeStage>>, AdapterError> {
        let plan::SubscribePlan { when, .. } = &plan;

        // Timestamp selection
        let oracle_read_ts = self.oracle_read_ts(ctx.session(), &timeline, when).await;
        let bundle = &global_mir_plan.id_bundle(optimizer.cluster_id());
        let as_of = self
            .determine_timestamp(
                ctx.session(),
                bundle,
                when,
                optimizer.cluster_id(),
                &timeline,
                oracle_read_ts,
                None,
            )
            .await?
            .timestamp_context
            .timestamp_or_default();
        if let Some(id) = ctx.extra().contents() {
            self.set_statement_execution_timestamp(id, as_of);
        }
        if let Some(up_to) = optimizer.up_to() {
            if as_of == up_to {
                ctx.session()
                    .add_notice(AdapterNotice::EqualSubscribeBounds { bound: up_to });
            } else if as_of > up_to {
                return Err(AdapterError::AbsurdSubscribeBounds { as_of, up_to });
            }
        }
        // Get read holds so that dependencies cannot be dropped during off-thread optimization.
        // Invariant: the last element in this connection's self.txn_read_holds must be the holds
        // here, because the last that same element will be removed during the finish stage.
        self.acquire_read_holds_auto_cleanup(ctx.session(), as_of, bundle, true)
            .expect("able to acquire read holds at the time that we just got from `determine_timestamp`");
        let global_mir_plan = global_mir_plan.resolve(Antichain::from_elem(as_of));

        // Optimize LIR
        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize subscribe (lir)",
            move || {
                span.in_scope(|| {
                    // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                    let global_lir_plan =
                        optimizer.catch_unwind_optimize(global_mir_plan.clone())?;

                    let stage = SubscribeStage::Finish(SubscribeFinish {
                        validity,
                        cluster_id: optimizer.cluster_id(),
                        plan,
                        global_lir_plan,
                    });
                    Ok(Box::new(stage))
                })
            },
        )))
    }

    #[instrument]
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
    ) -> Result<StageResult<Box<SubscribeStage>>, AdapterError> {
        let sink_id = global_lir_plan.sink_id();

        let (tx, rx) = mpsc::unbounded_channel();
        let active_subscribe = ActiveSubscribe {
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
            output,
        };
        active_subscribe.initialize();

        let (df_desc, df_meta) = global_lir_plan.unapply();
        // Emit notices.
        self.emit_optimizer_notices(ctx.session(), &df_meta.optimizer_notices);

        // Release the pre-optimization read holds because the controller is now handling those.
        let mut txn_reads = self
            .txn_read_holds
            .remove(ctx.session().conn_id())
            .expect("expected read holds to exist for sequenced SUBSCRIBE");
        let last = txn_reads.pop().expect("expected read hold to exist");
        self.release_read_holds(vec![last]);
        if !txn_reads.is_empty() {
            self.txn_read_holds
                .insert(ctx.session().conn_id().clone(), txn_reads);
        }

        // Add metadata for the new SUBSCRIBE.
        let write_notify_fut = self
            .add_active_compute_sink(sink_id, ActiveComputeSink::Subscribe(active_subscribe))
            .await;
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

        let resp = ExecuteResponse::Subscribing {
            rx,
            ctx_extra: std::mem::take(ctx.extra_mut()),
        };
        let resp = match copy_to {
            None => resp,
            Some(format) => ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            },
        };
        Ok(StageResult::Response(resp))
    }
}
