// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_sql::plan::{self, QueryWhen};
use timely::progress::Antichain;
use tokio::sync::mpsc;

use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::check_log_reads;
use crate::coord::{Coordinator, TargetCluster};
use crate::error::AdapterError;
use crate::optimize::Optimize;
use crate::session::TransactionOps;
use crate::subscribe::ActiveSubscribe;
use crate::util::{ComputeSinkId, ResultExt};
use crate::{optimize, AdapterNotice, ExecuteContext, TimelineContext};

impl Coordinator {
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_subscribe_off_thread(
        &mut self,
        ctx: &mut ExecuteContext,
        plan: plan::SubscribePlan,
        target_cluster: TargetCluster,
    ) -> Result<ExecuteResponse, AdapterError> {
        ::tracing::info!("sequence_subscribe_off_thread");

        let plan::SubscribePlan {
            from,
            with_snapshot,
            when,
            copy_to,
            emit_progress,
            up_to,
            output,
        } = plan;

        let cluster = self
            .catalog()
            .resolve_target_cluster(target_cluster, ctx.session())?;
        let cluster_id = cluster.id;

        let target_replica_name = ctx.session().vars().cluster_replica();
        let mut target_replica = target_replica_name
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
        if when == QueryWhen::Immediately {
            // If this isn't a SUBSCRIBE AS OF, the SUBSCRIBE can be in a transaction if it's the
            // only operation.
            ctx.session_mut()
                .add_transaction_ops(TransactionOps::Subscribe)?;
        }

        let depends_on = from.depends_on();

        // Run `check_log_reads` and emit notices.
        let notices = check_log_reads(
            self.catalog(),
            cluster,
            &depends_on,
            &mut target_replica,
            ctx.session().vars(),
        )?;
        ctx.session_mut().add_notices(notices);

        // Determine timeline.
        let mut timeline = self.validate_timeline_context(depends_on.clone())?;
        if matches!(timeline, TimelineContext::TimestampIndependent) && from.contains_temporal() {
            // If the from IDs are timestamp independent but the query contains temporal functions
            // then the timeline context needs to be upgraded to timestamp dependent.
            timeline = TimelineContext::TimestampDependent;
        }

        // Collect optimizer parameters.
        let compute_instance = self
            .instance_snapshot(cluster_id)
            .expect("compute instance does not exist");
        let id = self.allocate_transient_id()?;
        let conn_id = ctx.session().conn_id().clone();
        let up_to = up_to
            .map(|expr| Coordinator::evaluate_when(self.catalog().state(), expr, ctx.session_mut()))
            .transpose()?;
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

        // Build an optimizer for this SUBSCRIBE.
        let mut optimizer = optimize::subscribe::Optimizer::new(
            self.owned_catalog(),
            compute_instance,
            id,
            conn_id,
            with_snapshot,
            up_to,
            optimizer_config,
        );

        // MIR ⇒ MIR optimization (global)
        let global_mir_plan = optimizer.optimize(from)?;
        // Timestamp selection
        let oracle_read_ts = self.oracle_read_ts(&ctx.session, &timeline, &when).await;
        let as_of = self
            .determine_timestamp(
                ctx.session(),
                &global_mir_plan.id_bundle(optimizer.cluster_id()),
                &when,
                cluster_id,
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
        if let Some(up_to) = up_to {
            if as_of == up_to {
                ctx.session_mut()
                    .add_notice(AdapterNotice::EqualSubscribeBounds { bound: up_to });
            } else if as_of > up_to {
                return Err(AdapterError::AbsurdSubscribeBounds { as_of, up_to });
            }
        }
        let global_mir_plan = global_mir_plan.resolve(Antichain::from_elem(as_of));
        // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
        let global_lir_plan = optimizer.optimize(global_mir_plan.clone())?;

        let sink_id = global_lir_plan.sink_id();

        let (tx, rx) = mpsc::unbounded_channel();
        let active_subscribe = ActiveSubscribe {
            user: ctx.session().user().clone(),
            conn_id: ctx.session().conn_id().clone(),
            channel: tx,
            emit_progress,
            as_of,
            arity: global_lir_plan.sink_desc().from_desc.arity(),
            cluster_id,
            depends_on: depends_on.into_iter().collect(),
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

        if let Some(target) = target_replica {
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
