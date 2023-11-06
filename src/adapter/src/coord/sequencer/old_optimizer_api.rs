// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains alternative implementations of `sequence_~` methods
//! that optimize plans using the new optimizer API. The `sequence_plan` method
//! in the parent module will delegate to these methods only if the
//! `enable_unified_optimizer_api` feature flag is off. Once we have gained
//! enough confidence that the new methods behave equally well as the old ones,
//! we will deprecate the old methods and move the implementations here to the
//! `inner` module.

use mz_compute_types::dataflows::DataflowDesc;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, SubscribeSinkConnection};
use mz_repr::RelationDesc;
use mz_sql::plan::{self, QueryWhen, SubscribeFrom};
use timely::progress::Antichain;
use tokio::sync::mpsc;

use crate::coord::sequencer::inner::check_log_reads;
use crate::coord::{Coordinator, TargetCluster};
use crate::session::TransactionOps;
use crate::subscribe::ActiveSubscribe;
use crate::util::{ComputeSinkId, ResultExt};
use crate::{AdapterError, AdapterNotice, ExecuteContext, ExecuteResponse, TimelineContext};

impl Coordinator {
    // Subscribe
    // ---------

    /// This should mirror the operational semantics of
    /// `Coordinator::sequence_subscribe`.
    #[deprecated = "This is being replaced by sequence_subscribe (see #20569)."]
    pub(super) async fn sequence_subscribe_deprecated(
        &mut self,
        ctx: &mut ExecuteContext,
        plan: plan::SubscribePlan,
        target_cluster: TargetCluster,
    ) -> Result<ExecuteResponse, AdapterError> {
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

        // Determine the frontier of updates to subscribe *from*.
        // Updates greater or equal to this frontier will be produced.
        let depends_on = from.depends_on();
        let notices = check_log_reads(
            self.catalog(),
            cluster,
            &depends_on,
            &mut target_replica,
            ctx.session().vars(),
        )?;
        ctx.session_mut().add_notices(notices);

        let id_bundle = self
            .index_oracle(cluster_id)
            .sufficient_collections(&depends_on);
        let mut timeline = self.validate_timeline_context(depends_on.clone())?;
        if matches!(timeline, TimelineContext::TimestampIndependent) && from.contains_temporal() {
            // If the from IDs are timestamp independent but the query contains temporal functions
            // then the timeline context needs to be upgraded to timestamp dependent.
            timeline = TimelineContext::TimestampDependent;
        }
        let as_of = self
            .determine_timestamp(
                ctx.session(),
                &id_bundle,
                &when,
                cluster_id,
                &timeline,
                None,
            )
            .await?
            .timestamp_context
            .timestamp_or_default();
        if let Some(id) = ctx.extra().contents() {
            self.set_statement_execution_timestamp(id, as_of);
        }

        let up_to = up_to
            .map(|expr| Coordinator::evaluate_when(self.catalog().state(), expr, ctx.session_mut()))
            .transpose()?;
        if let Some(up_to) = up_to {
            if as_of == up_to {
                ctx.session_mut()
                    .add_notice(AdapterNotice::EqualSubscribeBounds { bound: up_to });
            } else if as_of > up_to {
                return Err(AdapterError::AbsurdSubscribeBounds { as_of, up_to });
            }
        }
        let up_to = up_to.map(Antichain::from_elem).unwrap_or_default();

        let (mut dataflow, dataflow_metainfo) = match from {
            SubscribeFrom::Id(from_id) => {
                let from = self.catalog().get_entry(&from_id);
                let from_desc = from
                    .desc(
                        &self
                            .catalog()
                            .resolve_full_name(from.name(), Some(ctx.session().conn_id())),
                    )
                    .expect("subscribes can only be run on items with descs")
                    .into_owned();
                let sink_id = self.allocate_transient_id()?;
                let sink_desc = ComputeSinkDesc {
                    from: from_id,
                    from_desc,
                    connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection::default()),
                    with_snapshot,
                    up_to,
                    // No `FORCE NOT NULL` for subscribes
                    non_null_assertions: vec![],
                };
                let sink_name = format!("subscribe-{}", sink_id);
                self.dataflow_builder(cluster_id)
                    .build_sink_dataflow(sink_name, sink_id, sink_desc)?
            }
            SubscribeFrom::Query { expr, desc } => {
                let id = self.allocate_transient_id()?;
                let expr = self.view_optimizer.optimize(expr)?;
                let desc = RelationDesc::new(expr.typ(), desc.iter_names());
                let sink_desc = ComputeSinkDesc {
                    from: id,
                    from_desc: desc,
                    connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection::default()),
                    with_snapshot,
                    up_to,
                    // No `FORCE NOT NULL` for subscribes
                    non_null_assertions: vec![],
                };
                let mut dataflow = DataflowDesc::new(format!("subscribe-{}", id));
                let mut dataflow_builder = self.dataflow_builder(cluster_id);
                dataflow_builder.import_view_into_dataflow(&id, &expr, &mut dataflow)?;
                let dataflow_metainfo =
                    dataflow_builder.build_sink_dataflow_into(&mut dataflow, id, sink_desc)?;
                (dataflow, dataflow_metainfo)
            }
        };

        self.emit_optimizer_notices(ctx.session(), &dataflow_metainfo.optimizer_notices);

        dataflow.set_as_of(Antichain::from_elem(as_of));

        let (&sink_id, sink_desc) = dataflow
            .sink_exports
            .iter()
            .next()
            .expect("subscribes have a single sink export");
        let (tx, rx) = mpsc::unbounded_channel();
        let active_subscribe = ActiveSubscribe {
            user: ctx.session().user().clone(),
            conn_id: ctx.session().conn_id().clone(),
            channel: tx,
            emit_progress,
            as_of,
            arity: sink_desc.from_desc.arity(),
            cluster_id,
            depends_on: depends_on.into_iter().collect(),
            start_time: self.now(),
            dropping: false,
            output,
        };
        active_subscribe.initialize();
        self.add_active_subscribe(sink_id, active_subscribe).await;

        // Allow while the introduction of the new optimizer API in
        // #20569 is in progress.
        #[allow(deprecated)]
        match self.ship_dataflow(dataflow, cluster_id).await {
            Ok(_) => {}
            Err(e) => {
                self.remove_active_subscribe(sink_id).await;
                return Err(e);
            }
        };
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
