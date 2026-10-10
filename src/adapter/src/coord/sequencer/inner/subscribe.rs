// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::StreamExt;
use mz_adapter_types::connection::ConnectionId;
use mz_adapter_types::dyncfgs::SUBSCRIBE_MAX_BUFFERED_BYTES;
use mz_cluster_client::ReplicaId;
use mz_compute_types::ComputeInstanceId;
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::LirRelationExpr;
use mz_ore::collections::CollectionExt;
use mz_ore::instrument;
use mz_repr::GlobalId;
use mz_sql::plan;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

use crate::active_compute_sink::{
    ActiveComputeSink, ActiveSubscribe, ActiveSubscribeOwner, SubscribeBacklogAccounting,
};
use crate::command::ExecuteResponse;
use crate::coord::Coordinator;
use crate::coord::appends::BuiltinTableAppendNotify;
use crate::coord::peek::PeekResponseUnary;
use crate::error::AdapterError;
use crate::{ExecuteContextGuard, ReadHolds};

impl Coordinator {
    #[instrument]
    pub(crate) async fn implement_subscribe(
        &mut self,
        ctx_extra: &mut ExecuteContextGuard,
        df_desc: DataflowDescription<LirRelationExpr>,
        dependency_ids: BTreeSet<GlobalId>,
        cluster_id: ComputeInstanceId,
        replica_id: Option<ReplicaId>,
        conn_id: ConnectionId,
        session_uuid: Uuid,
        read_holds: ReadHolds,
        plan: plan::SubscribePlan,
    ) -> Result<(ExecuteResponse, BuiltinTableAppendNotify), AdapterError> {
        let sink_id = df_desc.sink_id();

        let (tx, rx) = mpsc::unbounded_channel::<PeekResponseUnary>();
        let backlog_accounting = Arc::new(Mutex::new(SubscribeBacklogAccounting::default()));
        let max_buffered_bytes =
            SUBSCRIBE_MAX_BUFFERED_BYTES.get(self.catalog().system_config().dyncfgs());
        let active_subscribe = ActiveSubscribe {
            owner: ActiveSubscribeOwner::Session {
                conn_id: conn_id.clone(),
                session_uuid,
            },
            channel: tx,
            backlog_accounting: Arc::clone(&backlog_accounting),
            max_buffered_bytes,
            emit_progress: plan.emit_progress,
            as_of: df_desc
                .as_of
                .as_ref()
                .and_then(|t| t.as_option())
                .copied()
                .expect("set to Some in an earlier stage"),
            arity: df_desc
                .sink_exports
                .values()
                .into_element()
                .from_desc
                .arity(),
            cluster_id,
            depends_on: dependency_ids,
            start_time: self.now(),
            output: plan.output,
            internal: false,
        };
        active_subscribe.initialize();

        // Register bookkeeping for the new SUBSCRIBE and ship its dataflow. The
        // `mz_subscriptions` write is deferred to a group commit (see
        // `add_active_compute_sink`) rather than committed inline, so it does not block
        // the coordinator loop on a timestamp-oracle round trip. We hand the notify back
        // so the caller can wait before returning the `SUBSCRIBE` response to the
        // subscribing session.
        let write_notify =
            self.add_active_compute_sink(sink_id, ActiveComputeSink::Subscribe(active_subscribe));

        // Ship the dataflow, handling errors gracefully. With the frontend subscribe
        // sequencing, a dependency can be dropped between sequencing (on the session
        // task) and here. The read holds acquired during sequencing don't prevent that:
        // they hold back compaction, not drops.
        if let Err(e) = self
            .try_ship_dataflow(df_desc, cluster_id, replica_id)
            .await
        {
            // Clean up the active compute sink that was added above, since the dataflow
            // was never created. If we don't do this, the sink_id remains in
            // `drop_sinks` but no collection exists in the compute controller, causing
            // a panic when the connection terminates. This also retracts the deferred
            // `mz_subscriptions` write, so `write_notify` can be dropped.
            self.remove_active_compute_sink(sink_id).await;
            return Err(AdapterError::concurrent_dependency_drop_from_dataflow_creation_error(e));
        }

        // Explicitly drop read holds, just to make it obvious what's happening.
        drop(read_holds);

        // Wrap the receiver so draining a message releases its footprint from the
        // shared accounting. FIFO delivery keeps the queue aligned with the
        // channel, so popping the oldest footprint matches the message just
        // drained. This keeps the accounting equal to the currently buffered
        // depth, which the coordinator watches to bound this subscribe.
        let rx = UnboundedReceiverStream::new(rx).map(move |response| {
            backlog_accounting
                .lock()
                .expect("subscribe backlog accounting poisoned")
                .pop();
            response
        });
        let resp = ExecuteResponse::Subscribing {
            rx: Box::new(rx),
            ctx_extra: std::mem::take(ctx_extra),
            instance_id: cluster_id,
        };
        let resp = match plan.copy_to {
            None => resp,
            Some(format) => ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            },
        };
        Ok((resp, write_notify))
    }
}
