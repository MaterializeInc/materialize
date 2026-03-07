// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordinator-side support machinery for frontend occ-based read-then write
//! sequencing in `frontend_read_then_write.rs`.
//!
//! N.B. It's a bit annoying that we still have the write submission go through
//! the coordinator. We can imagine in the long run we want a group-commit task
//! that runs independently, and we can directly submit write requests there.

use std::collections::BTreeSet;

use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_sql::plan::SubscribeOutput;
use tokio::sync::mpsc;

use crate::PeekResponseUnary;
use crate::active_compute_sink::{ActiveComputeSink, ActiveSubscribe};
use crate::coord::Coordinator;
use crate::coord::appends::TimestampedWriteResult;
use crate::error::AdapterError;

impl Coordinator {
    /// Handle a Command to create an internal subscribe.
    ///
    /// Internal subscribes are not visible in introspection collections. They
    /// are initially used for frontend-sequenced read-then-write
    /// (DELETE/UPDATE/INSERT ...SELECT) via OCC.
    ///
    /// This is called from the frontend OCC implementation after it has
    /// acquired the semaphore permit. We create the subscribe here (on the
    /// coordinator) and return the channel to the caller.
    ///
    /// The `read_holds` parameter contains the read holds for this specific
    /// operation. They are passed directly through the stages (not via the
    /// connection-keyed txn_read_holds map) to avoid issues where multiple
    /// operations on the same connection could interfere with each other's
    /// holds.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn handle_create_internal_subscribe(
        &mut self,
        df_desc: crate::optimize::LirDataflowDescription,
        cluster_id: mz_compute_types::ComputeInstanceId,
        replica_id: Option<mz_cluster_client::ReplicaId>,
        depends_on: BTreeSet<GlobalId>,
        as_of: Timestamp,
        arity: usize,
        sink_id: GlobalId,
        conn_id: mz_adapter_types::connection::ConnectionId,
        session_uuid: uuid::Uuid,
        start_time: mz_ore::now::EpochMillis,
        read_holds: crate::ReadHolds<Timestamp>,
        response_tx: tokio::sync::oneshot::Sender<
            Result<mpsc::UnboundedReceiver<PeekResponseUnary>, AdapterError>,
        >,
    ) {
        // Check if connection still exists. If not, the client disconnected
        // while waiting for the semaphore - the operation is effectively cancelled.
        if !self.active_conns.contains_key(&conn_id) {
            // Send error to indicate cancellation
            let _ = response_tx.send(Err(AdapterError::Canceled));
            return;
        }

        // Create the channel for subscribe responses
        let (tx, rx) = mpsc::unbounded_channel();

        let active_subscribe = ActiveSubscribe {
            conn_id: conn_id.clone(),
            session_uuid,
            channel: tx,
            emit_progress: true, // We need progress updates for OCC
            as_of,
            arity,
            cluster_id,
            depends_on,
            start_time,
            output: SubscribeOutput::Diffs, // Output format for diffs
            internal: true,                 // Internal subscribe - skip builtin table updates
        };
        active_subscribe.initialize();

        // Add metadata for the subscribe
        let write_notify_fut = self
            .add_active_compute_sink(sink_id, ActiveComputeSink::Subscribe(active_subscribe))
            .await;

        // Ship dataflow
        let ship_dataflow_fut = self.ship_dataflow(df_desc, cluster_id, replica_id);

        let ((), ()) = futures::future::join(write_notify_fut, ship_dataflow_fut).await;

        // Send the receiver back to the frontend
        let _ = response_tx.send(Ok(rx));

        // The read_holds are dropped here at the end of the function, after ship_dataflow
        // has completed. This ensures the since doesn't advance past our as_of timestamp
        // until the dataflow is running.
        drop(read_holds);
    }

    /// Handle the write attempt from the OCC loop.
    pub(crate) async fn handle_attempt_timestamped_write(
        &mut self,
        target_id: mz_repr::CatalogItemId,
        diffs: Vec<(Row, Diff)>,
        write_ts: Timestamp,
        result_tx: tokio::sync::oneshot::Sender<TimestampedWriteResult>,
    ) {
        use crate::coord::appends::PendingWriteTxn;
        use mz_storage_client::client::TableData;
        use smallvec::smallvec;
        use std::collections::BTreeMap;
        use tracing::Span;

        if self.controller.read_only() {
            panic!(
                "attempting OCC read-then-write in read-only mode: write_ts={}, target_id={:?}",
                write_ts, target_id
            );
        }

        // Early check if timestamp already passed
        let next_eligible_write_ts = self.peek_local_write_ts().await.step_forward();
        if write_ts < next_eligible_write_ts {
            let _ = result_tx.send(TimestampedWriteResult::TimestampPassed {
                target_timestamp: write_ts,
                current_write_ts: next_eligible_write_ts,
            });
            return;
        }

        // Create TableData from accumulated diffs
        let table_data = TableData::Rows(diffs);
        let writes = BTreeMap::from([(target_id, smallvec![table_data])]);

        tracing::trace!(?writes, ?write_ts, "about to attempt read-then-write");

        // Push internal timestamped write directly to pending_writes
        self.pending_writes
            .push(PendingWriteTxn::InternalTimestamped {
                span: Span::current(),
                writes,
                target_timestamp: write_ts,
                result_tx,
            });
        self.trigger_group_commit();
    }

    /// Drop an internal subscribe.
    pub(crate) async fn drop_internal_subscribe(&mut self, sink_id: GlobalId) {
        // Use drop_compute_sink instead of remove_active_compute_sink to also
        // cancel the dataflow on the compute side, not just remove bookkeeping.
        let _ = self.drop_compute_sink(sink_id).await;
    }
}
