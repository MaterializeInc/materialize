// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Client-side handle for executing standing queries without going through
//! the coordinator loop.
//!
//! [`StandingQueryExecuteClient`] is shared between the session client (which
//! writes param rows) and the handler task (which delivers results from the
//! subscribe). It allows EXECUTE STANDING QUERY to bypass the coordinator
//! entirely for the write path.
//!
//! The param collection is NOT registered with the collection manager's
//! append-only write task. Instead, we hold a [`WriteHandle`] directly and
//! do our own `compare_and_append`. This gives us full control over the
//! write timestamp: we write at the shard's current upper (which is already
//! closed by the base table), so the subscribe can produce results immediately
//! without waiting for the next group commit to advance the base table frontier.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use mz_persist_client::write::WriteHandle;
use mz_repr::{CatalogItemId, Diff, GlobalId, Row, Timestamp, TimestampManipulation};
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};
use uuid::Uuid;

/// Notification sent from the client to the handler task when param rows
/// have been written, so the handler can track in-flight request IDs.
#[derive(Debug)]
pub struct StandingQueryFlush {
    pub sink_id: GlobalId,
    pub write_ts: Timestamp,
    pub request_ids: Vec<Uuid>,
    pub param_rows: Vec<(Uuid, Row)>,
}

/// Client-side handle for a single standing query.
///
/// Cloneable — each session client that needs to execute this standing query
/// holds a clone. The handler task also holds a clone to deliver results
/// and queue retractions.
#[derive(Clone, Debug)]
pub struct StandingQueryExecuteClient {
    /// The CatalogItemId of this standing query.
    pub item_id: CatalogItemId,
    /// Persist write handle for the param collection shard.
    /// We are the sole writer, so we track the upper ourselves.
    /// Uses tokio::sync::Mutex because we hold it across .await.
    write_handle: Arc<tokio::sync::Mutex<WriteHandle<SourceData, (), Timestamp, StorageDiff>>>,
    /// Our tracked upper for the param collection shard.
    current_upper: Arc<tokio::sync::Mutex<Timestamp>>,
    /// Shared map from request_id → result sender.
    /// The client registers a sender here before writing the param row.
    /// The handler task removes it and sends results.
    result_senders: Arc<Mutex<BTreeMap<Uuid, oneshot::Sender<Vec<Row>>>>>,
    /// Channel to notify the handler task of write_ts → request_ids mappings.
    flush_tx: mpsc::UnboundedSender<StandingQueryFlush>,
    /// The GlobalId of the subscribe sink (used as the standing query key).
    sink_id: GlobalId,
}

impl StandingQueryExecuteClient {
    pub fn new(
        item_id: CatalogItemId,
        sink_id: GlobalId,
        write_handle: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
        flush_tx: mpsc::UnboundedSender<StandingQueryFlush>,
    ) -> Self {
        let upper = write_handle
            .shared_upper()
            .into_option()
            .unwrap_or(TimelyTimestamp::minimum());
        Self {
            item_id,
            write_handle: Arc::new(tokio::sync::Mutex::new(write_handle)),
            current_upper: Arc::new(tokio::sync::Mutex::new(upper)),
            result_senders: Arc::new(Mutex::new(BTreeMap::new())),
            flush_tx,
            sink_id,
        }
    }

    /// Execute a standing query with the given parameters.
    ///
    /// Writes the param row via direct `compare_and_append` (bypassing the
    /// collection manager), then waits for results from the subscribe handler.
    pub async fn execute(
        &self,
        params: &[(Row, mz_repr::SqlScalarType)],
    ) -> Result<Vec<Row>, StandingQueryExecuteError> {
        let request_id = Uuid::new_v4();

        // Build the parameter row: (request_id, param_1, param_2, ...).
        let mut param_row = Row::default();
        {
            let mut packer = param_row.packer();
            packer.push(mz_repr::Datum::Uuid(request_id));
            for (value, _typ) in params {
                packer.push(value.unpack_first());
            }
        }

        // Register a result channel before writing, so results can't arrive
        // before we're listening.
        let (result_tx, result_rx) = oneshot::channel();
        {
            let mut senders = self.result_senders.lock().expect("lock poisoned");
            senders.insert(request_id, result_tx);
        }

        // Write the param row via direct compare_and_append.
        let write_ts = self
            .append_param_row(&param_row, Diff::from(1i64))
            .await
            .map_err(|e| {
                // Clean up the registered sender on write failure.
                let mut senders = self.result_senders.lock().expect("lock poisoned");
                senders.remove(&request_id);
                StandingQueryExecuteError::WriteError(e)
            })?;

        debug!(
            "standing query {}: wrote param for request {request_id} at ts {write_ts}",
            self.sink_id
        );

        // Notify the handler task of the write_ts → request_id mapping.
        let _ = self.flush_tx.send(StandingQueryFlush {
            sink_id: self.sink_id,
            write_ts,
            request_ids: vec![request_id],
            param_rows: vec![(request_id, param_row)],
        });

        // Wait for results from the subscribe handler.
        result_rx
            .await
            .map_err(|_| StandingQueryExecuteError::ResultChannelClosed)
    }

    /// Append a single row to the param collection shard.
    ///
    /// We are the sole writer, so we use our tracked upper directly.
    /// Returns the timestamp at which the row was written.
    async fn append_param_row(&self, row: &Row, diff: Diff) -> Result<Timestamp, String> {
        // Lock both together — we're the sole writer so no contention.
        let mut wh = self.write_handle.lock().await;
        let mut upper_guard = self.current_upper.lock().await;
        let upper = *upper_guard;

        let new_upper = TimestampManipulation::step_forward(&upper);
        let updates = vec![(
            (SourceData(Ok(row.clone())), ()),
            upper.clone(),
            diff.into_inner(),
        )];

        let res = wh
            .compare_and_append(
                updates,
                Antichain::from_elem(upper),
                Antichain::from_elem(new_upper),
            )
            .await
            .expect("valid persist usage");

        match res {
            Ok(()) => {
                *upper_guard = new_upper;
                Ok(upper)
            }
            Err(mismatch) => {
                // We're the sole writer, so this shouldn't happen.
                // If it does, sync up and retry would be needed.
                Err(format!(
                    "unexpected upper mismatch: expected {:?}, actual {:?}",
                    mismatch.expected, mismatch.current
                ))
            }
        }
    }

    /// Advance the param shard's upper to at least `target`, without writing
    /// any data. This keeps the param collection's upper in sync with the
    /// subscribe frontier so that compaction can proceed.
    ///
    /// Only advances if the current upper is behind `target`.
    pub async fn advance_upper(&self, target: Timestamp) {
        let mut wh = self.write_handle.lock().await;
        let mut upper_guard = self.current_upper.lock().await;
        let upper = *upper_guard;

        if upper >= target {
            return;
        }

        let res = wh
            .compare_and_append(
                Vec::<((SourceData, ()), Timestamp, i64)>::new(),
                Antichain::from_elem(upper),
                Antichain::from_elem(target),
            )
            .await
            .expect("valid persist usage");

        match res {
            Ok(()) => {
                *upper_guard = target;
            }
            Err(mismatch) => {
                // Someone else advanced it (shouldn't happen, we're the sole writer).
                if let Some(actual) = mismatch.current.into_option() {
                    *upper_guard = actual;
                }
            }
        }
    }

    /// Deliver results for a request_id. Called by the handler task.
    ///
    /// Returns the oneshot sender if the request_id is found, None otherwise.
    pub fn take_result_sender(&self, request_id: &Uuid) -> Option<oneshot::Sender<Vec<Row>>> {
        let mut senders = self.result_senders.lock().expect("lock poisoned");
        senders.remove(request_id)
    }

    /// Queue a retraction for param rows by writing diff=-1 via compare_and_append.
    pub fn retract(&self, param_rows: Vec<Row>) {
        let write_handle = Arc::clone(&self.write_handle);
        let current_upper = Arc::clone(&self.current_upper);
        mz_ore::task::spawn(|| "standing-query-retract", async move {
            let mut wh = write_handle.lock().await;
            let mut upper_guard = current_upper.lock().await;
            let upper = *upper_guard;

            let new_upper = TimestampManipulation::step_forward(&upper);
            let updates: Vec<_> = param_rows
                .into_iter()
                .map(|row| {
                    (
                        (SourceData(Ok(row)), ()),
                        upper.clone(),
                        Diff::from(-1i64).into_inner(),
                    )
                })
                .collect();

            let res = wh
                .compare_and_append(
                    updates,
                    Antichain::from_elem(upper),
                    Antichain::from_elem(new_upper),
                )
                .await
                .expect("valid persist usage");

            match res {
                Ok(()) => {
                    *upper_guard = new_upper;
                }
                Err(mismatch) => {
                    warn!(
                        "standing query retraction: unexpected upper mismatch: expected {:?}, actual {:?}",
                        mismatch.expected, mismatch.current
                    );
                }
            }
        });
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StandingQueryExecuteError {
    #[error("failed to write param row: {0}")]
    WriteError(String),
    #[error("result channel closed (standing query may have been dropped)")]
    ResultChannelClosed,
}
