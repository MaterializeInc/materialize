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
//! Param writes are **batched**: `execute()` sends requests to a background
//! batcher task that drains all pending requests and writes them in a single
//! `compare_and_append`. This amortizes the persist write cost across many
//! concurrent executions.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mz_persist_client::write::WriteHandle;
use mz_repr::{CatalogItemId, GlobalId, Row, Timestamp, TimestampManipulation};
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, error, warn};

/// Notification sent from the batcher to the handler task when param rows
/// have been written, so the handler can track in-flight request IDs.
#[derive(Debug)]
pub struct StandingQueryFlush {
    pub sink_id: GlobalId,
    pub write_ts: Timestamp,
    pub request_ids: Vec<u64>,
}

/// A request to write a param row, sent from `execute()` to the batcher task.
#[derive(Debug)]
struct WriteRequest {
    request_id: u64,
    param_row: Row,
}

/// Commands sent to the batcher task.
#[derive(Debug)]
enum BatcherCmd {
    /// Write param rows (from execute()).
    Write(WriteRequest),
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
    /// Channel to send commands to the batcher task.
    batcher_tx: mpsc::UnboundedSender<BatcherCmd>,
    /// Shared map from request_id → result sender.
    /// The client registers a sender here before writing the param row.
    /// The handler task removes it and sends results.
    result_senders: Arc<Mutex<BTreeMap<u64, oneshot::Sender<RequestResult>>>>,
    /// Monotonically increasing counter for generating request IDs.
    next_request_id: Arc<AtomicU64>,
}

impl StandingQueryExecuteClient {
    pub fn new(
        item_id: CatalogItemId,
        sink_id: GlobalId,
        write_handle: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
        flush_tx: mpsc::UnboundedSender<StandingQueryFlush>,
        advance_upper_rx: watch::Receiver<Timestamp>,
    ) -> Self {
        let (batcher_tx, batcher_rx) = mpsc::unbounded_channel();
        spawn_batcher_task(
            sink_id,
            write_handle,
            batcher_rx,
            flush_tx,
            advance_upper_rx,
        );
        Self {
            item_id,
            batcher_tx,
            result_senders: Arc::new(Mutex::new(BTreeMap::new())),
            next_request_id: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Execute a standing query with the given parameters.
    ///
    /// Sends the param row to the batcher task for batched writing, then
    /// waits for results from the subscribe handler. If this future is
    /// cancelled (e.g. client disconnect), the param row is retracted
    /// so it doesn't accumulate in the subscribe's working set.
    pub async fn execute(
        &self,
        params: &[(Row, mz_repr::SqlScalarType)],
    ) -> Result<Vec<Row>, StandingQueryExecuteError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);

        // Build the parameter row: (request_id, param_1, param_2, ...).
        let mut param_row = Row::default();
        {
            let mut packer = param_row.packer();
            packer.push(mz_repr::Datum::UInt64(request_id));
            for (value, _typ) in params {
                packer.push(value.unpack_first());
            }
        }

        // Register a result channel before sending to the batcher, so results
        // can't arrive before we're listening.
        let (result_tx, result_rx) = oneshot::channel();
        {
            let mut senders = self.result_senders.lock().expect("lock poisoned");
            senders.insert(request_id, result_tx);
        }

        // Send to the batcher task for batched writing.
        if self
            .batcher_tx
            .send(BatcherCmd::Write(WriteRequest {
                request_id,
                param_row,
            }))
            .is_err()
        {
            let mut senders = self.result_senders.lock().expect("lock poisoned");
            senders.remove(&request_id);
            return Err(StandingQueryExecuteError::WriteError(
                "batcher task closed".to_string(),
            ));
        }

        // Wait for results from the subscribe handler.
        // The batcher retracts every param row it wrote with its next write,
        // so no explicit cleanup is needed on cancellation.
        result_rx
            .await
            .map_err(|_| StandingQueryExecuteError::ResultChannelClosed)?
            .map_err(StandingQueryExecuteError::Evaluation)
    }

    /// Deliver results for a request_id. Called by the handler task.
    ///
    /// Returns the oneshot sender if the request_id is found, None otherwise.
    pub fn take_result_sender(&self, request_id: &u64) -> Option<oneshot::Sender<RequestResult>> {
        let mut senders = self.result_senders.lock().expect("lock poisoned");
        senders.remove(request_id)
    }
}

/// Spawn the batcher task that owns the persist `WriteHandle` and batches
/// param writes and retractions into single `compare_and_append` calls.
fn spawn_batcher_task(
    sink_id: GlobalId,
    write_handle: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    batcher_rx: mpsc::UnboundedReceiver<BatcherCmd>,
    flush_tx: mpsc::UnboundedSender<StandingQueryFlush>,
    advance_upper_rx: watch::Receiver<Timestamp>,
) {
    mz_ore::task::spawn(
        || format!("standing-query-batcher-{sink_id}"),
        batcher_task(
            sink_id,
            write_handle,
            batcher_rx,
            flush_tx,
            advance_upper_rx,
        ),
    );
}

async fn batcher_task(
    sink_id: GlobalId,
    mut write_handle: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    mut batcher_rx: mpsc::UnboundedReceiver<BatcherCmd>,
    flush_tx: mpsc::UnboundedSender<StandingQueryFlush>,
    mut advance_upper_rx: watch::Receiver<Timestamp>,
) {
    let mut current_upper: Timestamp = write_handle
        .shared_upper()
        .into_option()
        .unwrap_or_else(TimelyTimestamp::minimum);

    debug!(
        %sink_id,
        %current_upper,
        initial_target = %*advance_upper_rx.borrow(),
        "batcher started",
    );

    // Each batch consumes one timestamp. Timestamps are milliseconds, and the
    // inputs' write frontier, which bounds how far ahead of the inputs the
    // param shard can be written without results waiting for the next tick,
    // advances with wall-clock time. Starting at most one batch per
    // millisecond keeps the param shard from outrunning the frontier and
    // using up the gap `advance_standing_query_uppers` leaves. Requests that
    // arrive meanwhile join the next batch.
    const BATCH_INTERVAL: Duration = Duration::from_millis(1);
    let mut next_batch = Instant::now();
    let mut cmds: Vec<BatcherCmd> = Vec::new();
    let mut writes = Vec::new();
    // Param rows of the last write, retracted by the next write.
    let mut pending_retractions: Vec<Row> = Vec::new();

    loop {
        // Apply the latest upper target before doing anything else.
        // This is critical: the subscribe can't produce output until the
        // param shard upper advances past its as_of. Using borrow_and_update
        // (not has_changed) ensures we catch values sent before we started
        // listening, not just new notifications.
        {
            let target = *advance_upper_rx.borrow_and_update();
            advance_upper(
                sink_id,
                &mut write_handle,
                &mut current_upper,
                &mut pending_retractions,
                target,
            )
            .await;
        }

        // Wait for at least one command or an upper-advance notification.
        tokio::select! {
            biased;

            result = advance_upper_rx.changed() => {
                if result.is_err() {
                    break;
                }
                let target = *advance_upper_rx.borrow_and_update();
                advance_upper(
                    sink_id,
                    &mut write_handle,
                    &mut current_upper,
                    &mut pending_retractions,
                    target,
                )
                .await;
            }

            count = batcher_rx.recv_many(&mut cmds, usize::MAX) => {
                if count == 0 {
                    break;
                }

                tokio::time::sleep_until(next_batch.into()).await;
                while let Ok(cmd) = batcher_rx.try_recv() {
                    cmds.push(cmd);
                }

                let lower = current_upper;
                let upper = TimestampManipulation::step_forward(&lower);

                let request_ids: Vec<_> = cmds.iter().map(|cmd| match cmd {
                    BatcherCmd::Write(req) => req.request_id
                }).collect();

                // Retract the previous write's param rows at this write's
                // timestamp. Every write, here or in `advance_upper`, starts at
                // the current upper, which the previous write left at its
                // timestamp + 1, so each param row exists for exactly one
                // timestamp. The handler relies on this: a request's results
                // are the positive updates at its write timestamp.
                writes.extend(
                    pending_retractions
                        .drain(..)
                        .map(|row| ((SourceData(Ok(row)), ()), lower, -1)),
                );
                pending_retractions.extend(cmds.drain(..).map(|cmd| match cmd {
                    BatcherCmd::Write(req) => req.param_row,
                }));
                writes.extend(
                    pending_retractions
                        .iter()
                        .map(|row| ((SourceData(Ok(row.clone())), ()), lower, 1)),
                );

                next_batch = Instant::now() + BATCH_INTERVAL;
                let append_start = Instant::now();
                let res = batch_append(
                    sink_id,
                    &mut write_handle,
                    lower,
                    upper,
                    writes.drain(..),
                )
                .await;
                match res {
                    Ok(()) => {
                        debug!(
                            %sink_id,
                            %lower,
                            %upper,
                            count = request_ids.len(),
                            append_duration = ?append_start.elapsed(),
                            "batched param writes",
                        );
                        let _ = flush_tx.send(StandingQueryFlush {
                            sink_id,
                            write_ts: lower,
                            request_ids,
                        });
                        current_upper = upper;
                    }
                    Err(e) => {
                        error!(%sink_id, upper = ?e, "batch append failed");
                        // TODO: Retry with new upper.
                        panic!("Unhandled upper mismatch");
                    }
                }
            }
        }
    }
}

/// Append a batch of param writes and retractions in a single
/// `compare_and_append`. Returns the upper timestamp on error.
async fn batch_append(
    sink_id: GlobalId,
    write_handle: &mut WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    lower: Timestamp,
    upper: Timestamp,
    updates: impl IntoIterator<Item = ((SourceData, ()), Timestamp, i64)>,
) -> Result<(), Antichain<Timestamp>> {
    let res = write_handle
        .compare_and_append(
            updates,
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
        )
        .await
        .expect("valid persist usage");

    match res {
        Ok(()) => Ok(()),
        Err(mismatch) => {
            warn!(%sink_id, %mismatch, "upper mismatch");
            Err(mismatch.current)
        }
    }
}

/// Advance the param shard's upper to at least `target`, retracting the
/// `pending_retractions` at the current upper.
///
/// Leaves `pending_retractions` in place if the upper is already at or past
/// `target`, or on an upper mismatch.
async fn advance_upper(
    sink_id: GlobalId,
    write_handle: &mut WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    current_upper: &mut Timestamp,
    pending_retractions: &mut Vec<Row>,
    target: Timestamp,
) {
    let upper = *current_upper;
    if upper >= target {
        return;
    }

    debug!(%sink_id, %upper, %target, "advance upper");

    // Without this, an idle standing query would keep its last param rows,
    // and their results, live until the next execution.
    let retractions: Vec<_> = pending_retractions
        .iter()
        .map(|row| ((SourceData(Ok(row.clone())), ()), upper, -1))
        .collect();
    let res = write_handle
        .compare_and_append(
            retractions,
            Antichain::from_elem(upper),
            Antichain::from_elem(target),
        )
        .await
        .expect("valid persist usage");

    match res {
        Ok(()) => {
            *current_upper = target;
            pending_retractions.clear();
        }
        Err(mismatch) => {
            warn!(
                %sink_id,
                expected = ?mismatch.expected,
                actual = ?mismatch.current,
                "upper advance mismatch",
            );
            if let Some(actual) = mismatch.current.into_option() {
                *current_upper = actual;
            }
        }
    }
}

/// The outcome of one request: its result rows, or the error its evaluation raised.
pub type RequestResult = Result<Vec<Row>, String>;

#[derive(Debug, thiserror::Error)]
pub enum StandingQueryExecuteError {
    #[error("failed to write param row: {0}")]
    WriteError(String),
    #[error("result channel closed (standing query may have been dropped)")]
    ResultChannelClosed,
    /// Evaluating the query for this request's parameters failed. Other requests are
    /// unaffected when the error is scoped to rows, see `SubscribeSinkConnection::inline_errors`.
    #[error("{0}")]
    Evaluation(String),
}
