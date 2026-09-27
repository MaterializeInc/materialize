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

use differential_dataflow::consolidation::consolidate;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{
    CatalogItemId, Datum, GlobalId, RelationDesc, Row, RowPacker, Timestamp, TimestampManipulation,
};
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, warn};

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
    /// `(request_id, param_1, ..., param_K)`. The batcher appends `write_ts`.
    param_row: Row,
    /// The earliest timestamp the param row may be written at.
    min_ts: Option<Timestamp>,
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
    /// Starts the batcher task that writes the param shard of `write_handle`.
    ///
    /// `persist_client` and `param_desc` open a reader of the same shard, which the batcher uses
    /// once, when it first becomes writable, to retract the rows an earlier process left behind.
    pub fn new(
        item_id: CatalogItemId,
        sink_id: GlobalId,
        persist_client: PersistClient,
        param_desc: RelationDesc,
        write_handle: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
        flush_tx: mpsc::UnboundedSender<StandingQueryFlush>,
        advance_upper_rx: watch::Receiver<Option<Timestamp>>,
    ) -> Self {
        let (batcher_tx, batcher_rx) = mpsc::unbounded_channel();
        spawn_batcher_task(
            sink_id,
            persist_client,
            param_desc,
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
    ///
    /// The results are those of the query at the param row's write
    /// timestamp, which is at least `min_ts`. A strictly serializable caller
    /// passes the oracle's read timestamp.
    pub async fn execute(
        &self,
        params: &[(Row, mz_repr::SqlScalarType)],
        min_ts: Option<Timestamp>,
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
                min_ts,
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
        // A param row is live in the dataflow only at its write timestamp, so
        // no explicit cleanup is needed on cancellation.
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
    persist_client: PersistClient,
    param_desc: RelationDesc,
    write_handle: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    batcher_rx: mpsc::UnboundedReceiver<BatcherCmd>,
    flush_tx: mpsc::UnboundedSender<StandingQueryFlush>,
    advance_upper_rx: watch::Receiver<Option<Timestamp>>,
) {
    mz_ore::task::spawn(
        || format!("standing-query-batcher-{sink_id}"),
        batcher_task(
            sink_id,
            persist_client,
            param_desc,
            write_handle,
            batcher_rx,
            flush_tx,
            advance_upper_rx,
        ),
    );
}

async fn batcher_task(
    sink_id: GlobalId,
    persist_client: PersistClient,
    param_desc: RelationDesc,
    write_handle: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    mut batcher_rx: mpsc::UnboundedReceiver<BatcherCmd>,
    flush_tx: mpsc::UnboundedSender<StandingQueryFlush>,
    mut advance_upper_rx: watch::Receiver<Option<Timestamp>>,
) {
    let current_upper: Timestamp = write_handle
        .shared_upper()
        .into_option()
        .unwrap_or_else(TimelyTimestamp::minimum);

    debug!(
        %sink_id,
        %current_upper,
        initial_target = ?*advance_upper_rx.borrow(),
        "batcher started",
    );

    let mut batcher = Batcher {
        sink_id,
        write_handle,
        flush_tx,
        current_upper,
        next_batch: Instant::now(),
        pending_retractions: Vec::new(),
    };
    // What `retract_leftovers` reads with, until the batcher first becomes writable.
    let mut leftover_reader = Some((persist_client, param_desc));
    let mut cmds: Vec<BatcherCmd> = Vec::new();
    // Requests whose `min_ts` the param shard has not reached, by `min_ts`.
    // They are written once `advance_upper` or other batches move the upper
    // there, which keeps the gap `advance_standing_query_uppers` leaves
    // instead of jumping the param shard ahead of the inputs.
    let mut parked: BTreeMap<Timestamp, Vec<WriteRequest>> = BTreeMap::new();

    loop {
        // Apply the latest upper target before doing anything else.
        // This is critical: the subscribe can't produce output until the
        // param shard upper advances past its as_of. Using borrow_and_update
        // (not has_changed) ensures we catch values sent before we started
        // listening, not just new notifications.
        //
        // No target means the coordinator is read-only. The param shard then
        // belongs to the leader, so this task must not write it, and requests
        // stay parked until a target arrives.
        let target = *advance_upper_rx.borrow_and_update();
        let writable = match target {
            Some(target) => {
                if let Some((persist_client, param_desc)) = leftover_reader.take() {
                    batcher.retract_leftovers(&persist_client, param_desc).await;
                }
                batcher.advance_upper(target).await;
                true
            }
            None => false,
        };

        if writable {
            let ready = take_ready(&mut parked, batcher.current_upper);
            if !ready.is_empty() {
                batcher.write(ready).await;
                continue;
            }
        }

        // Wait for at least one command or an upper-advance notification.
        tokio::select! {
            biased;

            result = advance_upper_rx.changed() => {
                if result.is_err() {
                    break;
                }
                // The next iteration applies the new target.
            }

            count = batcher_rx.recv_many(&mut cmds, usize::MAX) => {
                if count == 0 {
                    break;
                }

                tokio::time::sleep_until(batcher.next_batch.into()).await;
                while let Ok(cmd) = batcher_rx.try_recv() {
                    cmds.push(cmd);
                }

                for cmd in cmds.drain(..) {
                    let BatcherCmd::Write(req) = cmd;
                    let min_ts = req.min_ts.unwrap_or_else(TimelyTimestamp::minimum);
                    parked.entry(min_ts).or_default().push(req);
                }
                if writable {
                    let ready = take_ready(&mut parked, batcher.current_upper);
                    if !ready.is_empty() {
                        batcher.write(ready).await;
                    }
                }
            }
        }
    }
}

/// Removes and returns the parked requests whose `min_ts` is at most `upper`.
fn take_ready(
    parked: &mut BTreeMap<Timestamp, Vec<WriteRequest>>,
    upper: Timestamp,
) -> Vec<WriteRequest> {
    let later = parked.split_off(&TimestampManipulation::step_forward(&upper));
    let ready = std::mem::replace(parked, later);
    ready.into_values().flatten().collect()
}

/// The state of the batcher task that writes the param shard.
struct Batcher {
    sink_id: GlobalId,
    write_handle: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    flush_tx: mpsc::UnboundedSender<StandingQueryFlush>,
    /// The param shard's upper, and the timestamp of the next write.
    current_upper: Timestamp,
    /// The earliest time the next batch may start.
    next_batch: Instant,
    /// Updates that undo the param rows the shard holds, written at the start
    /// of the next write. They only reclaim space: the dataflow's temporal
    /// filter already confines each row to its `write_ts`.
    pending_retractions: Vec<(Row, StorageDiff)>,
}

impl Batcher {
    /// Writes `requests` at the current upper, with `write_ts` set to that
    /// timestamp, retracts the previous write's rows there, and notifies the
    /// handler.
    ///
    /// The handler relies on each param row being live in the dataflow only at
    /// its `write_ts`: a request's results are the positive updates there.
    async fn write(&mut self, requests: Vec<WriteRequest>) {
        // Each batch consumes one timestamp. Timestamps are milliseconds, and
        // the inputs' write frontier, which bounds how far ahead of the inputs
        // the param shard can be written without results waiting for the next
        // tick, advances with wall-clock time. Starting at most one batch per
        // millisecond keeps the param shard from outrunning the frontier and
        // using up the gap `advance_standing_query_uppers` leaves. Requests
        // that arrive meanwhile join the next batch.
        const BATCH_INTERVAL: Duration = Duration::from_millis(1);
        tokio::time::sleep_until(self.next_batch.into()).await;

        let sink_id = self.sink_id;
        let request_ids: Vec<_> = requests.iter().map(|req| req.request_id).collect();
        self.next_batch = Instant::now() + BATCH_INTERVAL;

        // Another process can append to the shard concurrently: a previous
        // leader keeps serving executions until it is terminated. An upper
        // mismatch therefore moves the batch to the shard's actual upper.
        // Parked requests' `min_ts` stays satisfied, as the upper only grows.
        loop {
            let lower = self.current_upper;
            let upper = TimestampManipulation::step_forward(&lower);

            // The previous write's rows are inert from `lower` on, so
            // retracting them changes no result. A crash before this write
            // leaves them in the shard, where the next process's
            // `retract_leftovers` finds them.
            let mut writes: Vec<_> = self
                .pending_retractions
                .iter()
                .map(|(row, diff)| ((SourceData(Ok(row.clone())), ()), lower, *diff))
                .collect();
            let mut new_rows = Vec::with_capacity(requests.len());
            for req in &requests {
                let mut row = req.param_row.clone();
                RowPacker::for_existing_row(&mut row).push(Datum::MzTimestamp(lower));
                writes.push(((SourceData(Ok(row.clone())), ()), lower, 1));
                new_rows.push(row);
            }

            let append_start = Instant::now();
            match batch_append(sink_id, &mut self.write_handle, lower, upper, writes).await {
                Ok(()) => {
                    debug!(
                        %sink_id,
                        %lower,
                        %upper,
                        count = request_ids.len(),
                        append_duration = ?append_start.elapsed(),
                        "batched param writes",
                    );
                    self.pending_retractions = new_rows.into_iter().map(|row| (row, -1)).collect();
                    let _ = self.flush_tx.send(StandingQueryFlush {
                        sink_id,
                        write_ts: lower,
                        request_ids,
                    });
                    self.current_upper = upper;
                    return;
                }
                Err(actual) => {
                    self.current_upper = actual.into_option().expect("param shard is never closed");
                }
            }
        }
    }

    /// Advance the param shard's upper to at least `target`, writing the
    /// pending retractions at the current upper.
    ///
    /// Leaves the pending retractions in place if the upper is already at or
    /// past `target`.
    async fn advance_upper(&mut self, target: Timestamp) {
        let sink_id = self.sink_id;
        // Retries on an upper mismatch for the reason given in `write`.
        while self.current_upper < target {
            let upper = self.current_upper;
            debug!(%sink_id, %upper, %target, "advance upper");

            // Without this, an idle standing query would keep its last param
            // rows in the shard until the next execution.
            let retractions: Vec<_> = self
                .pending_retractions
                .iter()
                .map(|(row, diff)| ((SourceData(Ok(row.clone())), ()), upper, *diff))
                .collect();
            let res = self
                .write_handle
                .compare_and_append(
                    retractions,
                    Antichain::from_elem(upper),
                    Antichain::from_elem(target),
                )
                .await
                .expect("valid persist usage");

            match res {
                Ok(()) => {
                    self.current_upper = target;
                    self.pending_retractions.clear();
                }
                Err(mismatch) => {
                    warn!(
                        %sink_id,
                        expected = ?mismatch.expected,
                        actual = ?mismatch.current,
                        "upper advance mismatch",
                    );
                    self.current_upper = mismatch
                        .current
                        .into_option()
                        .expect("param shard is never closed");
                }
            }
        }
    }

    /// Retracts the shard's contents, the rows that earlier processes wrote
    /// and did not retract.
    ///
    /// Must run before this batcher's first write. The shard's upper moves to
    /// just past the retraction. On error the leftovers stay in the shard.
    async fn retract_leftovers(
        &mut self,
        persist_client: &PersistClient,
        param_desc: RelationDesc,
    ) {
        let sink_id = self.sink_id;
        // Opened here, not at startup, so that a read-only process never
        // registers a reader of a shard the leader owns.
        let mut read_handle = persist_client
            .open_leased_reader::<SourceData, (), Timestamp, StorageDiff>(
                self.write_handle.shard_id(),
                Arc::new(param_desc),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: sink_id.to_string(),
                    handle_purpose: format!("standing query param leftovers for {sink_id}"),
                },
                false,
            )
            .await
            .expect("valid persist usage");

        // The retraction must be appended at exactly the upper the snapshot
        // was read below. A previous leader can still write between process
        // start and here, and retracting rows it has since retracted itself
        // would leave negative multiplicities. So read the freshest upper,
        // and retry when the append finds the upper moved.
        loop {
            let upper = self
                .write_handle
                .fetch_recent_upper()
                .await
                .as_option()
                .copied()
                .expect("param shard is never closed");
            self.current_upper = upper;
            // An upper at the minimum means the shard never held data.
            let Some(as_of) = upper.step_back() else {
                break;
            };
            let updates = match read_handle
                .snapshot_and_fetch(Antichain::from_elem(as_of))
                .await
            {
                Ok(updates) => updates,
                Err(since) => {
                    warn!(%sink_id, %as_of, ?since, "cannot read param leftovers");
                    break;
                }
            };
            let mut leftovers: Vec<_> = updates
                .into_iter()
                .filter_map(|((data, ()), _ts, diff)| match data.0 {
                    Ok(row) => Some((row, diff)),
                    // The batcher writes no errors, so no row carries one.
                    Err(err) => {
                        warn!(%sink_id, %err, "param shard holds an error");
                        None
                    }
                })
                .collect();
            consolidate(&mut leftovers);
            if leftovers.is_empty() {
                break;
            }
            debug!(%sink_id, %as_of, count = leftovers.len(), "retracting param leftovers");
            let retract_upper = TimestampManipulation::step_forward(&upper);
            let retractions = leftovers
                .into_iter()
                .map(|(row, diff)| ((SourceData(Ok(row)), ()), upper, -diff));
            match batch_append(
                sink_id,
                &mut self.write_handle,
                upper,
                retract_upper,
                retractions,
            )
            .await
            {
                Ok(()) => {
                    self.current_upper = retract_upper;
                    break;
                }
                // `batch_append` logs the mismatch. Read again at the new upper.
                Err(_) => continue,
            }
        }
        read_handle.expire().await;
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

#[cfg(test)]
mod tests;
