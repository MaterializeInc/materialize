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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mz_persist_client::write::WriteHandle;
use mz_repr::{CatalogItemId, Diff, GlobalId, Row, Timestamp, TimestampManipulation};
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, warn};
use uuid::Uuid;

/// Notification sent from the batcher to the handler task when param rows
/// have been written, so the handler can track in-flight request IDs.
#[derive(Debug)]
pub struct StandingQueryFlush {
    pub sink_id: GlobalId,
    pub write_ts: Timestamp,
    pub request_ids: Vec<Uuid>,
    pub param_rows: Vec<(Uuid, Row)>,
}

/// A request to write a param row, sent from `execute()` to the batcher task.
#[derive(Debug)]
struct WriteRequest {
    request_id: Uuid,
    param_row: Row,
}

/// Commands sent to the batcher task.
#[derive(Debug)]
enum BatcherCmd {
    /// Write param rows (from execute()).
    Write(WriteRequest),
    /// Retract previously-delivered param rows (from handler).
    Retract(Vec<Row>),
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
    result_senders: Arc<Mutex<BTreeMap<Uuid, oneshot::Sender<Vec<Row>>>>>,
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
        }
    }

    /// Execute a standing query with the given parameters.
    ///
    /// Sends the param row to the batcher task for batched writing, then
    /// waits for results from the subscribe handler.
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
        result_rx
            .await
            .map_err(|_| StandingQueryExecuteError::ResultChannelClosed)
    }

    /// Deliver results for a request_id. Called by the handler task.
    ///
    /// Returns the oneshot sender if the request_id is found, None otherwise.
    pub fn take_result_sender(&self, request_id: &Uuid) -> Option<oneshot::Sender<Vec<Row>>> {
        let mut senders = self.result_senders.lock().expect("lock poisoned");
        senders.remove(request_id)
    }

    /// Queue retractions for param rows via the batcher task.
    pub fn retract(&self, param_rows: Vec<Row>) {
        let _ = self.batcher_tx.send(BatcherCmd::Retract(param_rows));
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
        "standing query {sink_id}: batcher started, shared_upper={current_upper}, initial_target={}",
        *advance_upper_rx.borrow()
    );

    const MIN_COLLECT: Duration = Duration::from_millis(1);
    const MAX_COLLECT: Duration = Duration::from_millis(50);
    let mut last_append_duration = MIN_COLLECT;

    loop {
        // Always apply the latest upper target before doing anything else.
        // This is critical: the subscribe can't produce output until the
        // param shard upper advances past its as_of. Using borrow_and_update
        // (not has_changed) ensures we catch values sent before we started
        // listening, not just new notifications.
        {
            let target = *advance_upper_rx.borrow_and_update();
            advance_upper(sink_id, &mut write_handle, &mut current_upper, target).await;
        }

        // Wait for at least one command or an upper-advance notification.
        tokio::select! {
            cmd = batcher_rx.recv() => {
                let Some(cmd) = cmd else {
                    break;
                };

                let mut writes: Vec<WriteRequest> = Vec::new();
                let mut retractions: Vec<Row> = Vec::new();

                apply_cmd(cmd, &mut writes, &mut retractions);

                // Drain what's already buffered.
                while let Ok(cmd) = batcher_rx.try_recv() {
                    apply_cmd(cmd, &mut writes, &mut retractions);
                }

                // Adaptively collect more: wait up to 2x the last append
                // duration for additional requests. Under light load, this
                // window is tiny (~1ms). Under heavy load, appends take
                // longer so we collect bigger batches automatically.
                let collect_budget = (2 * last_append_duration).clamp(MIN_COLLECT, MAX_COLLECT);
                let collect_deadline = Instant::now() + collect_budget;
                loop {
                    let remaining = collect_deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    match tokio::time::timeout(remaining, batcher_rx.recv()).await {
                        Ok(Some(cmd)) => apply_cmd(cmd, &mut writes, &mut retractions),
                        Ok(None) => break,
                        Err(_) => break,
                    }
                }

                if writes.is_empty() && retractions.is_empty() {
                    continue;
                }

                let append_start = Instant::now();
                match batch_append(
                    sink_id,
                    &mut write_handle,
                    &mut current_upper,
                    &writes,
                    &retractions,
                )
                .await
                {
                    Ok(write_ts) => {
                        last_append_duration = append_start.elapsed();
                        if !writes.is_empty() {
                            let request_ids: Vec<Uuid> =
                                writes.iter().map(|w| w.request_id).collect();
                            let param_rows: Vec<(Uuid, Row)> = writes
                                .into_iter()
                                .map(|w| (w.request_id, w.param_row))
                                .collect();
                            debug!(
                                "standing query {sink_id}: batched {} param writes at ts {write_ts}",
                                request_ids.len()
                            );
                            let _ = flush_tx.send(StandingQueryFlush {
                                sink_id,
                                write_ts,
                                request_ids,
                                param_rows,
                            });
                        }
                    }
                    Err(e) => {
                        last_append_duration = append_start.elapsed();
                        warn!("standing query {sink_id}: batch append failed: {e}");
                    }
                }
            }
            result = advance_upper_rx.changed() => {
                if result.is_err() {
                    break;
                }
                let target = *advance_upper_rx.borrow_and_update();
                advance_upper(sink_id, &mut write_handle, &mut current_upper, target).await;
            }
        }
    }
}

fn apply_cmd(cmd: BatcherCmd, writes: &mut Vec<WriteRequest>, retractions: &mut Vec<Row>) {
    match cmd {
        BatcherCmd::Write(req) => writes.push(req),
        BatcherCmd::Retract(rows) => retractions.extend(rows),
    }
}

/// Append a batch of param writes and retractions in a single
/// `compare_and_append`. Returns the timestamp at which the writes landed.
async fn batch_append(
    sink_id: GlobalId,
    write_handle: &mut WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    current_upper: &mut Timestamp,
    writes: &[WriteRequest],
    retractions: &[Row],
) -> Result<Timestamp, String> {
    let upper = *current_upper;
    let new_upper = TimestampManipulation::step_forward(&upper);

    let mut updates: Vec<((SourceData, ()), Timestamp, i64)> =
        Vec::with_capacity(writes.len() + retractions.len());

    for req in writes {
        updates.push((
            (SourceData(Ok(req.param_row.clone())), ()),
            upper,
            Diff::from(1i64).into_inner(),
        ));
    }
    for row in retractions {
        updates.push((
            (SourceData(Ok(row.clone())), ()),
            upper,
            Diff::from(-1i64).into_inner(),
        ));
    }

    debug!(
        "standing query {sink_id}: batch append {upper}..{new_upper}, {} updates",
        updates.len()
    );

    let res = write_handle
        .compare_and_append(
            updates,
            Antichain::from_elem(upper),
            Antichain::from_elem(new_upper),
        )
        .await
        .expect("valid persist usage");

    match res {
        Ok(()) => {
            *current_upper = new_upper;
            Ok(upper)
        }
        Err(mismatch) => {
            let err = format!(
                "upper mismatch: expected {:?}, actual {:?}",
                mismatch.expected, mismatch.current
            );
            warn!("standing query {sink_id}: {err}");
            if let Some(actual) = mismatch.current.into_option() {
                *current_upper = actual;
            }
            Err(err)
        }
    }
}

/// Advance the param shard's upper to at least `target`, without writing data.
async fn advance_upper(
    sink_id: GlobalId,
    write_handle: &mut WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    current_upper: &mut Timestamp,
    target: Timestamp,
) {
    let upper = *current_upper;
    if upper >= target {
        return;
    }

    debug!("standing query {sink_id}: advance upper {upper}..{target}");

    let res = write_handle
        .compare_and_append(
            Vec::<((SourceData, ()), Timestamp, i64)>::new(),
            Antichain::from_elem(upper),
            Antichain::from_elem(target),
        )
        .await
        .expect("valid persist usage");

    match res {
        Ok(()) => {
            *current_upper = target;
        }
        Err(mismatch) => {
            warn!(
                "standing query {sink_id}: upper advance mismatch: expected {:?}, actual {:?}",
                mismatch.expected, mismatch.current
            );
            if let Some(actual) = mismatch.current.into_option() {
                *current_upper = actual;
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StandingQueryExecuteError {
    #[error("failed to write param row: {0}")]
    WriteError(String),
    #[error("result channel closed (standing query may have been dropped)")]
    ResultChannelClosed,
}
