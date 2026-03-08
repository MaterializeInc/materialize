// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Standing query handler task.
//!
//! Each standing query runs an independent handler task that:
//! 1. Receives subscribe batches forwarded by the coordinator.
//! 2. Receives flush notifications from clients (write_ts → request_ids).
//! 3. Buffers result rows per request_id.
//! 4. When the subscribe frontier advances past a write timestamp T,
//!    delivers results via oneshot channels in the shared client.
//!
//! Param rows are self-retracting (written as +1 at ts, -1 at ts+1),
//! so no explicit retraction is needed.
//!
//! This task runs entirely off the coordinator loop.

use std::collections::BTreeMap;

use mz_compute_client::protocol::response::SubscribeBatch;
use mz_repr::{Datum, GlobalId, Row, SharedRow, Timestamp};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::standing_query_client::{StandingQueryExecuteClient, StandingQueryFlush};

/// Spawn a handler task for a standing query.
///
/// The caller creates the channels and the client beforehand, then passes
/// the receive halves to the handler and the send halves to the coordinator
/// and client respectively.
pub(crate) fn spawn_standing_query_handler(
    sink_id: GlobalId,
    client: StandingQueryExecuteClient,
    subscribe_rx: mpsc::UnboundedReceiver<SubscribeBatch>,
    flush_rx: mpsc::UnboundedReceiver<StandingQueryFlush>,
) {
    mz_ore::task::spawn(
        || format!("standing-query-handler-{sink_id}"),
        standing_query_handler_task(sink_id, client, subscribe_rx, flush_rx),
    );
}

async fn standing_query_handler_task(
    sink_id: GlobalId,
    client: StandingQueryExecuteClient,
    mut subscribe_rx: mpsc::UnboundedReceiver<SubscribeBatch>,
    mut flush_rx: mpsc::UnboundedReceiver<StandingQueryFlush>,
) {
    info!("standing query {sink_id}: handler task started");

    let mut in_flight: BTreeMap<Timestamp, Vec<u64>> = BTreeMap::new();
    let mut result_buffer: BTreeMap<u64, Vec<Row>> = BTreeMap::new();

    loop {
        tokio::select! {
            batch = subscribe_rx.recv() => {
                let Some(batch) = batch else {
                    // Coordinator dropped the sender — standing query is being dropped.
                    break;
                };
                // Before processing the batch, drain all available flush notifications.
                drain_flushes(&mut flush_rx, &mut in_flight);

                process_batch(
                    sink_id,
                    &client,
                    batch,
                    &mut in_flight,
                    &mut result_buffer,
                );
            }
            flush = flush_rx.recv() => {
                let Some(flush) = flush else {
                    // All clients dropped — but we keep running until subscribe_rx closes.
                    continue;
                };
                apply_flush(flush, &mut in_flight);
            }
        }
    }

    info!("standing query {sink_id}: handler task shutting down");
}

fn drain_flushes(
    flush_rx: &mut mpsc::UnboundedReceiver<StandingQueryFlush>,
    in_flight: &mut BTreeMap<Timestamp, Vec<u64>>,
) {
    while let Ok(flush) = flush_rx.try_recv() {
        apply_flush(flush, in_flight);
    }
}

fn apply_flush(flush: StandingQueryFlush, in_flight: &mut BTreeMap<Timestamp, Vec<u64>>) {
    in_flight
        .entry(flush.write_ts)
        .or_default()
        .extend(flush.request_ids);
}

fn process_batch(
    sink_id: GlobalId,
    client: &StandingQueryExecuteClient,
    batch: SubscribeBatch,
    in_flight: &mut BTreeMap<Timestamp, Vec<u64>>,
    result_buffer: &mut BTreeMap<u64, Vec<Row>>,
) {
    let SubscribeBatch {
        lower: _,
        upper,
        updates,
    } = batch;

    // Buffer positive diffs per request_id.
    match updates {
        Ok(rows) => {
            for (_ts, row, diff) in rows {
                if !diff.is_positive() {
                    continue;
                }

                let mut datums = row.iter();
                let request_id = match datums.next() {
                    Some(Datum::UInt64(id)) => id,
                    other => {
                        warn!(
                            "standing query {sink_id}: expected UInt64 request_id, got {:?}",
                            other
                        );
                        continue;
                    }
                };

                let result_row = {
                    let mut row_buf = SharedRow::get();
                    row_buf.packer().extend(datums);
                    row_buf.clone()
                };

                debug!("standing query {sink_id}: buffering result for request {request_id}");
                result_buffer
                    .entry(request_id)
                    .or_default()
                    .push(result_row);
            }
        }
        Err(err) => {
            warn!("standing query {sink_id}: subscribe error: {err}");
            return;
        }
    }

    // Deliver completed requests whose timestamps the frontier has advanced past.
    let completed_timestamps: Vec<Timestamp> = in_flight
        .keys()
        .copied()
        .take_while(|ts| !upper.less_equal(ts))
        .collect();

    for ts in completed_timestamps {
        if let Some(request_ids) = in_flight.remove(&ts) {
            for request_id in request_ids {
                let results = result_buffer.remove(&request_id).unwrap_or_default();
                if let Some(tx) = client.take_result_sender(&request_id) {
                    debug!(
                        "standing query {sink_id}: delivering {} rows for request {request_id}",
                        results.len()
                    );
                    let _ = tx.send(results);
                }
            }
        }
    }
}
