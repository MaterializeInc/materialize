// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Server-side implementation of the bogo-consensus gRPC service.
//!
//! The state model intentionally mirrors `mz_persist::mem::MemConsensus`: each
//! key maps to an append-only `Vec<VersionedData>` ordered by sequence number,
//! and a compare-and-set succeeds iff the proposed seqno is exactly one more
//! than the current head.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use futures::Stream;
use mz_ore::cast::CastFrom;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::warn;

use crate::metrics::BogoMetrics;
use crate::proto::bogo_consensus_server::BogoConsensus;
use crate::proto::{
    CaSResult, CompareAndSetRequest, CompareAndSetResponse, HeadRequest, HeadResponse,
    ListKeysRequest, ListKeysResponse, ScanRequest, ScanResponse, TruncateRequest,
    TruncateResponse, VersionedData,
};

/// In-memory consensus state, exposed via gRPC.
///
/// The state is held behind a [`std::sync::Mutex`] (not `tokio::sync::Mutex`)
/// to match MemConsensus's documented avoidance of intermittent deadlocks under
/// the persist concurrency tests. The lock is held only for short, bounded
/// operations on the `BTreeMap`, so blocking the runtime is acceptable.
#[derive(Debug)]
pub struct BogoConsensusServer {
    data: Arc<Mutex<BTreeMap<String, Vec<VersionedData>>>>,
    metrics: Arc<BogoMetrics>,
}

impl BogoConsensusServer {
    pub fn new(metrics: Arc<BogoMetrics>) -> Self {
        Self {
            data: Arc::new(Mutex::new(BTreeMap::new())),
            metrics,
        }
    }

    /// Update the `shards_total` and `versions_total` gauges incrementally.
    /// Called from the hot path while holding `self.data`'s mutex, so the
    /// per-call cost must be O(1) — iterating the BTreeMap here adds ~100 µs
    /// per CAS at 10k shards and head-of-line-blocks every other op behind
    /// the same mutex.
    ///
    /// `shards_delta` is +1 for a CAS that created a new key, 0 otherwise.
    /// `versions_delta` is the net change to the total number of stored
    /// versions: +1 for a successful CAS append, -N for a truncate that
    /// removed N entries.
    fn bump_state_gauges(&self, shards_delta: i64, versions_delta: i64) {
        if shards_delta != 0 {
            self.metrics.shards_total.add(shards_delta);
        }
        if versions_delta != 0 {
            self.metrics.versions_total.add(versions_delta);
        }
    }
}

// i64::MAX as a u64; written this way to avoid an `as` conversion.
const SEQNO_MAX: u64 = u64::MAX >> 1;

fn observe<T>(
    metrics: &BogoMetrics,
    rpc: &'static str,
    start: Instant,
    result: Result<T, Status>,
) -> Result<T, Status> {
    let elapsed = start.elapsed().as_secs_f64();
    metrics
        .rpc_seconds
        .with_label_values(&[rpc])
        .observe(elapsed);
    let outcome = match &result {
        Ok(_) => "ok",
        Err(status) => match status.code() {
            tonic::Code::FailedPrecondition => "mismatch",
            _ => "err",
        },
    };
    metrics
        .rpc_completed
        .with_label_values(&[rpc, outcome])
        .inc();
    if let Err(status) = &result {
        warn!(rpc, code = ?status.code(), message = %status.message(), "rpc error");
    }
    result
}

#[async_trait::async_trait]
impl BogoConsensus for BogoConsensusServer {
    async fn head(&self, request: Request<HeadRequest>) -> Result<Response<HeadResponse>, Status> {
        let start = Instant::now();
        self.metrics.rpc_started.with_label_values(&["head"]).inc();
        let HeadRequest { key } = request.into_inner();

        let result = (|| {
            let store = self
                .data
                .lock()
                .map_err(|e| Status::internal(format!("lock poisoned: {e}")))?;
            let data = store.get(&key).and_then(|v| v.last()).cloned();
            if let Some(d) = &data {
                self.metrics
                    .rpc_bytes_out
                    .with_label_values(&["head"])
                    .inc_by(u64::cast_from(d.data.len()));
            }
            Ok(Response::new(HeadResponse { data }))
        })();

        observe(&self.metrics, "head", start, result)
    }

    async fn compare_and_set(
        &self,
        request: Request<CompareAndSetRequest>,
    ) -> Result<Response<CompareAndSetResponse>, Status> {
        let start = Instant::now();
        self.metrics
            .rpc_started
            .with_label_values(&["compare_and_set"])
            .inc();
        let CompareAndSetRequest { key, new } = request.into_inner();
        let new = new.ok_or_else(|| Status::invalid_argument("missing `new` field"))?;

        let result = (|| {
            if new.seqno > SEQNO_MAX {
                return Err(Status::invalid_argument(format!(
                    "sequence numbers must fit within [0, i64::MAX], received: {}",
                    new.seqno
                )));
            }
            // CAS: `new.seqno` must equal previous.seqno + 1 (or 0 if empty).
            // The trait phrases this as `new.seqno == previous.seqno.next()`,
            // but here we work with the raw u64 so we compute `expected` as
            // `new.seqno.checked_sub(1)` — `None` means "must be empty".
            let expected = new.seqno.checked_sub(1);
            let data_len = u64::cast_from(new.data.len());

            let mut store = self
                .data
                .lock()
                .map_err(|e| Status::internal(format!("lock poisoned: {e}")))?;

            let current = store.get(&key).and_then(|v| v.last()).map(|d| d.seqno);
            if current != expected {
                return Ok(Response::new(CompareAndSetResponse {
                    result: i32::from(CaSResult::ExpectationMismatch),
                }));
            }
            let is_new_key = !store.contains_key(&key);
            store.entry(key).or_default().push(new);
            drop(store);
            self.metrics
                .rpc_bytes_in
                .with_label_values(&["compare_and_set"])
                .inc_by(data_len);
            self.bump_state_gauges(if is_new_key { 1 } else { 0 }, 1);
            Ok(Response::new(CompareAndSetResponse {
                result: i32::from(CaSResult::Committed),
            }))
        })();

        observe(&self.metrics, "compare_and_set", start, result)
    }

    async fn scan(&self, request: Request<ScanRequest>) -> Result<Response<ScanResponse>, Status> {
        let start = Instant::now();
        self.metrics.rpc_started.with_label_values(&["scan"]).inc();
        let ScanRequest { key, from, limit } = request.into_inner();
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);

        let result = (|| {
            let store = self
                .data
                .lock()
                .map_err(|e| Status::internal(format!("lock poisoned: {e}")))?;
            let data = if let Some(values) = store.get(&key) {
                let from_idx = values.partition_point(|x| x.seqno < from);
                let slice = &values[from_idx..];
                let end = usize::min(limit, slice.len());
                slice[..end].to_vec()
            } else {
                Vec::new()
            };
            let bytes_out: u64 = data.iter().map(|d| u64::cast_from(d.data.len())).sum();
            self.metrics
                .rpc_bytes_out
                .with_label_values(&["scan"])
                .inc_by(bytes_out);
            Ok(Response::new(ScanResponse { data }))
        })();

        observe(&self.metrics, "scan", start, result)
    }

    async fn truncate(
        &self,
        request: Request<TruncateRequest>,
    ) -> Result<Response<TruncateResponse>, Status> {
        let start = Instant::now();
        self.metrics
            .rpc_started
            .with_label_values(&["truncate"])
            .inc();
        let TruncateRequest { key, seqno } = request.into_inner();

        let result = (|| {
            let mut store = self
                .data
                .lock()
                .map_err(|e| Status::internal(format!("lock poisoned: {e}")))?;
            // Mirror MemConsensus: error if seqno > current head, or if no data
            // exists for the key. Both map to FailedPrecondition (Determinate
            // on the client side).
            let current_head = store.get(&key).and_then(|v| v.last()).map(|d| d.seqno);
            let too_high = match current_head {
                None => true,
                Some(head) => head < seqno,
            };
            if too_high {
                return Err(Status::failed_precondition(format!(
                    "upper bound too high for truncate: {seqno}"
                )));
            }
            let mut deleted: u64 = 0;
            if let Some(values) = store.get_mut(&key) {
                let count_before = values.len();
                values.retain(|val| val.seqno >= seqno);
                deleted = u64::cast_from(count_before - values.len());
            }
            drop(store);
            let versions_delta = -i64::try_from(deleted).unwrap_or(i64::MAX);
            self.bump_state_gauges(0, versions_delta);
            Ok(Response::new(TruncateResponse {
                deleted: Some(deleted),
            }))
        })();

        observe(&self.metrics, "truncate", start, result)
    }

    type ListKeysStream =
        Pin<Box<dyn Stream<Item = Result<ListKeysResponse, Status>> + Send + 'static>>;

    async fn list_keys(
        &self,
        _request: Request<ListKeysRequest>,
    ) -> Result<Response<Self::ListKeysStream>, Status> {
        let start = Instant::now();
        self.metrics
            .rpc_started
            .with_label_values(&["list_keys"])
            .inc();

        let keys: Vec<String> = {
            let store = self
                .data
                .lock()
                .map_err(|e| Status::internal(format!("lock poisoned: {e}")))?;
            store.keys().cloned().collect()
        };

        // We collect under lock then drop it before streaming. The persist
        // trait documents list_keys as administrative-only, so the
        // collect-then-stream shape is fine.
        let (tx, rx) = mpsc::channel(64);
        mz_ore::task::spawn(|| "bogo_consensus::list_keys", async move {
            for key in keys {
                if tx.send(Ok(ListKeysResponse { key })).await.is_err() {
                    break;
                }
            }
        });

        let stream: Self::ListKeysStream = Box::pin(ReceiverStream::new(rx));
        let response = Response::new(stream);
        observe(&self.metrics, "list_keys", start, Ok(response))
    }
}
