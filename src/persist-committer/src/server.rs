// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Core service logic for the persist committer.
//!
//! `PersistCommitter` owns the only authoritative `Consensus` implementation
//! in an `environmentd`. All `Head`, `Scan`, `CompareAndSet`, `Truncate`, and
//! `ListKeys` calls coming over gRPC funnel through here; the gRPC wiring
//! itself is in a follow-up.

use std::sync::Arc;

use futures::TryStreamExt;
use mz_ore::task::spawn;
use mz_persist::location::{CaSResult, Consensus, ExternalError, SeqNo, VersionedData};
use tracing::{debug, warn};

use crate::cache::ShardCache;
use crate::metrics::CommitterMetrics;
use crate::subscribe::SubscriberRegistry;

/// In-envd persist consensus committer.
///
/// Acts as a proxy over a backing `Consensus` implementation (typically
/// `PostgresConsensus`) while maintaining a monotonic per-shard read cache
/// and a per-shard subscriber broadcast.
#[derive(Debug)]
pub struct PersistCommitter {
    consensus: Arc<dyn Consensus + Send + Sync>,
    cache: Arc<ShardCache>,
    registry: Arc<SubscriberRegistry>,
    metrics: CommitterMetrics,
}

impl PersistCommitter {
    pub fn new(
        consensus: Arc<dyn Consensus + Send + Sync>,
        cache: Arc<ShardCache>,
        registry: Arc<SubscriberRegistry>,
        metrics: CommitterMetrics,
    ) -> Self {
        Self {
            consensus,
            cache,
            registry,
            metrics,
        }
    }

    /// Read the latest `VersionedData` for `shard`, populating the cache on miss.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn head_inner(&self, shard: &str) -> Result<Option<VersionedData>, ExternalError> {
        if let Some(cached) = self.cache.get(shard) {
            self.metrics
                .cache_hits_total
                .with_label_values(&["head"])
                .inc();
            debug!(shard, seqno = ?cached.seqno, "head cache hit");
            return Ok(Some(cached));
        }
        self.metrics
            .cache_misses_total
            .with_label_values(&["head"])
            .inc();
        let head = self.consensus.head(shard).await?;
        if let Some(v) = &head {
            debug!(shard, seqno = ?v.seqno, "head cache miss, populating from underlying");
            self.cache.insert(shard, v.clone());
        }
        Ok(head)
    }

    /// Pass-through scan; intentionally not cached.
    pub async fn scan_inner(
        &self,
        shard: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        self.consensus.scan(shard, from, limit).await
    }

    /// Forward a `compare_and_set` to the backing store.
    ///
    /// On `Committed`, monotonic-merge the new value into the cache and publish
    /// it to subscribers. On `ExpectationMismatch`, spawn a fire-and-forget
    /// `head()` to refresh the cache so the caller's follow-up
    /// `fetch_current_state` can be served from cache; the underlying trait
    /// does not return current state on mismatch.
    #[tracing::instrument(level = "debug", skip(self, new), fields(seqno = ?new.seqno))]
    pub async fn cas_inner(
        &self,
        shard: &str,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        let result = self.consensus.compare_and_set(shard, new.clone()).await?;
        match result {
            CaSResult::Committed => {
                debug!(shard, seqno = ?new.seqno, "committer CaS committed");
                self.cache.insert(shard, new.clone());
                self.registry.publish(shard, new);
            }
            CaSResult::ExpectationMismatch => {
                debug!(shard, seqno = ?new.seqno, "committer CaS mismatch, refreshing");
                self.spawn_refresh(shard.to_string());
            }
        }
        Ok(result)
    }

    /// Forward a `truncate` to the backing store. The cache is unaffected
    /// because truncation removes only historical sequence numbers.
    pub async fn truncate_inner(
        &self,
        shard: &str,
        seqno: SeqNo,
    ) -> Result<Option<usize>, ExternalError> {
        self.consensus.truncate(shard, seqno).await
    }

    /// Collect the backing store's `list_keys` stream into a vector. This is
    /// an administrative operation; not on the hot path.
    pub async fn list_keys_inner(&self) -> Result<Vec<String>, ExternalError> {
        let stream = self.consensus.list_keys();
        stream.try_collect().await
    }

    /// Register a subscriber for `shard`, returning the broadcast receiver and
    /// the snapshot (current cached state) the caller should emit first.
    pub async fn subscribe_inner(
        &self,
        shard: &str,
    ) -> Result<
        (
            Option<VersionedData>,
            tokio::sync::broadcast::Receiver<VersionedData>,
            crate::cache::SubscriberToken,
        ),
        ExternalError,
    > {
        let snapshot = self.head_inner(shard).await?;
        let token = self.cache.subscribe(shard);
        let rx = self.registry.register(shard);
        Ok((snapshot, rx, token))
    }

    /// Convenience accessor for tests and the in-process adapter.
    pub fn cache(&self) -> &Arc<ShardCache> {
        &self.cache
    }

    /// Spawn a background `head()` to refresh the cache for `shard`. Failures
    /// are logged and dropped; the TTL refresh task is the safety net.
    fn spawn_refresh(&self, shard: String) {
        let consensus = Arc::clone(&self.consensus);
        let cache = Arc::clone(&self.cache);
        let registry = Arc::clone(&self.registry);
        spawn(|| "persist_committer::cas_mismatch_refresh", async move {
            match consensus.head(&shard).await {
                Ok(Some(v)) => {
                    let prev = cache.get(&shard).map(|p| p.seqno);
                    let new_seqno = v.seqno;
                    cache.insert(&shard, v.clone());
                    if Some(new_seqno) != prev {
                        registry.publish(&shard, v);
                    }
                }
                Ok(None) => {}
                Err(e) => warn!(shard, error = %e, "cas_mismatch_refresh head failed"),
            }
        });
    }
}

// gRPC service wiring.

use std::pin::Pin;

use bytes::Bytes;
use futures::Stream;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{Request, Response, Status, async_trait};

use crate::proto::proto_persist_consensus_server::{
    ProtoPersistConsensus, ProtoPersistConsensusServer,
};
use crate::proto::{
    ProtoCompareAndSetOk, ProtoCompareAndSetRequest, ProtoCompareAndSetResponse, ProtoDeterminacy,
    ProtoHeadOk, ProtoHeadRequest, ProtoHeadResponse, ProtoListKeysOk, ProtoListKeysRequest,
    ProtoListKeysResponse, ProtoOperationError, ProtoScanOk, ProtoScanRequest, ProtoScanResponse,
    ProtoSubscribeMessage, ProtoSubscribeRequest, ProtoTruncateOk, ProtoTruncateRequest,
    ProtoTruncateResponse, ProtoVersionedData, proto_compare_and_set_response, proto_head_response,
    proto_list_keys_response, proto_scan_response, proto_subscribe_message,
    proto_truncate_response,
};

type SubscribeStream =
    Pin<Box<dyn Stream<Item = std::result::Result<ProtoSubscribeMessage, Status>> + Send>>;

fn to_proto(v: VersionedData) -> ProtoVersionedData {
    ProtoVersionedData {
        seqno: v.seqno.0,
        data: v.data.to_vec(),
    }
}

fn from_proto(p: ProtoVersionedData) -> VersionedData {
    VersionedData {
        seqno: SeqNo(p.seqno),
        data: Bytes::from(p.data),
    }
}

fn to_proto_error(e: ExternalError) -> ProtoOperationError {
    let determinacy = match &e {
        ExternalError::Determinate(_) => ProtoDeterminacy::Determinate,
        ExternalError::Indeterminate(_) => ProtoDeterminacy::Indeterminate,
    };
    ProtoOperationError {
        determinacy: i32::from(determinacy),
        message: e.to_string(),
    }
}

#[async_trait]
impl ProtoPersistConsensus for PersistCommitter {
    async fn head(
        &self,
        request: Request<ProtoHeadRequest>,
    ) -> std::result::Result<Response<ProtoHeadResponse>, Status> {
        let shard = request.into_inner().shard;
        let result = match self.head_inner(&shard).await {
            Ok(current) => proto_head_response::Result::Ok(ProtoHeadOk {
                current: current.map(to_proto),
            }),
            Err(e) => proto_head_response::Result::Err(to_proto_error(e)),
        };
        Ok(Response::new(ProtoHeadResponse {
            result: Some(result),
        }))
    }

    async fn scan(
        &self,
        request: Request<ProtoScanRequest>,
    ) -> std::result::Result<Response<ProtoScanResponse>, Status> {
        let r = request.into_inner();
        let limit = usize::try_from(r.limit).unwrap_or(usize::MAX);
        let result = match self.scan_inner(&r.shard, SeqNo(r.from), limit).await {
            Ok(versions) => proto_scan_response::Result::Ok(ProtoScanOk {
                versions: versions.into_iter().map(to_proto).collect(),
            }),
            Err(e) => proto_scan_response::Result::Err(to_proto_error(e)),
        };
        Ok(Response::new(ProtoScanResponse {
            result: Some(result),
        }))
    }

    async fn compare_and_set(
        &self,
        request: Request<ProtoCompareAndSetRequest>,
    ) -> std::result::Result<Response<ProtoCompareAndSetResponse>, Status> {
        let r = request.into_inner();
        let new = from_proto(
            r.new
                .ok_or_else(|| Status::invalid_argument("missing new VersionedData"))?,
        );
        let result = match self.cas_inner(&r.shard, new).await {
            Ok(cas_result) => proto_compare_and_set_response::Result::Ok(ProtoCompareAndSetOk {
                committed: matches!(cas_result, CaSResult::Committed),
            }),
            Err(e) => proto_compare_and_set_response::Result::Err(to_proto_error(e)),
        };
        Ok(Response::new(ProtoCompareAndSetResponse {
            result: Some(result),
        }))
    }

    async fn truncate(
        &self,
        request: Request<ProtoTruncateRequest>,
    ) -> std::result::Result<Response<ProtoTruncateResponse>, Status> {
        let r = request.into_inner();
        let result = match self.truncate_inner(&r.shard, SeqNo(r.seqno)).await {
            Ok(deleted) => proto_truncate_response::Result::Ok(ProtoTruncateOk {
                deleted: deleted.map(|n| u64::try_from(n).unwrap_or(u64::MAX)),
            }),
            Err(e) => proto_truncate_response::Result::Err(to_proto_error(e)),
        };
        Ok(Response::new(ProtoTruncateResponse {
            result: Some(result),
        }))
    }

    async fn list_keys(
        &self,
        _request: Request<ProtoListKeysRequest>,
    ) -> std::result::Result<Response<ProtoListKeysResponse>, Status> {
        let result = match self.list_keys_inner().await {
            Ok(keys) => proto_list_keys_response::Result::Ok(ProtoListKeysOk { keys }),
            Err(e) => proto_list_keys_response::Result::Err(to_proto_error(e)),
        };
        Ok(Response::new(ProtoListKeysResponse {
            result: Some(result),
        }))
    }

    type SubscribeStream = SubscribeStream;

    async fn subscribe(
        &self,
        request: Request<ProtoSubscribeRequest>,
    ) -> std::result::Result<Response<Self::SubscribeStream>, Status> {
        let shard = request.into_inner().shard;
        // Subscribe still maps subscribe_inner failures to tonic::Status because
        // the only failure mode is establishing the underlying read; once the
        // stream is open, operation errors become an end-of-stream condition.
        let (snapshot, rx, token) = self.subscribe_inner(&shard).await.map_err(|e| {
            // Preserve determinacy through the message body; clients that care
            // about retry semantics on Subscribe must re-establish anyway.
            Status::internal(e.to_string())
        })?;
        let snapshot_proto = snapshot.map(to_proto).unwrap_or_default();
        let stream = async_stream::try_stream! {
            yield ProtoSubscribeMessage {
                kind: Some(proto_subscribe_message::Kind::Snapshot(snapshot_proto)),
            };
            let mut rx = BroadcastStream::new(rx);
            while let Some(item) = futures::StreamExt::next(&mut rx).await {
                match item {
                    Ok(v) => yield ProtoSubscribeMessage {
                        kind: Some(proto_subscribe_message::Kind::Diff(to_proto(v))),
                    },
                    Err(_lagged) => {
                        // Lagged: subscriber must resync via Head. Drop and continue.
                        continue;
                    }
                }
            }
            // Token is moved into the stream and dropped when the stream ends.
            drop(token);
        };
        Ok(Response::new(Box::pin(stream)))
    }
}

impl PersistCommitter {
    /// Build a tonic service that shares this committer instance with any
    /// in-process consumers (e.g. `InProcessConsensus`).
    pub fn into_service(self: Arc<Self>) -> ProtoPersistConsensusServer<PersistCommitter> {
        ProtoPersistConsensusServer::from_arc(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mz_persist::location::SeqNo;
    use mz_persist::mem::MemConsensus;

    fn v(seqno: u64, byte: u8) -> VersionedData {
        VersionedData {
            seqno: SeqNo(seqno),
            data: Bytes::from(vec![byte]),
        }
    }

    fn fixture() -> (Arc<MemConsensus>, PersistCommitter) {
        let consensus = Arc::new(MemConsensus::default());
        let consensus_dyn: Arc<dyn Consensus + Send + Sync> =
            Arc::<MemConsensus>::clone(&consensus);
        let cache = Arc::new(ShardCache::new(100));
        let registry = Arc::new(SubscriberRegistry::new());
        let metrics = CommitterMetrics::for_tests();
        let committer = PersistCommitter::new(consensus_dyn, cache, registry, metrics);
        (consensus, committer)
    }

    #[mz_ore::test(tokio::test)]
    async fn head_reads_from_consensus_on_miss() {
        let (consensus, committer) = fixture();
        let _ = consensus.compare_and_set("s1", v(0, 0xAA)).await.unwrap();
        let got = committer.head_inner("s1").await.unwrap();
        assert_eq!(got.unwrap().seqno, SeqNo(0));
    }

    #[mz_ore::test(tokio::test)]
    async fn head_returns_cached_value_without_underlying() {
        // Underlying is empty; cache is pre-populated; head must return cache.
        let consensus: Arc<dyn Consensus + Send + Sync> = Arc::new(MemConsensus::default());
        let cache = Arc::new(ShardCache::new(100));
        cache.insert("s1", v(5, 0xCC));
        let registry = Arc::new(SubscriberRegistry::new());
        let committer =
            PersistCommitter::new(consensus, cache, registry, CommitterMetrics::for_tests());
        let got = committer.head_inner("s1").await.unwrap();
        assert_eq!(got.unwrap().seqno, SeqNo(5));
    }

    #[mz_ore::test(tokio::test)]
    async fn cas_committed_updates_cache_and_publishes() {
        let (_, committer) = fixture();
        let mut sub = committer.registry.register("s1");

        let result = committer.cas_inner("s1", v(0, 0xAA)).await.unwrap();
        assert_eq!(result, CaSResult::Committed);

        let pushed = sub.recv().await.unwrap();
        assert_eq!(pushed.seqno, SeqNo(0));

        let head = committer.head_inner("s1").await.unwrap().unwrap();
        assert_eq!(head.seqno, SeqNo(0));
    }

    #[mz_ore::test(tokio::test)]
    async fn cas_mismatch_refreshes_cache_async() {
        let (consensus, committer) = fixture();
        // Underlying gets a value the committer's cache doesn't know about.
        let _ = consensus.compare_and_set("s1", v(0, 0xAA)).await.unwrap();
        // CaS against an expected predecessor that does not exist (seqno=5).
        let result = committer.cas_inner("s1", v(5, 0xBB)).await.unwrap();
        assert_eq!(result, CaSResult::ExpectationMismatch);

        // Background refresh eventually populates the cache. Poll a few
        // iterations rather than racing the spawned task.
        for _ in 0..50 {
            if committer.cache.get("s1").is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(committer.cache.get("s1").unwrap().seqno, SeqNo(0));
    }

    #[mz_ore::test(tokio::test)]
    async fn truncate_passthrough() {
        let (consensus, committer) = fixture();
        let _ = consensus.compare_and_set("s1", v(0, 0xAA)).await.unwrap();
        let _ = consensus.compare_and_set("s1", v(1, 0xBB)).await.unwrap();
        // Truncate everything below SeqNo(1).
        let deleted = committer.truncate_inner("s1", SeqNo(1)).await.unwrap();
        assert_eq!(deleted, Some(1));
    }

    #[mz_ore::test(tokio::test)]
    async fn list_keys_returns_underlying_keys() {
        let (consensus, committer) = fixture();
        let _ = consensus.compare_and_set("s1", v(0, 0xAA)).await.unwrap();
        let _ = consensus.compare_and_set("s2", v(0, 0xBB)).await.unwrap();
        let mut keys = committer.list_keys_inner().await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["s1".to_string(), "s2".to_string()]);
    }
}
