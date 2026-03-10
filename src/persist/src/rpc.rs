// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A [`Consensus`] implementation backed by the two-tier shared log service.
//!
//! Writes go to the acceptor (blind append), then the learner is queried for
//! the CAS result. Reads go directly to the learner.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use tonic::transport::Channel;

use crate::generated::consensus_service::consensus_acceptor_client::ConsensusAcceptorClient;
use crate::generated::consensus_service::consensus_learner_client::ConsensusLearnerClient;
use crate::generated::consensus_service::{
    ProtoAppendRequest, ProtoAwaitResultRequest, ProtoCasProposal, ProtoHeadRequest,
    ProtoListKeysRequest, ProtoScanRequest, ProtoTruncateProposal, ProtoVersionedData,
    ProtoWalProposal, proto_wal_proposal,
};
use crate::location::{CaSResult, Consensus, ExternalError, ResultStream, SeqNo, VersionedData};

/// Configuration for an [`RpcConsensus`] client.
#[derive(Debug, Clone)]
pub struct RpcConsensusConfig {
    /// The gRPC endpoint URI, e.g. `http://host:port`.
    pub endpoint: String,
    /// Number of independent HTTP/2 connections to open. More connections
    /// reduce h2 mux contention under high concurrency. Defaults to 1.
    pub pool_size: usize,
    /// Timeout for establishing each TCP connection. Defaults to 5s.
    pub connect_timeout: Duration,
    /// Timeout applied to each individual RPC request. Defaults to 5s.
    pub request_timeout: Duration,
    /// Interval between HTTP/2 keep-alive PINGs on idle connections.
    /// Detects dead connections without waiting for the next request.
    /// Defaults to 3s (matching compute/storage gRPC clients).
    pub http2_keep_alive_interval: Duration,
    /// How long to wait for a keep-alive PING response before considering
    /// the connection dead. Defaults to 60s.
    pub http2_keep_alive_timeout: Duration,
}

impl RpcConsensusConfig {
    /// Parses an `rpc://host:port` URI into an [`RpcConsensusConfig`].
    pub fn try_from_uri(url: &url::Url) -> Result<Self, ExternalError> {
        let host = url
            .host_str()
            .ok_or_else(|| ExternalError::from(anyhow!("rpc consensus URI missing host")))?;
        let port = url
            .port()
            .ok_or_else(|| ExternalError::from(anyhow!("rpc consensus URI missing port")))?;
        Ok(RpcConsensusConfig {
            endpoint: format!("http://{}:{}", host, port),
            pool_size: 1,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            http2_keep_alive_interval: Duration::from_secs(3),
            http2_keep_alive_timeout: Duration::from_secs(60),
        })
    }
}

/// A [`Consensus`] implementation backed by the two-tier shared log service.
///
/// Writes (CAS, truncate) go to the acceptor as blind proposals, then the
/// learner is queried for the result after materialization. Reads (head, scan,
/// list_keys) go directly to the learner.
///
/// Maintains a pool of independent HTTP/2 connections with round-robin
/// selection to spread h2 mux contention under high concurrency.
#[derive(Debug)]
pub struct RpcConsensus {
    acceptors: Vec<ConsensusAcceptorClient<Channel>>,
    learners: Vec<ConsensusLearnerClient<Channel>>,
    next: AtomicUsize,
}

impl RpcConsensus {
    /// Opens `config.pool_size` connections to the remote consensus service.
    ///
    /// Both the acceptor and learner services are expected at the same endpoint
    /// (they're served by the same process today). Channels are created with
    /// `connect_lazy` so tonic will automatically reconnect after a dead
    /// connection is detected.
    pub async fn open(config: RpcConsensusConfig) -> Result<Self, ExternalError> {
        let pool_size = config.pool_size.max(1);
        let mut acceptors = Vec::with_capacity(pool_size);
        let mut learners = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let channel = Channel::from_shared(config.endpoint.clone())
                .map_err(|e| ExternalError::from(anyhow!("invalid rpc endpoint: {}", e)))?
                .connect_timeout(config.connect_timeout)
                .timeout(config.request_timeout)
                .http2_keep_alive_interval(config.http2_keep_alive_interval)
                .keep_alive_timeout(config.http2_keep_alive_timeout)
                .keep_alive_while_idle(true)
                .connect_lazy();
            acceptors.push(ConsensusAcceptorClient::new(channel.clone()));
            learners.push(ConsensusLearnerClient::new(channel));
        }
        let consensus = RpcConsensus {
            acceptors,
            learners,
            next: AtomicUsize::new(0),
        };

        // Health check: verify the endpoint is reachable.
        let mut learner = consensus.get_learner();
        learner
            .head(ProtoHeadRequest {
                key: String::new(),
            })
            .await
            .map_err(|e| ExternalError::from(anyhow!("rpc health check failed: {}", e)))?;

        Ok(consensus)
    }

    fn get_acceptor(&self) -> ConsensusAcceptorClient<Channel> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.acceptors.len();
        self.acceptors[idx].clone()
    }

    fn get_learner(&self) -> ConsensusLearnerClient<Channel> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.learners.len();
        self.learners[idx].clone()
    }
}

fn from_proto_versioned_data(p: ProtoVersionedData) -> VersionedData {
    VersionedData {
        seqno: SeqNo(p.seqno),
        data: Bytes::from(p.data),
    }
}

#[async_trait]
impl Consensus for RpcConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        let mut client = self.get_learner();
        Box::pin(try_stream! {
            let response = client
                .list_keys(ProtoListKeysRequest {})
                .await
                .map_err(|e| ExternalError::from(anyhow!("rpc list_keys failed: {}", e)))?;
            let mut stream = response.into_inner();
            loop {
                match stream.message().await {
                    Ok(Some(msg)) => yield msg.key,
                    Ok(None) => break,
                    Err(e) => Err(ExternalError::from(anyhow!("rpc list_keys stream error: {}", e)))?,
                }
            }
        })
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let mut client = self.get_learner();
        let response = client
            .head(ProtoHeadRequest {
                key: key.to_string(),
            })
            .await
            .map_err(|e| ExternalError::from(anyhow!("rpc head failed: {}", e)))?;
        let inner = response.into_inner();
        Ok(inner.data.map(from_proto_versioned_data))
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        // 1. Append proposal to acceptor (blocks until group commit flush).
        let mut acceptor = self.get_acceptor();
        let receipt = acceptor
            .append(ProtoAppendRequest {
                proposal: Some(ProtoWalProposal {
                    op: Some(proto_wal_proposal::Op::Cas(ProtoCasProposal {
                        key: key.to_string(),
                        expected: expected.map(|s| s.0),
                        new_seqno: new.seqno.0,
                        data: new.data.to_vec(),
                    })),
                }),
            })
            .await
            .map_err(|e| ExternalError::from(anyhow!("rpc append failed: {}", e)))?
            .into_inner();

        // 2. Wait for learner to materialize and return CAS result.
        let mut learner = self.get_learner();
        let result = learner
            .await_cas_result(ProtoAwaitResultRequest {
                batch_number: receipt.batch_number,
                position: receipt.position,
            })
            .await
            .map_err(|e| ExternalError::from(anyhow!("rpc await_cas_result failed: {}", e)))?
            .into_inner();

        if result.committed {
            Ok(CaSResult::Committed)
        } else {
            Ok(CaSResult::ExpectationMismatch)
        }
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let mut client = self.get_learner();
        let response = client
            .scan(ProtoScanRequest {
                key: key.to_string(),
                from: from.0,
                limit: u64::try_from(limit).unwrap_or(u64::MAX),
            })
            .await
            .map_err(|e| ExternalError::from(anyhow!("rpc scan failed: {}", e)))?;
        let inner = response.into_inner();
        Ok(inner.data.into_iter().map(from_proto_versioned_data).collect())
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        // 1. Append truncate proposal to acceptor.
        let mut acceptor = self.get_acceptor();
        let receipt = acceptor
            .append(ProtoAppendRequest {
                proposal: Some(ProtoWalProposal {
                    op: Some(proto_wal_proposal::Op::Truncate(ProtoTruncateProposal {
                        key: key.to_string(),
                        seqno: seqno.0,
                    })),
                }),
            })
            .await
            .map_err(|e| ExternalError::from(anyhow!("rpc append failed: {}", e)))?
            .into_inner();

        // 2. Wait for learner to materialize and return truncate result.
        let mut learner = self.get_learner();
        let result = learner
            .await_truncate_result(ProtoAwaitResultRequest {
                batch_number: receipt.batch_number,
                position: receipt.position,
            })
            .await
            .map_err(|e| ExternalError::from(anyhow!("rpc await_truncate_result failed: {}", e)))?
            .into_inner();

        Ok(result
            .deleted
            .map(|d| usize::try_from(d).expect("deleted count fits in usize")))
    }
}

/// Opens an [`RpcConsensus`] behind an `Arc<dyn Consensus>`.
pub async fn open(config: RpcConsensusConfig) -> Result<Arc<dyn Consensus>, ExternalError> {
    Ok(Arc::new(RpcConsensus::open(config).await?))
}
