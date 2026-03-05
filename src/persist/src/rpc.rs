// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A [`Consensus`] implementation backed by a gRPC service.

use std::sync::Arc;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use tonic::transport::Channel;

use crate::generated::consensus_service::consensus_service_client::ConsensusServiceClient;
use crate::generated::consensus_service::{
    ProtoCompareAndSetRequest, ProtoHeadRequest, ProtoListKeysRequest, ProtoScanRequest,
    ProtoTruncateRequest, ProtoVersionedData,
};
use crate::location::{CaSResult, Consensus, ExternalError, ResultStream, SeqNo, VersionedData};

/// Configuration for an [`RpcConsensus`] client.
#[derive(Debug, Clone)]
pub struct RpcConsensusConfig {
    /// The gRPC endpoint URI, e.g. `http://host:port`.
    pub endpoint: String,
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
        })
    }
}

/// A [`Consensus`] implementation that delegates to a remote gRPC
/// `ConsensusService`.
#[derive(Debug)]
pub struct RpcConsensus {
    client: ConsensusServiceClient<Channel>,
}

impl RpcConsensus {
    /// Opens a connection to the remote consensus service.
    pub async fn open(config: RpcConsensusConfig) -> Result<Self, ExternalError> {
        let channel = Channel::from_shared(config.endpoint.clone())
            .map_err(|e| ExternalError::from(anyhow!("invalid rpc endpoint: {}", e)))?
            .connect()
            .await
            .map_err(|e| ExternalError::from(anyhow!("rpc connect failed: {}", e)))?;
        Ok(RpcConsensus {
            client: ConsensusServiceClient::new(channel),
        })
    }
}

fn to_proto_versioned_data(v: &VersionedData) -> ProtoVersionedData {
    ProtoVersionedData {
        seqno: v.seqno.0,
        data: v.data.to_vec(),
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
        let mut client = self.client.clone();
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
        let mut client = self.client.clone();
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
        let mut client = self.client.clone();
        let response = client
            .compare_and_set(ProtoCompareAndSetRequest {
                key: key.to_string(),
                expected: expected.map(|s| s.0),
                new: Some(to_proto_versioned_data(&new)),
            })
            .await
            .map_err(|e| ExternalError::from(anyhow!("rpc compare_and_set failed: {}", e)))?;
        let inner = response.into_inner();
        if inner.committed {
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
        let mut client = self.client.clone();
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
        let mut client = self.client.clone();
        let response = client
            .truncate(ProtoTruncateRequest {
                key: key.to_string(),
                seqno: seqno.0,
            })
            .await
            .map_err(|e| ExternalError::from(anyhow!("rpc truncate failed: {}", e)))?;
        let inner = response.into_inner();
        Ok(inner.deleted.map(|d| d as usize))
    }
}

/// Opens an [`RpcConsensus`] behind an `Arc<dyn Consensus>`.
pub async fn open(config: RpcConsensusConfig) -> Result<Arc<dyn Consensus>, ExternalError> {
    Ok(Arc::new(RpcConsensus::open(config).await?))
}
