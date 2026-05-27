// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `Consensus` implementation that routes through the persist committer
//! gRPC service. Used by clusterds (and by `environmentd` itself over an
//! in-process channel) when the `persist_consensus_use_committer` flag is on.

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use mz_persist::location::{
    CaSResult, Consensus, ExternalError, ResultStream, SeqNo, VersionedData,
};
use mz_persist_committer::proto::proto_persist_consensus_client::ProtoPersistConsensusClient;
use mz_persist_committer::proto::{
    ProtoCompareAndSetRequest, ProtoHeadRequest, ProtoListKeysRequest, ProtoScanRequest,
    ProtoTruncateRequest, ProtoVersionedData,
};
use tonic::transport::Channel;

/// Client-side `Consensus` impl that forwards every operation to the persist
/// committer over gRPC.
#[derive(Debug, Clone)]
pub struct RpcConsensus {
    client: ProtoPersistConsensusClient<Channel>,
}

impl RpcConsensus {
    /// Construct a new `RpcConsensus` that uses `channel` to reach a persist
    /// committer.
    pub fn new(channel: Channel) -> Self {
        Self {
            client: ProtoPersistConsensusClient::new(channel),
        }
    }
}

fn into_external(e: tonic::Status) -> ExternalError {
    ExternalError::from(anyhow!("persist committer rpc: {}", e))
}

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

#[async_trait]
impl Consensus for RpcConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        let mut client = self.client.clone();
        let stream = async_stream::try_stream! {
            let resp = client
                .list_keys(ProtoListKeysRequest {})
                .await
                .map_err(into_external)?
                .into_inner();
            for key in resp.keys {
                yield key;
            }
        };
        Box::pin(stream)
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let mut client = self.client.clone();
        let resp = client
            .head(ProtoHeadRequest {
                shard: key.to_string(),
            })
            .await
            .map_err(into_external)?
            .into_inner();
        Ok(resp.current.map(from_proto))
    }

    async fn compare_and_set(
        &self,
        key: &str,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        let mut client = self.client.clone();
        let resp = client
            .compare_and_set(ProtoCompareAndSetRequest {
                shard: key.to_string(),
                new: Some(to_proto(new)),
            })
            .await
            .map_err(into_external)?
            .into_inner();
        if resp.committed {
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
        let resp = client
            .scan(ProtoScanRequest {
                shard: key.to_string(),
                from: from.0,
                limit: u64::try_from(limit).unwrap_or(u64::MAX),
            })
            .await
            .map_err(into_external)?
            .into_inner();
        Ok(resp.versions.into_iter().map(from_proto).collect())
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        let mut client = self.client.clone();
        let resp = client
            .truncate(ProtoTruncateRequest {
                shard: key.to_string(),
                seqno: seqno.0,
            })
            .await
            .map_err(into_external)?
            .into_inner();
        Ok(resp
            .deleted
            .map(|n| usize::try_from(n).unwrap_or(usize::MAX)))
    }
}
