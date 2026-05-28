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
    CaSResult, Consensus, Determinate, ExternalError, Indeterminate, ResultStream, SeqNo,
    VersionedData,
};
use mz_persist_committer::proto::proto_persist_consensus_client::ProtoPersistConsensusClient;
use mz_persist_committer::proto::{
    ProtoCompareAndSetRequest, ProtoDeterminacy, ProtoHeadRequest, ProtoListKeysRequest,
    ProtoOperationError, ProtoScanRequest, ProtoTruncateRequest, ProtoVersionedData,
    proto_compare_and_set_response, proto_head_response, proto_list_keys_response,
    proto_scan_response, proto_truncate_response,
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

/// True transport / committer failures: the operation may or may not have
/// succeeded, so retrying is the conservative choice.
fn transport_to_external(e: tonic::Status) -> ExternalError {
    ExternalError::Indeterminate(Indeterminate::new(anyhow!(
        "persist committer transport: {}",
        e
    )))
}

/// Operation-level errors carry the underlying store's determinacy
/// classification as data on the response. An unknown / unspecified
/// determinacy defaults to indeterminate (retry-safe) so newer clients can
/// talk to older servers without changing behavior unsafely.
fn from_proto_error(e: ProtoOperationError) -> ExternalError {
    let inner = anyhow!("persist committer: {}", e.message);
    match ProtoDeterminacy::try_from(e.determinacy) {
        Ok(ProtoDeterminacy::Determinate) => ExternalError::Determinate(Determinate::new(inner)),
        _ => ExternalError::Indeterminate(Indeterminate::new(inner)),
    }
}

fn missing_result() -> ExternalError {
    ExternalError::Indeterminate(Indeterminate::new(anyhow!(
        "persist committer: response missing result oneof"
    )))
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
                .map_err(transport_to_external)?
                .into_inner();
            let ok = match resp.result.ok_or_else(missing_result)? {
                proto_list_keys_response::Result::Ok(ok) => ok,
                proto_list_keys_response::Result::Err(e) => Err(from_proto_error(e))?,
            };
            for key in ok.keys {
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
            .map_err(transport_to_external)?
            .into_inner();
        match resp.result.ok_or_else(missing_result)? {
            proto_head_response::Result::Ok(ok) => Ok(ok.current.map(from_proto)),
            proto_head_response::Result::Err(e) => Err(from_proto_error(e)),
        }
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
            .map_err(transport_to_external)?
            .into_inner();
        match resp.result.ok_or_else(missing_result)? {
            proto_compare_and_set_response::Result::Ok(ok) => {
                if ok.committed {
                    Ok(CaSResult::Committed)
                } else {
                    Ok(CaSResult::ExpectationMismatch)
                }
            }
            proto_compare_and_set_response::Result::Err(e) => Err(from_proto_error(e)),
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
            .map_err(transport_to_external)?
            .into_inner();
        match resp.result.ok_or_else(missing_result)? {
            proto_scan_response::Result::Ok(ok) => {
                Ok(ok.versions.into_iter().map(from_proto).collect())
            }
            proto_scan_response::Result::Err(e) => Err(from_proto_error(e)),
        }
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        let mut client = self.client.clone();
        let resp = client
            .truncate(ProtoTruncateRequest {
                shard: key.to_string(),
                seqno: seqno.0,
            })
            .await
            .map_err(transport_to_external)?
            .into_inner();
        match resp.result.ok_or_else(missing_result)? {
            proto_truncate_response::Result::Ok(ok) => {
                Ok(ok.deleted.map(|n| usize::try_from(n).unwrap_or(usize::MAX)))
            }
            proto_truncate_response::Result::Err(e) => Err(from_proto_error(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proto_err(determinacy: ProtoDeterminacy, message: &str) -> ProtoOperationError {
        ProtoOperationError {
            determinacy: i32::from(determinacy),
            message: message.to_string(),
        }
    }

    #[mz_ore::test]
    fn determinate_proto_error_roundtrips_to_determinate_external() {
        let err = from_proto_error(proto_err(ProtoDeterminacy::Determinate, "db error"));
        assert!(matches!(err, ExternalError::Determinate(_)));
    }

    #[mz_ore::test]
    fn indeterminate_proto_error_roundtrips_to_indeterminate_external() {
        let err = from_proto_error(proto_err(ProtoDeterminacy::Indeterminate, "db timeout"));
        assert!(matches!(err, ExternalError::Indeterminate(_)));
    }

    #[mz_ore::test]
    fn unspecified_determinacy_defaults_to_indeterminate() {
        let err = from_proto_error(proto_err(ProtoDeterminacy::Unspecified, "no classifier"));
        assert!(matches!(err, ExternalError::Indeterminate(_)));
    }

    #[mz_ore::test]
    fn transport_status_becomes_indeterminate() {
        let status = tonic::Status::unavailable("committer not reachable");
        let err = transport_to_external(status);
        assert!(matches!(err, ExternalError::Indeterminate(_)));
    }

    #[mz_ore::test]
    fn missing_result_is_indeterminate() {
        assert!(matches!(missing_result(), ExternalError::Indeterminate(_)));
    }
}
