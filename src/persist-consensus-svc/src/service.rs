// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! gRPC service implementation that dispatches to the actor.

use tracing::debug;

use mz_persist::generated::consensus_service::consensus_service_server::ConsensusService;
use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetRequest, ProtoCompareAndSetResponse, ProtoHeadRequest, ProtoHeadResponse,
    ProtoListKeysRequest, ProtoListKeysResponse, ProtoScanRequest, ProtoScanResponse,
    ProtoTruncateRequest, ProtoTruncateResponse,
};

use crate::actor::{ActorError, ActorHandle};

impl From<ActorError> for tonic::Status {
    fn from(e: ActorError) -> Self {
        match e {
            ActorError::Shutdown => tonic::Status::unavailable("actor shut down"),
            ActorError::DroppedReply => tonic::Status::internal("actor dropped reply"),
            ActorError::Command(msg) => tonic::Status::internal(msg),
        }
    }
}

/// The gRPC service implementation that dispatches to the actor.
#[derive(Debug)]
pub struct ConsensusGrpcService {
    pub handle: ActorHandle,
}

#[tonic::async_trait]
impl ConsensusService for ConsensusGrpcService {
    async fn head(
        &self,
        request: tonic::Request<ProtoHeadRequest>,
    ) -> Result<tonic::Response<ProtoHeadResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, "head");
        let resp = self.handle.head(req.key).await?;
        Ok(tonic::Response::new(resp))
    }

    async fn compare_and_set(
        &self,
        request: tonic::Request<ProtoCompareAndSetRequest>,
    ) -> Result<tonic::Response<ProtoCompareAndSetResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, expected = req.expected, new_seqno = req.new.as_ref().map(|v| v.seqno), "cas");
        let new = req
            .new
            .ok_or_else(|| tonic::Status::invalid_argument("missing `new` field"))?;
        let resp = self
            .handle
            .compare_and_set(req.key, req.expected, new)
            .await?;
        Ok(tonic::Response::new(resp))
    }

    async fn scan(
        &self,
        request: tonic::Request<ProtoScanRequest>,
    ) -> Result<tonic::Response<ProtoScanResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, from = req.from, limit = req.limit, "scan");
        let resp = self.handle.scan(req.key, req.from, req.limit).await?;
        Ok(tonic::Response::new(resp))
    }

    async fn truncate(
        &self,
        request: tonic::Request<ProtoTruncateRequest>,
    ) -> Result<tonic::Response<ProtoTruncateResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, seqno = req.seqno, "truncate");
        let resp = self.handle.truncate(req.key, req.seqno).await?;
        Ok(tonic::Response::new(resp))
    }

    type ListKeysStream =
        tokio_stream::wrappers::ReceiverStream<Result<ProtoListKeysResponse, tonic::Status>>;

    async fn list_keys(
        &self,
        _request: tonic::Request<ProtoListKeysRequest>,
    ) -> Result<tonic::Response<Self::ListKeysStream>, tonic::Status> {
        debug!("list_keys");
        let keys = self.handle.list_keys().await?;
        let (stream_tx, stream_rx) = tokio::sync::mpsc::channel(64);
        mz_ore::task::spawn(|| "list-keys-stream", async move {
            for key in keys {
                if stream_tx
                    .send(Ok(ProtoListKeysResponse { key }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(stream_rx),
        ))
    }
}
