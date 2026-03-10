// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! gRPC service implementations for the acceptor and learner.

use tracing::debug;

use mz_persist::generated::consensus_service::consensus_acceptor_server::ConsensusAcceptor;
use mz_persist::generated::consensus_service::consensus_learner_server::ConsensusLearner;
use mz_persist::generated::consensus_service::{
    ProtoAppendRequest, ProtoAppendResponse, ProtoAwaitResultRequest, ProtoCompareAndSetResponse,
    ProtoHeadRequest, ProtoHeadResponse, ProtoLatestCommittedBatchRequest,
    ProtoLatestCommittedBatchResponse, ProtoListKeysRequest, ProtoListKeysResponse,
    ProtoScanRequest, ProtoScanResponse, ProtoTruncateResponse,
};

use crate::acceptor::{AcceptorError, AcceptorHandle};
use crate::learner::{LearnerError, LearnerHandle};

// ---------------------------------------------------------------------------
// Error conversions
// ---------------------------------------------------------------------------

impl From<AcceptorError> for tonic::Status {
    fn from(e: AcceptorError) -> Self {
        match e {
            AcceptorError::Shutdown => tonic::Status::unavailable("acceptor shut down"),
            AcceptorError::DroppedReply => tonic::Status::internal("acceptor dropped reply"),
            AcceptorError::Command(msg) => tonic::Status::internal(msg),
        }
    }
}

impl From<LearnerError> for tonic::Status {
    fn from(e: LearnerError) -> Self {
        match e {
            LearnerError::Shutdown => tonic::Status::unavailable("learner shut down"),
            LearnerError::DroppedReply => tonic::Status::internal("learner dropped reply"),
            LearnerError::Command(msg) => tonic::Status::internal(msg),
        }
    }
}

// ---------------------------------------------------------------------------
// Acceptor gRPC service
// ---------------------------------------------------------------------------

/// gRPC service for the acceptor (blind group commit).
#[derive(Debug)]
pub struct AcceptorGrpcService {
    pub handle: AcceptorHandle,
}

#[tonic::async_trait]
impl ConsensusAcceptor for AcceptorGrpcService {
    async fn append(
        &self,
        request: tonic::Request<ProtoAppendRequest>,
    ) -> Result<tonic::Response<ProtoAppendResponse>, tonic::Status> {
        let req = request.into_inner();
        let proposal = req
            .proposal
            .ok_or_else(|| tonic::Status::invalid_argument("missing proposal"))?;
        let resp = self.handle.append(proposal).await?;
        Ok(tonic::Response::new(resp))
    }

    async fn latest_committed_batch(
        &self,
        _request: tonic::Request<ProtoLatestCommittedBatchRequest>,
    ) -> Result<tonic::Response<ProtoLatestCommittedBatchResponse>, tonic::Status> {
        let batch = self.handle.latest_committed_batch();
        Ok(tonic::Response::new(ProtoLatestCommittedBatchResponse {
            batch_number: batch,
        }))
    }
}

// ---------------------------------------------------------------------------
// Learner gRPC service
// ---------------------------------------------------------------------------

/// gRPC service for the learner (reads + result queries).
///
/// Read linearization is handled by the [`LearnerHandle`] — it queries the
/// acceptor's latest committed batch internally before each read.
#[derive(Debug)]
pub struct LearnerGrpcService {
    pub learner_handle: LearnerHandle,
}

#[tonic::async_trait]
impl ConsensusLearner for LearnerGrpcService {
    async fn await_cas_result(
        &self,
        request: tonic::Request<ProtoAwaitResultRequest>,
    ) -> Result<tonic::Response<ProtoCompareAndSetResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(
            batch = req.batch_number,
            position = req.position,
            "await_cas_result"
        );
        let resp = self
            .learner_handle
            .await_cas_result(req.batch_number, req.position)
            .await?;
        Ok(tonic::Response::new(resp))
    }

    async fn await_truncate_result(
        &self,
        request: tonic::Request<ProtoAwaitResultRequest>,
    ) -> Result<tonic::Response<ProtoTruncateResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(
            batch = req.batch_number,
            position = req.position,
            "await_truncate_result"
        );
        let resp = self
            .learner_handle
            .await_truncate_result(req.batch_number, req.position)
            .await?;
        Ok(tonic::Response::new(resp))
    }

    async fn head(
        &self,
        request: tonic::Request<ProtoHeadRequest>,
    ) -> Result<tonic::Response<ProtoHeadResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, "head");
        let resp = self.learner_handle.head(req.key).await?;
        Ok(tonic::Response::new(resp))
    }

    async fn scan(
        &self,
        request: tonic::Request<ProtoScanRequest>,
    ) -> Result<tonic::Response<ProtoScanResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, from = req.from, limit = req.limit, "scan");
        let resp = self
            .learner_handle
            .scan(req.key, req.from, req.limit)
            .await?;
        Ok(tonic::Response::new(resp))
    }

    type ListKeysStream =
        tokio_stream::wrappers::ReceiverStream<Result<ProtoListKeysResponse, tonic::Status>>;

    async fn list_keys(
        &self,
        _request: tonic::Request<ProtoListKeysRequest>,
    ) -> Result<tonic::Response<Self::ListKeysStream>, tonic::Status> {
        debug!("list_keys");
        let keys = self.learner_handle.list_keys().await?;
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
