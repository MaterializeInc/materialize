// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! gRPC service implementing the `PersistSharedLog` proto service.
//!
//! Maps 1:1 to the persist `Consensus` trait surface: `compare_and_set`,
//! `head`, `scan`, `list_keys`, `truncate`. Write operations handle the full
//! acceptor-append → learner-await pipeline server-side, so each client call
//! is a single RPC round-trip.

use tracing::debug;

use mz_persist::generated::consensus_service::persist_shared_log_server::PersistSharedLog;
use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetRequest, ProtoCompareAndSetResponse, ProtoHeadRequest, ProtoHeadResponse,
    ProtoListKeysRequest, ProtoListKeysResponse, ProtoLogProposal, ProtoScanRequest,
    ProtoScanResponse, ProtoTruncateRequest, ProtoTruncateResponse, proto_log_proposal,
};

use crate::{AcceptorError, LearnerError};

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
// PersistSharedLog gRPC service
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct PersistSharedLogGrpcService<A: crate::Acceptor, L: crate::Learner> {
    pub acceptor: A,
    pub learner: L,
}

#[tonic::async_trait]
impl<A: crate::Acceptor, L: crate::Learner> PersistSharedLog for PersistSharedLogGrpcService<A, L> {
    async fn head(
        &self,
        request: tonic::Request<ProtoHeadRequest>,
    ) -> Result<tonic::Response<ProtoHeadResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, "head");
        let resp = self.learner.head(req.key).await?;
        Ok(tonic::Response::new(resp))
    }

    async fn scan(
        &self,
        request: tonic::Request<ProtoScanRequest>,
    ) -> Result<tonic::Response<ProtoScanResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, from = req.from, limit = req.limit, "scan");
        let resp = self.learner.scan(req.key, req.from, req.limit).await?;
        Ok(tonic::Response::new(resp))
    }

    type ListKeysStream =
        tokio_stream::wrappers::ReceiverStream<Result<ProtoListKeysResponse, tonic::Status>>;

    async fn list_keys(
        &self,
        _request: tonic::Request<ProtoListKeysRequest>,
    ) -> Result<tonic::Response<Self::ListKeysStream>, tonic::Status> {
        debug!("list_keys");
        let keys = self.learner.list_keys().await?;
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

    async fn compare_and_set(
        &self,
        request: tonic::Request<ProtoCompareAndSetRequest>,
    ) -> Result<tonic::Response<ProtoCompareAndSetResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, "compare_and_set");

        let new = req
            .new
            .ok_or_else(|| tonic::Status::invalid_argument("missing new"))?;

        let proposal = ProtoLogProposal {
            op: Some(proto_log_proposal::Op::Cas(
                mz_persist::generated::consensus_service::ProtoCasProposal {
                    key: req.key,
                    expected: req.expected,
                    new_seqno: new.seqno,
                    data: new.data,
                },
            )),
        };

        let receipt = self.acceptor.append(proposal).await?;

        let result = self
            .learner
            .await_cas_result(receipt.batch_number, receipt.position)
            .await?;

        Ok(tonic::Response::new(result))
    }

    async fn truncate(
        &self,
        request: tonic::Request<ProtoTruncateRequest>,
    ) -> Result<tonic::Response<ProtoTruncateResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, seqno = req.seqno, "truncate");

        let proposal = ProtoLogProposal {
            op: Some(proto_log_proposal::Op::Truncate(
                mz_persist::generated::consensus_service::ProtoTruncateProposal {
                    key: req.key,
                    seqno: req.seqno,
                },
            )),
        };

        let receipt = self.acceptor.append(proposal).await?;

        let result = self
            .learner
            .await_truncate_result(receipt.batch_number, receipt.position)
            .await?;

        Ok(tonic::Response::new(result))
    }
}
