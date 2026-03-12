// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! gRPC service implementations for the acceptor, learner, and combined
//! PersistSharedLog service.

use tracing::debug;

use mz_persist::generated::consensus_service::consensus_acceptor_server::ConsensusAcceptor;
use mz_persist::generated::consensus_service::consensus_learner_server::ConsensusLearner;
use mz_persist::generated::consensus_service::persist_shared_log_server::PersistSharedLog;
use mz_persist::generated::consensus_service::{
    ProtoAppendRequest, ProtoAppendResponse, ProtoAwaitResultRequest, ProtoCompareAndSetRequest,
    ProtoCompareAndSetResponse, ProtoHeadRequest, ProtoHeadResponse,
    ProtoLatestCommittedBatchRequest, ProtoLatestCommittedBatchResponse, ProtoListKeysRequest,
    ProtoListKeysResponse, ProtoScanRequest, ProtoScanResponse, ProtoTruncateRequest,
    ProtoTruncateResponse, ProtoWalProposal, proto_wal_proposal,
};

use crate::acceptor::AcceptorError;
use crate::learner::LearnerError;
use crate::traits;

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
pub struct AcceptorGrpcService<A: traits::Acceptor> {
    pub handle: A,
}

#[tonic::async_trait]
impl<A: traits::Acceptor> ConsensusAcceptor for AcceptorGrpcService<A> {
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
/// Read linearization is handled by the learner implementation — it queries
/// the acceptor's latest committed batch internally before each read.
#[derive(Debug)]
pub struct LearnerGrpcService<L: traits::Learner> {
    pub learner_handle: L,
}

#[tonic::async_trait]
impl<L: traits::Learner> ConsensusLearner for LearnerGrpcService<L> {
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

// ---------------------------------------------------------------------------
// Combined PersistSharedLog gRPC service
// ---------------------------------------------------------------------------

/// Combined gRPC service that maps 1:1 to the persist `Consensus` trait.
///
/// `CompareAndSet` and `Truncate` handle the full acceptor-append → learner-
/// await pipeline server-side, so each client `Consensus` method is a single
/// RPC round-trip.
#[derive(Debug)]
pub struct PersistSharedLogGrpcService<A: traits::Acceptor, L: traits::Learner> {
    pub acceptor: A,
    pub learner: L,
}

#[tonic::async_trait]
impl<A: traits::Acceptor, L: traits::Learner> PersistSharedLog
    for PersistSharedLogGrpcService<A, L>
{
    async fn head(
        &self,
        request: tonic::Request<ProtoHeadRequest>,
    ) -> Result<tonic::Response<ProtoHeadResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, "persist_shared_log::head");
        let resp = self.learner.head(req.key).await?;
        Ok(tonic::Response::new(resp))
    }

    async fn scan(
        &self,
        request: tonic::Request<ProtoScanRequest>,
    ) -> Result<tonic::Response<ProtoScanResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, from = req.from, limit = req.limit, "persist_shared_log::scan");
        let resp = self.learner.scan(req.key, req.from, req.limit).await?;
        Ok(tonic::Response::new(resp))
    }

    type ListKeysStream =
        tokio_stream::wrappers::ReceiverStream<Result<ProtoListKeysResponse, tonic::Status>>;

    async fn list_keys(
        &self,
        _request: tonic::Request<ProtoListKeysRequest>,
    ) -> Result<tonic::Response<Self::ListKeysStream>, tonic::Status> {
        debug!("persist_shared_log::list_keys");
        let keys = self.learner.list_keys().await?;
        let (stream_tx, stream_rx) = tokio::sync::mpsc::channel(64);
        mz_ore::task::spawn(|| "psl-list-keys-stream", async move {
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
        debug!(key = %req.key, "persist_shared_log::compare_and_set");

        let new = req
            .new
            .ok_or_else(|| tonic::Status::invalid_argument("missing new"))?;

        // Build WAL proposal from the CAS request.
        let proposal = ProtoWalProposal {
            op: Some(proto_wal_proposal::Op::Cas(
                mz_persist::generated::consensus_service::ProtoCasProposal {
                    key: req.key,
                    expected: req.expected,
                    new_seqno: new.seqno,
                    data: new.data,
                },
            )),
        };

        // Append to acceptor (blocks until group commit flush).
        let receipt = self.acceptor.append(proposal).await?;

        // Await learner materialization.
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
        debug!(key = %req.key, seqno = req.seqno, "persist_shared_log::truncate");

        // Build WAL proposal from the truncate request.
        let proposal = ProtoWalProposal {
            op: Some(proto_wal_proposal::Op::Truncate(
                mz_persist::generated::consensus_service::ProtoTruncateProposal {
                    key: req.key,
                    seqno: req.seqno,
                },
            )),
        };

        // Append to acceptor (blocks until group commit flush).
        let receipt = self.acceptor.append(proposal).await?;

        // Await learner materialization.
        let result = self
            .learner
            .await_truncate_result(receipt.batch_number, receipt.position)
            .await?;

        Ok(tonic::Response::new(result))
    }
}
