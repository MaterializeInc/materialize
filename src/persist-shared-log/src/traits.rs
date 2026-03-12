// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Traits for the acceptor and learner, enabling alternative implementations
//! (e.g. a persist-shard-backed variant) to be slotted in.

use mz_persist::generated::consensus_service::{
    ProtoAppendResponse, ProtoCompareAndSetResponse, ProtoHeadResponse, ProtoScanResponse,
    ProtoTruncateResponse, ProtoLogProposal,
};

use crate::acceptor::AcceptorError;
use crate::learner::LearnerError;

#[async_trait::async_trait]
pub trait Acceptor: Clone + std::fmt::Debug + Send + Sync + 'static {
    async fn append(
        &self,
        proposal: ProtoLogProposal,
    ) -> Result<ProtoAppendResponse, AcceptorError>;

    /// Read the latest committed batch number. Log-backed acceptors track
    /// this via a shared atomic; persist-backed acceptors don't need it (the
    /// learner queries the shard upper directly). Returns `None` by default.
    fn latest_committed_batch(&self) -> Option<u64> {
        None
    }
}

#[async_trait::async_trait]
pub trait Learner: Clone + std::fmt::Debug + Send + Sync + 'static {
    async fn head(&self, key: String) -> Result<ProtoHeadResponse, LearnerError>;
    async fn scan(
        &self,
        key: String,
        from: u64,
        limit: u64,
    ) -> Result<ProtoScanResponse, LearnerError>;
    async fn list_keys(&self) -> Result<Vec<String>, LearnerError>;
    async fn await_cas_result(
        &self,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoCompareAndSetResponse, LearnerError>;
    async fn await_truncate_result(
        &self,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoTruncateResponse, LearnerError>;
}
