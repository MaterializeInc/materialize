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
    ProtoAppendResponse, ProtoCompareAndSetResponse, ProtoHeadResponse, ProtoLogProposal,
    ProtoScanResponse, ProtoTruncateResponse,
};

/// Configuration for the [`ActorAcceptor`](crate::actor::acceptor::ActorAcceptor).
#[derive(Debug, Clone)]
pub struct AcceptorConfig {
    /// Depth of the command channel (mpsc queue).
    pub queue_depth: usize,
    /// How often to flush pending proposals to the log, in milliseconds.
    pub flush_interval_ms: u64,
}

impl Default for AcceptorConfig {
    fn default() -> Self {
        AcceptorConfig {
            queue_depth: 4096,
            flush_interval_ms: 5,
        }
    }
}

/// Error returned by acceptor handle methods.
#[derive(Debug)]
pub enum AcceptorError {
    /// The acceptor's command channel was closed (acceptor shut down).
    Shutdown,
    /// The acceptor dropped the reply sender without responding.
    DroppedReply,
    /// The acceptor returned an application-level error.
    Command(String),
}

impl std::fmt::Display for AcceptorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AcceptorError::Shutdown => write!(f, "acceptor shut down"),
            AcceptorError::DroppedReply => write!(f, "acceptor dropped reply"),
            AcceptorError::Command(msg) => write!(f, "{}", msg),
        }
    }
}

/// Error returned by learner handle methods.
#[derive(Debug)]
pub enum LearnerError {
    /// The learner's command channel was closed (learner shut down).
    Shutdown,
    /// The learner dropped the reply sender without responding.
    DroppedReply,
    /// The learner returned an application-level error.
    Command(String),
}

impl std::fmt::Display for LearnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LearnerError::Shutdown => write!(f, "learner shut down"),
            LearnerError::DroppedReply => write!(f, "learner dropped reply"),
            LearnerError::Command(msg) => write!(f, "{}", msg),
        }
    }
}

#[async_trait::async_trait]
pub trait Acceptor: Clone + std::fmt::Debug + Send + Sync + 'static {
    async fn append(
        &self,
        proposal: ProtoLogProposal,
    ) -> Result<ProtoAppendResponse, AcceptorError>;
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
