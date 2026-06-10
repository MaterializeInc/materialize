// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! In-process `Consensus` adapter for `environmentd`'s own persist traffic.
//!
//! Mirrors the pubsub `new_same_process_connection` pattern: envd never goes
//! through tonic for its own consensus calls; it dispatches directly into
//! `PersistCommitter`. The gRPC server is still present for clusterds.
//!
//! Using gRPC for in-process self-traffic causes runtime starvation and
//! deadlocks under load on bounded multi-thread runtimes, as observed in
//! `pgwire` and `sqllogictest` integration suites.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use mz_persist::location::{
    CaSResult, Consensus, ExternalError, ResultStream, SeqNo, VersionedData,
};

use crate::server::PersistCommitter;

/// `Consensus` implementation backed by direct calls into a local
/// `PersistCommitter`. Cheap to clone via the inner `Arc`.
#[derive(Debug, Clone)]
pub struct InProcessConsensus {
    inner: Arc<PersistCommitter>,
}

impl InProcessConsensus {
    pub fn new(inner: Arc<PersistCommitter>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl Consensus for InProcessConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        let inner = Arc::clone(&self.inner);
        Box::pin(
            stream::once(async move {
                // The underlying `Consensus::list_keys` returns a streaming
                // result; we collect it for the gRPC layer anyway, so do the
                // same here and replay the vector as a stream for trait compat.
                inner.list_keys_inner().await
            })
            .flat_map(|res| match res {
                Ok(keys) => stream::iter(keys.into_iter().map(Ok)).boxed(),
                Err(e) => stream::iter(std::iter::once(Err(e))).boxed(),
            }),
        )
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        self.inner.head_inner(key).await
    }

    async fn compare_and_set(
        &self,
        key: &str,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        self.inner.cas_inner(key, new).await
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        self.inner.scan_inner(key, from, limit).await
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        self.inner.truncate_inner(key, seqno).await
    }
}
