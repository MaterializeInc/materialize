// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Test utilities for injecting latency and errors.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Instant, UNIX_EPOCH};

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use tracing::trace;

use crate::location::{
    Atomicity, BlobMulti, Consensus, Determinate, ExternalError, SeqNo, VersionedData,
};

#[derive(Debug)]
struct UnreliableCore {
    rng: SmallRng,
    should_happen: f64,
    should_timeout: f64,
    // TODO: Delays, what else?
}

/// A handle for controlling the behavior of an unreliable delegate.
#[derive(Clone, Debug)]
pub struct UnreliableHandle {
    core: Arc<Mutex<UnreliableCore>>,
}

impl Default for UnreliableHandle {
    fn default() -> Self {
        let seed = UNIX_EPOCH
            .elapsed()
            .map_or(0, |x| u64::from(x.subsec_nanos()));
        Self::new(seed, 0.95, 0.05)
    }
}

impl UnreliableHandle {
    /// Returns a new [UnreliableHandle].
    pub fn new(seed: u64, should_happen: f64, should_timeout: f64) -> Self {
        assert!(should_happen >= 0.0);
        assert!(should_happen <= 1.0);
        assert!(should_timeout >= 0.0);
        assert!(should_timeout <= 1.0);
        let core = UnreliableCore {
            rng: SmallRng::seed_from_u64(seed),
            should_happen,
            should_timeout,
        };
        UnreliableHandle {
            core: Arc::new(Mutex::new(core)),
        }
    }

    /// Cause all later calls to sometimes return an error.
    pub fn partially_available(&self, should_happen: f64, should_timeout: f64) {
        assert!(should_happen >= 0.0);
        assert!(should_happen <= 1.0);
        assert!(should_timeout >= 0.0);
        assert!(should_timeout <= 1.0);
        let mut core = self.core.lock().expect("mutex poisoned");
        core.should_happen = should_happen;
        core.should_timeout = should_timeout;
    }

    /// Cause all later calls to return an error.
    pub fn totally_unavailable(&self) {
        self.partially_available(0.0, 1.0);
    }

    /// Cause all later calls to succeed.
    pub fn totally_available(&self) {
        self.partially_available(1.0, 0.0);
    }

    fn should_happen(&self) -> bool {
        let mut core = self.core.lock().expect("mutex poisoned");
        let should_happen = core.should_happen;
        core.rng.gen_bool(should_happen)
    }

    fn should_timeout(&self) -> bool {
        let mut core = self.core.lock().expect("mutex poisoned");
        let should_timeout = core.should_timeout;
        core.rng.gen_bool(should_timeout)
    }

    async fn run_op<R, F, WorkFn>(
        &self,
        deadline: Instant,
        name: &str,
        work_fn: WorkFn,
    ) -> Result<R, ExternalError>
    where
        F: Future<Output = Result<R, ExternalError>>,
        WorkFn: FnOnce() -> F,
    {
        let (should_happen, should_timeout) = (self.should_happen(), self.should_timeout());
        trace!(
            "unreliable {} should_happen={} should_timeout={}",
            name,
            should_happen,
            should_timeout,
        );
        match (should_happen, should_timeout) {
            (true, true) => {
                let _res = work_fn().await;
                Err(ExternalError::new_timeout(deadline))
            }
            (true, false) => work_fn().await,
            (false, true) => Err(ExternalError::new_timeout(deadline)),
            (false, false) => Err(ExternalError::Determinate(Determinate::new(anyhow!(
                "unreliable"
            )))),
        }
    }
}

/// An unreliable delegate to [BlobMulti].
#[derive(Debug)]
pub struct UnreliableBlobMulti {
    handle: UnreliableHandle,
    blob: Arc<dyn BlobMulti + Send + Sync>,
}

impl UnreliableBlobMulti {
    /// Returns a new [UnreliableBlobMulti].
    pub fn new(blob: Arc<dyn BlobMulti + Send + Sync>, handle: UnreliableHandle) -> Self {
        UnreliableBlobMulti { handle, blob }
    }
}

#[async_trait]
impl BlobMulti for UnreliableBlobMulti {
    async fn get(&self, deadline: Instant, key: &str) -> Result<Option<Vec<u8>>, ExternalError> {
        self.handle
            .run_op(deadline, "get", || self.blob.get(deadline, key))
            .await
    }

    async fn list_keys(&self, deadline: Instant) -> Result<Vec<String>, ExternalError> {
        self.handle
            .run_op(deadline, "list_keys", || self.blob.list_keys(deadline))
            .await
    }

    async fn set(
        &self,
        deadline: Instant,
        key: &str,
        value: Bytes,
        atomic: Atomicity,
    ) -> Result<(), ExternalError> {
        self.handle
            .run_op(deadline, "set", || {
                self.blob.set(deadline, key, value, atomic)
            })
            .await
    }

    async fn delete(&self, deadline: Instant, key: &str) -> Result<(), ExternalError> {
        self.handle
            .run_op(deadline, "delete", || self.blob.delete(deadline, key))
            .await
    }
}

/// An unreliable delegate to [Consensus].
#[derive(Debug)]
pub struct UnreliableConsensus {
    handle: UnreliableHandle,
    consensus: Arc<dyn Consensus + Send + Sync>,
}

impl UnreliableConsensus {
    /// Returns a new [UnreliableConsensus].
    pub fn new(consensus: Arc<dyn Consensus + Send + Sync>, handle: UnreliableHandle) -> Self {
        UnreliableConsensus { consensus, handle }
    }
}

#[async_trait]
impl Consensus for UnreliableConsensus {
    async fn head(
        &self,
        deadline: Instant,
        key: &str,
    ) -> Result<Option<VersionedData>, ExternalError> {
        self.handle
            .run_op(deadline, "head", || self.consensus.head(deadline, key))
            .await
    }

    async fn compare_and_set(
        &self,
        deadline: Instant,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<Result<(), Option<VersionedData>>, ExternalError> {
        self.handle
            .run_op(deadline, "compare_and_set", || {
                self.consensus.compare_and_set(deadline, key, expected, new)
            })
            .await
    }

    async fn scan(
        &self,
        deadline: Instant,
        key: &str,
        from: SeqNo,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        self.handle
            .run_op(deadline, "scan", || {
                self.consensus.scan(deadline, key, from)
            })
            .await
    }

    async fn truncate(
        &self,
        deadline: Instant,
        key: &str,
        seqno: SeqNo,
    ) -> Result<(), ExternalError> {
        self.handle
            .run_op(deadline, "truncate", || {
                self.consensus.truncate(deadline, key, seqno)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::mem::{MemBlobMulti, MemBlobMultiConfig, MemConsensus};

    use super::*;

    #[tokio::test]
    async fn unreliable_blob_multi() {
        let blob = Arc::new(MemBlobMulti::open(MemBlobMultiConfig::default()))
            as Arc<dyn BlobMulti + Send + Sync>;
        let handle = UnreliableHandle::default();
        let blob = UnreliableBlobMulti::new(blob, handle.clone());
        let deadline = Instant::now() + Duration::from_secs(1_000_000_000);

        // Use a fixed seed so this test doesn't flake.
        {
            (*handle.core.lock().expect("mutex poisoned")).rng = SmallRng::seed_from_u64(0);
        }

        // By default, it's partially reliable.
        let mut succeeded = 0;
        for _ in 0..100 {
            if blob.get(deadline, "a").await.is_ok() {
                succeeded += 1;
            }
        }
        // Intentionally have pretty loose bounds so this assertion doesn't
        // become a maintenance burden if the rng impl changes.
        assert!(succeeded > 50 && succeeded < 99, "succeeded={}", succeeded);

        // Reliable doesn't error.
        handle.totally_available();
        assert!(blob.get(deadline, "a").await.is_ok());

        // Unreliable does error.
        handle.totally_unavailable();
        assert!(blob.get(deadline, "a").await.is_err());
    }

    #[tokio::test]
    async fn unreliable_consensus() {
        let consensus = Arc::new(MemConsensus::default()) as Arc<dyn Consensus + Send + Sync>;
        let handle = UnreliableHandle::default();
        let consensus = UnreliableConsensus::new(consensus, handle.clone());
        let deadline = Instant::now() + Duration::from_secs(1_000_000_000);

        // Use a fixed seed so this test doesn't flake.
        {
            (*handle.core.lock().expect("mutex poisoned")).rng = SmallRng::seed_from_u64(0);
        }

        // By default, it's partially reliable.
        let mut succeeded = 0;
        for _ in 0..100 {
            if consensus.head(deadline, "key").await.is_ok() {
                succeeded += 1;
            }
        }
        // Intentionally have pretty loose bounds so this assertion doesn't
        // become a maintenance burden if the rng impl changes.
        assert!(succeeded > 50 && succeeded < 99, "succeeded={}", succeeded);

        // Reliable doesn't error.
        handle.totally_available();
        assert!(consensus.head(deadline, "key").await.is_ok());

        // Unreliable does error.
        handle.totally_unavailable();
        assert!(consensus.head(deadline, "key").await.is_err());
    }
}
