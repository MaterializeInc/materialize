// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A [`Blob`] wrapper that injects artificial latency, enabling benchmarking
//! under realistic storage latency profiles (S3 Express, S3 Standard, etc.)
//! without touching real object storage.
//!
//! Latency is injected on `set()` (the blob write path used by
//! `compare_and_append`) and `get()` (the blob read path used by `listen`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use mz_ore::bytes::SegmentedBytes;

use mz_persist::location::{Blob, BlobMetadata, ExternalError};

/// Latency profile for benchmarking storage backends.
#[derive(Debug, Clone)]
pub enum LatencyProfile {
    /// Return immediately (no added latency).
    Zero,
    /// Fixed latency for every operation.
    Fixed(Duration),
    /// Sample from a distribution: p50 latency with occasional p99 spikes.
    /// Roughly 95% of operations take `p50`, 5% take `p99`.
    P50P99 { p50: Duration, p99: Duration },
}

/// A [`Blob`] wrapper that adds configurable latency to `set()` and `get()`
/// calls, simulating storage round-trip times.
#[derive(Debug)]
pub struct LatencyBlob {
    inner: Box<dyn Blob>,
    profile: LatencyProfile,
    /// Counter-based selection for p50/p99, matching the actor LatencyStorage
    /// pattern (avoids pulling in rand as a non-dev dependency).
    counter: AtomicU64,
    /// Total set() calls (for diagnosing blob I/O per flush).
    pub sets: AtomicU64,
    /// Total get() calls.
    pub gets: AtomicU64,
}

impl LatencyBlob {
    /// Wraps the given blob with the specified latency profile.
    pub fn new(inner: Box<dyn Blob>, profile: LatencyProfile) -> Self {
        LatencyBlob {
            inner,
            profile,
            counter: AtomicU64::new(0),
            sets: AtomicU64::new(0),
            gets: AtomicU64::new(0),
        }
    }

    /// Returns the delay for the current operation according to the profile.
    fn delay(&self) -> Option<Duration> {
        match &self.profile {
            LatencyProfile::Zero => None,
            LatencyProfile::Fixed(d) => Some(*d),
            LatencyProfile::P50P99 { p50, p99 } => {
                let n = self.counter.fetch_add(1, Ordering::Relaxed);
                // ~5% of operations get p99 latency.
                Some(if n % 20 == 0 { *p99 } else { *p50 })
            }
        }
    }

    async fn inject_latency(&self) {
        if let Some(d) = self.delay() {
            tokio::time::sleep(d).await;
        }
    }
}

#[async_trait]
impl Blob for LatencyBlob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.inject_latency().await;
        self.inner.get(key).await
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        self.inner.list_keys_and_metadata(key_prefix, f).await
    }

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        self.sets.fetch_add(1, Ordering::Relaxed);
        self.inject_latency().await;
        self.inner.set(key, value).await
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        self.inner.delete(key).await
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        self.inner.restore(key).await
    }
}

impl Drop for LatencyBlob {
    fn drop(&mut self) {
        let sets = self.sets.load(Ordering::Relaxed);
        let gets = self.gets.load(Ordering::Relaxed);
        eprintln!(
            "LatencyBlob stats: sets={}, gets={}, total={}",
            sets,
            gets,
            sets + gets,
        );
    }
}
