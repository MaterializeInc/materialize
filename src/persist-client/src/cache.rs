// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A cache of [PersistClient]s indexed by [PersistLocation]s.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_ore::metrics::MetricsRegistry;
use mz_persist::cfg::{BlobConfig, ConsensusConfig};
use mz_persist::location::{
    Blob, Consensus, ExternalError, BLOB_GET_LIVENESS_KEY, CONSENSUS_HEAD_LIVENESS_KEY,
};
use tokio::task::JoinHandle;
use tracing::instrument;

use crate::async_runtime::CpuHeavyRuntime;
use crate::internal::machine::retry_external;
use crate::internal::metrics::{Metrics, MetricsBlob, MetricsConsensus};
use crate::{PersistClient, PersistConfig, PersistLocation};

/// A cache of [PersistClient]s indexed by [PersistLocation]s.
///
/// There should be at most one of these per process. All production
/// PersistClients should be created through this cache.
///
/// This is because, in production, persist is heavily limited by the number of
/// server-side Postgres/Aurora connections. This cache allows PersistClients to
/// share, for example, these Postgres connections.
#[derive(Debug, Clone)]
pub struct PersistClientCache {
    pub(crate) cfg: PersistConfig,
    pub(crate) metrics: Arc<Metrics>,
    blob_by_uri: BTreeMap<String, Arc<dyn Blob + Send + Sync>>,
    consensus_by_uri: BTreeMap<String, Arc<dyn Consensus + Send + Sync>>,
    cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
    rtt_latency_tasks: Vec<Arc<JoinHandle<()>>>,
}

impl PersistClientCache {
    /// Returns a new [PersistClientCache].
    pub fn new(cfg: PersistConfig, registry: &MetricsRegistry) -> Self {
        let metrics = Metrics::new(&cfg, registry);
        PersistClientCache {
            cfg,
            metrics: Arc::new(metrics),
            blob_by_uri: BTreeMap::new(),
            consensus_by_uri: BTreeMap::new(),
            cpu_heavy_runtime: Arc::new(CpuHeavyRuntime::new()),
            rtt_latency_tasks: Vec::new(),
        }
    }

    /// A test helper that returns a [PersistClientCache] disconnected from
    /// metrics.
    #[cfg(test)]
    pub fn new_no_metrics() -> Self {
        Self::new(PersistConfig::new_for_tests(), &MetricsRegistry::new())
    }

    /// Returns a new [PersistClient] for interfacing with persist shards made
    /// durable to the given [PersistLocation].
    ///
    /// The same `location` may be used concurrently from multiple processes.
    #[instrument(level = "debug", skip_all)]
    pub async fn open(
        &mut self,
        location: PersistLocation,
    ) -> Result<PersistClient, ExternalError> {
        let blob = self.open_blob(location.blob_uri).await?;
        let consensus = self.open_consensus(location.consensus_uri).await?;
        PersistClient::new(
            self.cfg.clone(),
            blob,
            consensus,
            Arc::clone(&self.metrics),
            Arc::clone(&self.cpu_heavy_runtime),
        )
    }

    // No sense in measuring rtt latencies more often than this.
    const PROMETHEUS_SCRAPE_INTERVAL: Duration = Duration::from_secs(60);

    async fn open_consensus(
        &mut self,
        consensus_uri: String,
    ) -> Result<Arc<dyn Consensus + Send + Sync>, ExternalError> {
        let consensus = match self.consensus_by_uri.entry(consensus_uri) {
            Entry::Occupied(x) => Arc::clone(x.get()),
            Entry::Vacant(x) => {
                // Intentionally hold the lock, so we don't double connect under
                // concurrency.
                let consensus = ConsensusConfig::try_from(
                    x.key(),
                    Box::new(self.cfg.clone()),
                    self.metrics.postgres_consensus.clone(),
                )?;
                let consensus =
                    retry_external(&self.metrics.retries.external.consensus_open, || {
                        consensus.clone().open()
                    })
                    .await;
                Arc::clone(x.insert(consensus))
            }
        };
        let consensus = Arc::new(MetricsConsensus::new(consensus, Arc::clone(&self.metrics)));
        self.rtt_latency_tasks.push(Arc::new(
            consensus_rtt_latency_task(
                Arc::clone(&consensus),
                Arc::clone(&self.metrics),
                Self::PROMETHEUS_SCRAPE_INTERVAL,
            )
            .await,
        ));
        Ok(consensus)
    }

    pub(crate) async fn open_blob(
        &mut self,
        blob_uri: String,
    ) -> Result<Arc<dyn Blob + Send + Sync>, ExternalError> {
        let blob = match self.blob_by_uri.entry(blob_uri) {
            Entry::Occupied(x) => Arc::clone(x.get()),
            Entry::Vacant(x) => {
                // Intentionally hold the lock, so we don't double connect under
                // concurrency.
                let blob = BlobConfig::try_from(
                    x.key(),
                    Box::new(self.cfg.clone()),
                    self.metrics.s3_blob.clone(),
                )
                .await?;
                let blob = retry_external(&self.metrics.retries.external.blob_open, || {
                    blob.clone().open()
                })
                .await;
                Arc::clone(x.insert(blob))
            }
        };
        let blob = Arc::new(MetricsBlob::new(blob, Arc::clone(&self.metrics)));
        self.rtt_latency_tasks.push(Arc::new(
            blob_rtt_latency_task(
                Arc::clone(&blob),
                Arc::clone(&self.metrics),
                Self::PROMETHEUS_SCRAPE_INTERVAL,
            )
            .await,
        ));
        Ok(blob)
    }
}

impl Drop for PersistClientCache {
    fn drop(&mut self) {
        for task in self.rtt_latency_tasks.iter() {
            task.abort();
        }
    }
}

/// Starts a task to periodically measure the persist-observed latency to
/// consensus.
///
/// This is a task, rather than something like looking at the latencies of prod
/// traffic, so that we minimize any issues around Futures not being polled
/// promptly (as can and does happen with the Timely-polled Futures).
///
/// The caller is responsible for shutdown via [JoinHandle::abort].
///
/// No matter whether we wrap MetricsConsensus before or after we start up the
/// rtt latency task, there's the possibility for it being confusing at some
/// point. Err on the side of more data (including the latency measurements) to
/// start.
async fn blob_rtt_latency_task(
    blob: Arc<MetricsBlob>,
    metrics: Arc<Metrics>,
    measurement_interval: Duration,
) -> JoinHandle<()> {
    mz_ore::task::spawn(|| "persist::blob_rtt_latency", async move {
        // Use the tokio Instant for next_measurement because the reclock tests
        // mess with the tokio sleep clock.
        let mut next_measurement = tokio::time::Instant::now();
        loop {
            tokio::time::sleep_until(next_measurement).await;
            let start = Instant::now();
            // Don't spam retries if this returns an error. We're guaranteed by
            // the method signature that we've already got metrics coverage of
            // these, so we'll count the errors.
            match blob.get(BLOB_GET_LIVENESS_KEY).await {
                Ok(_) => {}
                Err(_) => {
                    next_measurement = tokio::time::Instant::now() + measurement_interval;
                    continue;
                }
            }
            metrics.blob.rtt_latency.set(start.elapsed().as_secs_f64());
            next_measurement = tokio::time::Instant::now() + measurement_interval;
        }
    })
}

/// Starts a task to periodically measure the persist-observed latency to
/// consensus.
///
/// This is a task, rather than something like looking at the latencies of prod
/// traffic, so that we minimize any issues around Futures not being polled
/// promptly (as can and does happen with the Timely-polled Futures).
///
/// The caller is responsible for shutdown via [JoinHandle::abort].
///
/// No matter whether we wrap MetricsConsensus before or after we start up the
/// rtt latency task, there's the possibility for it being confusing at some
/// point. Err on the side of more data (including the latency measurements) to
/// start.
async fn consensus_rtt_latency_task(
    consensus: Arc<MetricsConsensus>,
    metrics: Arc<Metrics>,
    measurement_interval: Duration,
) -> JoinHandle<()> {
    mz_ore::task::spawn(|| "persist::blob_rtt_latency", async move {
        // Use the tokio Instant for next_measurement because the reclock tests
        // mess with the tokio sleep clock.
        let mut next_measurement = tokio::time::Instant::now();
        loop {
            tokio::time::sleep_until(next_measurement).await;
            let start = Instant::now();
            // Don't spam retries if this returns an error. We're guaranteed by
            // the method signature that we've already got metrics coverage of
            // these, so we'll count the errors.
            match consensus.head(CONSENSUS_HEAD_LIVENESS_KEY).await {
                Ok(_) => {}
                Err(_) => {
                    next_measurement = tokio::time::Instant::now() + measurement_interval;
                    continue;
                }
            }
            metrics
                .consensus
                .rtt_latency
                .set(start.elapsed().as_secs_f64());
            next_measurement = tokio::time::Instant::now() + measurement_interval;
        }
    })
}

#[cfg(test)]
mod tests {
    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::now::SYSTEM_TIME;

    use super::*;

    #[tokio::test]
    async fn client_cache() {
        let mut cache = PersistClientCache::new(
            PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone()),
            &MetricsRegistry::new(),
        );
        assert_eq!(cache.blob_by_uri.len(), 0);
        assert_eq!(cache.consensus_by_uri.len(), 0);

        // Opening a location on an empty cache saves the results.
        let _ = cache
            .open(PersistLocation {
                blob_uri: "mem://blob_zero".to_owned(),
                consensus_uri: "mem://consensus_zero".to_owned(),
            })
            .await
            .expect("failed to open location");
        assert_eq!(cache.blob_by_uri.len(), 1);
        assert_eq!(cache.consensus_by_uri.len(), 1);

        // Opening a location with an already opened consensus reuses it, even
        // if the blob is different.
        let _ = cache
            .open(PersistLocation {
                blob_uri: "mem://blob_one".to_owned(),
                consensus_uri: "mem://consensus_zero".to_owned(),
            })
            .await
            .expect("failed to open location");
        assert_eq!(cache.blob_by_uri.len(), 2);
        assert_eq!(cache.consensus_by_uri.len(), 1);

        // Ditto the other way.
        let _ = cache
            .open(PersistLocation {
                blob_uri: "mem://blob_one".to_owned(),
                consensus_uri: "mem://consensus_one".to_owned(),
            })
            .await
            .expect("failed to open location");
        assert_eq!(cache.blob_by_uri.len(), 2);
        assert_eq!(cache.consensus_by_uri.len(), 2);

        // Query params and path matter, so we get new instances.
        let _ = cache
            .open(PersistLocation {
                blob_uri: "mem://blob_one?foo".to_owned(),
                consensus_uri: "mem://consensus_one/bar".to_owned(),
            })
            .await
            .expect("failed to open location");
        assert_eq!(cache.blob_by_uri.len(), 3);
        assert_eq!(cache.consensus_by_uri.len(), 3);

        // User info and port also matter, so we get new instances.
        let _ = cache
            .open(PersistLocation {
                blob_uri: "mem://user@blob_one".to_owned(),
                consensus_uri: "mem://@consensus_one:123".to_owned(),
            })
            .await
            .expect("failed to open location");
        assert_eq!(cache.blob_by_uri.len(), 4);
        assert_eq!(cache.consensus_by_uri.len(), 4);
    }
}
