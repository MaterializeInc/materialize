// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A cache of [PersistClient]s indexed by [PersistLocation]s.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use mz_ore::metrics::MetricsRegistry;
use mz_persist::cfg::{BlobConfig, ConsensusConfig};
use mz_persist::location::{Blob, Consensus, ExternalError};
use tracing::debug;

use crate::r#impl::machine::retry_external;
use crate::r#impl::metrics::{Metrics, MetricsBlob, MetricsConsensus};
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
    metrics: Arc<Metrics>,
    blob_by_uri: HashMap<String, Arc<dyn Blob + Send + Sync>>,
    consensus_by_uri: HashMap<String, Arc<dyn Consensus + Send + Sync>>,
}

impl PersistClientCache {
    /// Returns a new [PersistClientCache].
    pub fn new(registry: &MetricsRegistry) -> Self {
        PersistClientCache {
            cfg: PersistConfig::default(),
            metrics: Arc::new(Metrics::new(registry)),
            blob_by_uri: HashMap::new(),
            consensus_by_uri: HashMap::new(),
        }
    }

    /// A test helper that returns a [PersistClientCache] disconnected from
    /// metrics.
    #[cfg(test)]
    pub fn new_no_metrics() -> Self {
        Self::new(&MetricsRegistry::new())
    }

    /// Returns a new [PersistClient] for interfacing with persist shards made
    /// durable to the given [PersistLocation].
    ///
    /// The same `location` may be used concurrently from multiple processes.
    pub async fn open(
        &mut self,
        location: PersistLocation,
    ) -> Result<PersistClient, ExternalError> {
        debug!(
            "PersistClientCache::open blob={} consensus={}",
            location.blob_uri, location.consensus_uri,
        );
        let blob = match self.blob_by_uri.entry(location.blob_uri) {
            Entry::Occupied(x) => Arc::clone(x.get()),
            Entry::Vacant(x) => {
                // Intentionally hold the lock, so we don't double connect under
                // concurrency.
                let blob = BlobConfig::try_from(x.key()).await?;
                let blob = retry_external(&self.metrics.retries.external.blob_open, || {
                    blob.clone().open()
                })
                .await;
                Arc::clone(x.insert(blob))
            }
        };
        let blob = Arc::new(MetricsBlob::new(blob, Arc::clone(&self.metrics)))
            as Arc<dyn Blob + Send + Sync>;
        let consensus = match self.consensus_by_uri.entry(location.consensus_uri) {
            Entry::Occupied(x) => Arc::clone(x.get()),
            Entry::Vacant(x) => {
                // Intentionally hold the lock, so we don't double connect under
                // concurrency.
                let consensus = ConsensusConfig::try_from(x.key()).await?;
                let consensus =
                    retry_external(&self.metrics.retries.external.consensus_open, || {
                        consensus.clone().open()
                    })
                    .await;
                Arc::clone(x.insert(consensus))
            }
        };
        let consensus = Arc::new(MetricsConsensus::new(consensus, Arc::clone(&self.metrics)))
            as Arc<dyn Consensus + Send + Sync>;
        PersistClient::new(self.cfg.clone(), blob, consensus, Arc::clone(&self.metrics)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn client_cache() {
        let mut cache = PersistClientCache::new(&MetricsRegistry::new());
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
