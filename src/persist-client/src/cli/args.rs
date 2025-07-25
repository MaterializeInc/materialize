// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI argument types for persist

use crate::ShardId;
use crate::cfg::PersistConfig;
use crate::internal::metrics::{MetricsBlob, MetricsConsensus};
use crate::internal::state_versions::StateVersions;
use crate::metrics::Metrics;
use async_trait::async_trait;
use bytes::Bytes;
use mz_build_info::BuildInfo;
use mz_ore::bytes::SegmentedBytes;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist::cfg::{BlobConfig, ConsensusConfig};
use mz_persist::location::{
    Blob, BlobMetadata, CaSResult, Consensus, ExternalError, ResultStream, SeqNo, Tasked,
    VersionedData,
};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::warn;

/// Arguments for commands that work over both backing stores.
/// TODO: squish this into `StateArgs`.
#[derive(Debug, Clone, clap::Parser)]
pub struct StoreArgs {
    /// Consensus to use.
    ///
    /// When connecting to a deployed environment's consensus table, the Postgres/CRDB connection
    /// string must contain the database name and `options=--search_path=consensus`.
    ///
    /// When connecting to Cockroach Cloud, use the following format:
    ///
    /// ```text
    /// postgresql://<user>:$COCKROACH_PW@<hostname>:<port>/environment_<environment-id>
    ///   ?sslmode=verify-full
    ///   &sslrootcert=/path/to/cockroach-cloud/certs/cluster-ca.crt
    ///   &options=--search_path=consensus
    /// ```
    ///
    #[clap(long, verbatim_doc_comment, env = "CONSENSUS_URI")]
    pub(crate) consensus_uri: SensitiveUrl,

    /// Blob to use
    ///
    /// When connecting to a deployed environment's blob, the necessary connection glue must be in
    /// place. e.g. for S3, sign into SSO, set AWS_PROFILE and AWS_REGION appropriately, with a blob
    /// URI scoped to the environment's bucket prefix.
    #[clap(long, env = "BLOB_URI")]
    pub(crate) blob_uri: SensitiveUrl,
}

/// Arguments for viewing the current state of a given shard
#[derive(Debug, Clone, clap::Parser)]
pub struct StateArgs {
    /// Shard to view
    #[clap(long)]
    pub(crate) shard_id: String,

    /// Consensus to use.
    ///
    /// When connecting to a deployed environment's consensus table, the Postgres/CRDB connection
    /// string must contain the database name and `options=--search_path=consensus`.
    ///
    /// When connecting to Cockroach Cloud, use the following format:
    ///
    /// ```text
    /// postgresql://<user>:$COCKROACH_PW@<hostname>:<port>/environment_<environment-id>
    ///   ?sslmode=verify-full
    ///   &sslrootcert=/path/to/cockroach-cloud/certs/cluster-ca.crt
    ///   &options=--search_path=consensus
    /// ```
    ///
    #[clap(long, verbatim_doc_comment, env = "CONSENSUS_URI")]
    pub(crate) consensus_uri: SensitiveUrl,

    /// Blob to use
    ///
    /// When connecting to a deployed environment's blob, the necessary connection glue must be in
    /// place. e.g. for S3, sign into SSO, set AWS_PROFILE and AWS_REGION appropriately, with a blob
    /// URI scoped to the environment's bucket prefix.
    #[clap(long, env = "BLOB_URI")]
    pub(crate) blob_uri: SensitiveUrl,
}

// BuildInfo with a larger version than any version we expect to see in prod,
// to ensure that any data read is from a smaller version and does not trigger
// alerts.
pub(crate) const READ_ALL_BUILD_INFO: BuildInfo = BuildInfo {
    version: "99.999.99+test",
    sha: "0000000000000000000000000000000000000000",
};

// All `inspect` command are read-only.
pub(crate) const NO_COMMIT: bool = false;

impl StateArgs {
    pub(crate) fn shard_id(&self) -> ShardId {
        ShardId::from_str(&self.shard_id).expect("invalid shard id")
    }

    pub(crate) async fn open(&self) -> Result<StateVersions, anyhow::Error> {
        let cfg = PersistConfig::new_default_configs(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
        let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
        let consensus =
            make_consensus(&cfg, &self.consensus_uri, NO_COMMIT, Arc::clone(&metrics)).await?;
        let blob = make_blob(&cfg, &self.blob_uri, NO_COMMIT, Arc::clone(&metrics)).await?;
        Ok(StateVersions::new(cfg, consensus, blob, metrics))
    }
}

pub(super) async fn make_consensus(
    cfg: &PersistConfig,
    consensus_uri: &SensitiveUrl,
    commit: bool,
    metrics: Arc<Metrics>,
) -> anyhow::Result<Arc<dyn Consensus>> {
    let consensus = ConsensusConfig::try_from(
        consensus_uri,
        Box::new(cfg.clone()),
        metrics.postgres_consensus.clone(),
        Arc::clone(&cfg.configs),
    )?;
    let consensus = consensus.clone().open().await?;
    let consensus = if commit {
        consensus
    } else {
        Arc::new(ReadOnly::new(consensus))
    };
    let consensus = Arc::new(MetricsConsensus::new(consensus, Arc::clone(&metrics)));
    let consensus = Arc::new(Tasked(consensus));
    Ok(consensus)
}

pub(super) async fn make_blob(
    cfg: &PersistConfig,
    blob_uri: &SensitiveUrl,
    commit: bool,
    metrics: Arc<Metrics>,
) -> anyhow::Result<Arc<dyn Blob>> {
    let blob =
        BlobConfig::try_from(blob_uri, Box::new(cfg.clone()), metrics.s3_blob.clone()).await?;
    let blob = blob.clone().open().await?;
    let blob = if commit {
        blob
    } else {
        Arc::new(ReadOnly::new(blob))
    };
    let blob = Arc::new(MetricsBlob::new(blob, Arc::clone(&metrics)));
    let blob = Arc::new(Tasked(blob));
    Ok(blob)
}

/// Wrap a lower-level service (Blob or Consensus) to make it read only.
/// This is probably not elaborate enough to work in general -- folks may expect to read
/// their own writes, among other things -- but it should handle the case of GC, where
/// all reads finish before the writes begin.
#[derive(Debug)]
struct ReadOnly<T> {
    store: T,
    ignored_write: AtomicBool,
}

impl<T> ReadOnly<T> {
    fn new(store: T) -> Self {
        Self {
            store,
            ignored_write: AtomicBool::new(false),
        }
    }

    fn ignored_write(&self) -> bool {
        self.ignored_write.load(Ordering::SeqCst)
    }

    fn ignoring_write(&self) {
        self.ignored_write.store(true, Ordering::SeqCst)
    }
}

#[async_trait]
impl Blob for ReadOnly<Arc<dyn Blob>> {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        if self.ignored_write() {
            warn!("potentially-invalid get({key}) after ignored write");
        }
        self.store.get(key).await
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        if self.ignored_write() {
            warn!("potentially-invalid list_keys_and_metadata() after ignored write");
        }
        self.store.list_keys_and_metadata(key_prefix, f).await
    }

    async fn set(&self, key: &str, _value: Bytes) -> Result<(), ExternalError> {
        warn!("ignoring set({key}) in read-only mode");
        self.ignoring_write();
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        warn!("ignoring delete({key}) in read-only mode");
        self.ignoring_write();
        Ok(None)
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        warn!("ignoring restore({key}) in read-only mode");
        self.ignoring_write();
        Ok(())
    }
}

#[async_trait]
impl Consensus for ReadOnly<Arc<dyn Consensus>> {
    fn list_keys(&self) -> ResultStream<String> {
        if self.ignored_write() {
            warn!("potentially-invalid list_keys() after ignored write");
        }
        self.store.list_keys()
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        if self.ignored_write() {
            warn!("potentially-invalid head({key}) after ignored write");
        }
        self.store.head(key).await
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        warn!(
            "ignoring cas({key}) in read-only mode ({} bytes at seqno {expected:?})",
            new.data.len(),
        );
        self.ignoring_write();
        Ok(CaSResult::Committed)
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        if self.ignored_write() {
            warn!("potentially-invalid scan({key}) after ignored write");
        }
        self.store.scan(key, from, limit).await
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<usize, ExternalError> {
        warn!("ignoring truncate({key}) in read-only mode (to seqno {seqno})");
        self.ignoring_write();
        Ok(0)
    }
}
