// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Configuration for [crate::location] implementations.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use tracing::warn;
use url::Url;

use crate::file::{FileBlob, FileBlobConfig};
use crate::location::{Blob, Consensus, ExternalError};
use crate::mem::{MemBlob, MemBlobConfig, MemConsensus};
use crate::metrics::{PostgresConsensusMetrics, S3BlobMetrics};
use crate::postgres::{PostgresConsensus, PostgresConsensusConfig};
use crate::s3::{S3Blob, S3BlobConfig};

/// Config for an implementation of [Blob].
#[derive(Debug, Clone)]
pub enum BlobConfig {
    /// Config for [FileBlob].
    File(FileBlobConfig),
    /// Config for [S3Blob].
    S3(S3BlobConfig),
    /// Config for [MemBlob], only available in testing to prevent
    /// footguns.
    Mem,
}

/// Configuration knobs for [Blob].
pub trait BlobKnobs: std::fmt::Debug + Send + Sync {
    /// Maximum time allowed for a network call, including retry attempts.
    fn operation_timeout(&self) -> Duration;
    /// Maximum time allowed for a single network call.
    fn operation_attempt_timeout(&self) -> Duration;
    /// Maximum time to wait for a socket connection to be made.
    fn connect_timeout(&self) -> Duration;
    /// Maximum time to wait to read the first byte of a response, including connection time.
    fn read_timeout(&self) -> Duration;
}

impl BlobConfig {
    /// Opens the associated implementation of [Blob].
    pub async fn open(self) -> Result<Arc<dyn Blob + Send + Sync>, ExternalError> {
        match self {
            BlobConfig::File(config) => Ok(Arc::new(FileBlob::open(config).await?)),
            BlobConfig::S3(config) => Ok(Arc::new(S3Blob::open(config).await?)),
            BlobConfig::Mem => Ok(Arc::new(MemBlob::open(MemBlobConfig::default()))),
        }
    }

    /// Parses a [Blob] config from a uri string.
    pub async fn try_from(
        value: &str,
        knobs: Box<dyn BlobKnobs>,
        metrics: S3BlobMetrics,
    ) -> Result<Self, ExternalError> {
        let url = Url::parse(value)
            .map_err(|err| anyhow!("failed to parse blob location {} as a url: {}", &value, err))?;
        let mut query_params = url.query_pairs().collect::<BTreeMap<_, _>>();

        let config = match url.scheme() {
            "file" => {
                let config = FileBlobConfig::from(url.path());
                Ok(BlobConfig::File(config))
            }
            "s3" => {
                let bucket = url
                    .host()
                    .ok_or_else(|| anyhow!("missing bucket: {}", &url.as_str()))?
                    .to_string();
                let prefix = url
                    .path()
                    .strip_prefix('/')
                    .unwrap_or_else(|| url.path())
                    .to_string();
                let role_arn = query_params.remove("role_arn").map(|x| x.into_owned());
                let endpoint = query_params.remove("endpoint").map(|x| x.into_owned());
                let region = query_params.remove("region").map(|x| x.into_owned());

                let credentials = match url.password() {
                    None => None,
                    Some(password) => Some((url.username().to_string(), password.to_string())),
                };

                let config = S3BlobConfig::new(
                    bucket,
                    prefix,
                    role_arn,
                    endpoint,
                    region,
                    credentials,
                    knobs,
                    metrics,
                )
                .await?;

                Ok(BlobConfig::S3(config))
            }
            "mem" => {
                if !cfg!(debug_assertions) {
                    warn!("persist unexpectedly using in-mem blob in a release binary");
                }
                query_params.clear();
                Ok(BlobConfig::Mem)
            }
            p => Err(anyhow!(
                "unknown persist blob scheme {}: {}",
                p,
                url.as_str()
            )),
        }?;

        if !query_params.is_empty() {
            return Err(ExternalError::from(anyhow!(
                "unknown blob location params {}: {}",
                query_params
                    .keys()
                    .map(|x| x.as_ref())
                    .collect::<Vec<_>>()
                    .join(" "),
                url.as_str(),
            )));
        }

        Ok(config)
    }
}

/// Config for an implementation of [Consensus].
#[derive(Debug, Clone)]
pub enum ConsensusConfig {
    /// Config for [PostgresConsensus].
    Postgres(PostgresConsensusConfig),
    /// Config for [MemConsensus], only available in testing.
    Mem,
}

/// Configuration knobs for [Consensus].
pub trait ConsensusKnobs: std::fmt::Debug + Send + Sync {
    /// Maximum number of connections allowed in a pool.
    fn connection_pool_max_size(&self) -> usize;
    /// Maximum number of connections allowed in a pool for batched CaS operations.
    fn connection_pool_batch_cas_max_size(&self) -> usize;
    /// Minimum TTL of a connection. It is expected that connections are
    /// routinely culled to balance load to the backing store of [Consensus].
    fn connection_pool_ttl(&self) -> Duration;
    /// Minimum time between TTLing connections. Helps stagger reconnections
    /// to avoid stampeding the backing store of [Consensus].
    fn connection_pool_ttl_stagger(&self) -> Duration;
    /// Time to wait for a connection to be made before trying.
    fn connect_timeout(&self) -> Duration;
    /// Enable batch CaS operations.
    fn batch_cas_enabled(&self) -> bool;
    /// Maximum batch size for batch CaS operations.
    fn max_batch_cas_size(&self) -> usize;
    /// Maximum time to wait to batch CaS operations.
    fn batch_cas_flush_interval(&self) -> Duration;
    /// Whether to use time-based flushing.
    fn batch_cas_flush_interval_enabled(&self) -> bool;
}

impl ConsensusConfig {
    /// Opens the associated implementation of [Consensus].
    pub async fn open(self) -> Result<Arc<dyn Consensus + Send + Sync>, ExternalError> {
        match self {
            ConsensusConfig::Postgres(config) => {
                Ok(Arc::new(PostgresConsensus::open(config).await?))
            }
            ConsensusConfig::Mem => Ok(Arc::new(MemConsensus::default())),
        }
    }

    /// Parses a [Consensus] config from a uri string.
    pub fn try_from(
        value: &str,
        knobs: Box<dyn ConsensusKnobs>,
        metrics: PostgresConsensusMetrics,
    ) -> Result<Self, ExternalError> {
        let url = Url::parse(value).map_err(|err| {
            anyhow!(
                "failed to parse consensus location {} as a url: {}",
                &value,
                err
            )
        })?;

        let config = match url.scheme() {
            "postgres" | "postgresql" => Ok(ConsensusConfig::Postgres(
                PostgresConsensusConfig::new(value, knobs, metrics)?,
            )),
            "mem" => {
                if !cfg!(debug_assertions) {
                    warn!("persist unexpectedly using in-mem consensus in a release binary");
                }
                Ok(ConsensusConfig::Mem)
            }
            p => Err(anyhow!(
                "unknown persist consensus scheme {}: {}",
                p,
                url.as_str()
            )),
        }?;
        Ok(config)
    }
}
