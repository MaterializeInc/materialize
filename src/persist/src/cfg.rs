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
use mz_dyncfg::ConfigSet;
use mz_ore::url::SensitiveUrl;
use tracing::warn;

use mz_postgres_client::PostgresClientKnobs;
use mz_postgres_client::metrics::PostgresClientMetrics;

use crate::azure::{AzureBlob, AzureBlobConfig};
use crate::file::{FileBlob, FileBlobConfig};
use crate::foundationdb::{FdbConsensus, FdbConsensusConfig};
use crate::location::{Blob, Consensus, Determinate, ExternalError};
use crate::mem::{MemBlob, MemBlobConfig, MemConsensus};
use crate::metrics::S3BlobMetrics;
use crate::postgres::{PostgresConsensus, PostgresConsensusConfig};
use crate::s3::{S3Blob, S3BlobConfig};

/// Adds the full set of all mz_persist `Config`s.
pub fn all_dyn_configs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&crate::indexed::columnar::arrow::ENABLE_ARROW_LGALLOC_CC_SIZES)
        .add(&crate::indexed::columnar::arrow::ENABLE_ARROW_LGALLOC_NONCC_SIZES)
        .add(&crate::s3::ENABLE_S3_LGALLOC_CC_SIZES)
        .add(&crate::s3::ENABLE_S3_LGALLOC_NONCC_SIZES)
        .add(&crate::postgres::USE_POSTGRES_TUNED_QUERIES)
}

/// Config for an implementation of [Blob].
#[derive(Debug, Clone)]
pub enum BlobConfig {
    /// Config for [FileBlob].
    File(FileBlobConfig),
    /// Config for [S3Blob].
    S3(S3BlobConfig),
    /// Config for [MemBlob], only available in testing to prevent
    /// footguns.
    Mem(bool),
    /// Config for [AzureBlob].
    Azure(AzureBlobConfig),
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
    /// Whether this is running in a "cc" sized cluster.
    fn is_cc_active(&self) -> bool;
}

impl BlobConfig {
    /// Opens the associated implementation of [Blob].
    pub async fn open(self) -> Result<Arc<dyn Blob>, ExternalError> {
        match self {
            BlobConfig::File(config) => Ok(Arc::new(FileBlob::open(config).await?)),
            BlobConfig::S3(config) => Ok(Arc::new(S3Blob::open(config).await?)),
            BlobConfig::Azure(config) => Ok(Arc::new(AzureBlob::open(config).await?)),
            BlobConfig::Mem(tombstone) => {
                Ok(Arc::new(MemBlob::open(MemBlobConfig::new(tombstone))))
            }
        }
    }

    /// Parses a [Blob] config from a uri string.
    pub async fn try_from(
        url: &SensitiveUrl,
        knobs: Box<dyn BlobKnobs>,
        metrics: S3BlobMetrics,
        cfg: Arc<ConfigSet>,
    ) -> Result<Self, ExternalError> {
        let mut query_params = url.query_pairs().collect::<BTreeMap<_, _>>();

        let config = match url.scheme() {
            "file" => {
                let mut config = FileBlobConfig::from(url.path());
                if query_params.remove("tombstone").is_some() {
                    config.tombstone = true;
                }
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
                    Some(password) => Some((
                        String::from_utf8_lossy(&urlencoding::decode_binary(
                            url.username().as_bytes(),
                        ))
                        .into_owned(),
                        String::from_utf8_lossy(&urlencoding::decode_binary(password.as_bytes()))
                            .into_owned(),
                    )),
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
                    cfg,
                )
                .await?;

                Ok(BlobConfig::S3(config))
            }
            "mem" => {
                if !cfg!(debug_assertions) {
                    warn!("persist unexpectedly using in-mem blob in a release binary");
                }
                let tombstone = match query_params.remove("tombstone").as_deref() {
                    None | Some("true") => true,
                    Some("false") => false,
                    Some(other) => Err(Determinate::new(anyhow!(
                        "invalid tombstone param value: {other}"
                    )))?,
                };
                query_params.clear();
                Ok(BlobConfig::Mem(tombstone))
            }
            "http" | "https" => match url
                .host()
                .ok_or_else(|| anyhow!("missing protocol: {}", &url.as_str()))?
                .to_string()
                .split_once('.')
            {
                // The Azurite emulator always uses the well-known account name devstoreaccount1
                Some((account, root))
                    if account == "devstoreaccount1" || root == "blob.core.windows.net" =>
                {
                    if let Some(container) = url
                        .path_segments()
                        .expect("azure blob storage container")
                        .next()
                    {
                        query_params.clear();
                        Ok(BlobConfig::Azure(AzureBlobConfig::new(
                            account.to_string(),
                            container.to_string(),
                            // Azure doesn't support prefixes in the way S3 does.
                            // This is always empty, but we leave the field for
                            // compatibility with our existing test suite.
                            "".to_string(),
                            metrics,
                            url.clone().into_redacted(),
                            knobs,
                            cfg,
                        )?))
                    } else {
                        Err(anyhow!("unknown persist blob scheme: {}", url.as_str()))
                    }
                }
                _ => Err(anyhow!("unknown persist blob scheme: {}", url.as_str())),
            },
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
    /// Config for [FdbConsensus].
    FoundationDB(FdbConsensusConfig),
    /// Config for [PostgresConsensus].
    Postgres(PostgresConsensusConfig),
    /// Config for [MemConsensus], only available in testing.
    Mem,
}

impl ConsensusConfig {
    /// Opens the associated implementation of [Consensus].
    pub async fn open(self) -> Result<Arc<dyn Consensus>, ExternalError> {
        match self {
            ConsensusConfig::FoundationDB(config) => {
                Ok(Arc::new(FdbConsensus::open(config).await?))
            }
            ConsensusConfig::Postgres(config) => {
                Ok(Arc::new(PostgresConsensus::open(config).await?))
            }
            ConsensusConfig::Mem => Ok(Arc::new(MemConsensus::default())),
        }
    }

    /// Parses a [Consensus] config from a uri string.
    pub fn try_from(
        url: &SensitiveUrl,
        knobs: Box<dyn PostgresClientKnobs>,
        metrics: PostgresClientMetrics,
        dyncfg: Arc<ConfigSet>,
    ) -> Result<Self, ExternalError> {
        let config = match url.scheme() {
            "fdb" | "foundationdb" => Ok(ConsensusConfig::FoundationDB(FdbConsensusConfig::new(
                url.clone(),
            )?)),
            "postgres" | "postgresql" => Ok(ConsensusConfig::Postgres(
                PostgresConsensusConfig::new(url, knobs, metrics, dyncfg)?,
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
