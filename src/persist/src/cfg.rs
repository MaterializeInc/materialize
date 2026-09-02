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
#[cfg(feature = "foundationdb")]
use crate::foundationdb::{FdbConsensus, FdbConsensusConfig};
use crate::hedge::HedgeSibling;
use crate::location::{Blob, Consensus, Determinate, ExternalError};
use crate::mem::{MemBlob, MemBlobConfig, MemConsensus};
use crate::metrics::S3BlobMetrics;
use crate::postgres::{PostgresConsensus, PostgresConsensusConfig};
use crate::s3::{S3Blob, S3BlobConfig};

/// Adds the full set of all mz_persist `Config`s.
pub fn all_dyn_configs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&crate::postgres::PG_CONSENSUS_READ_COMMITTED)
        .add(&crate::hedge::BLOB_HEDGED_GET_ENABLED)
        .add(&crate::hedge::BLOB_HEDGED_GET_DELAY)
        .add(&crate::hedge::BLOB_HEDGED_GET_MAX_CONCURRENT)
        .add(&crate::hedge::BLOB_HEDGED_GET_BUDGET_RATIO)
        .add(&crate::hedge::BLOB_HEDGED_GET_WARM_INTERVAL)
}

/// Opens the sibling handle that [crate::hedge::HedgedBlob] runs hedge
/// requests on for `url`.
///
/// Contract:
/// - An [HedgeSibling::Isolated] handle observes exactly the same durable
///   store as a handle opened from the same `url`, but is built from a
///   scratch client: it shares no HTTP connection pool, DNS state, or
///   credential chain, so a hedge request on it can never be assigned a
///   connection the primary's pool has already half-killed.
/// - Backends where a second open would observe an independent store (mem,
///   turmoil's simulated store), or that have no connection state to isolate
///   (file), return [HedgeSibling::SharedWithPrimary] instead.
/// - Callers must use the handle only for idempotent reads.
///
/// Errors opening the sibling degrade to [HedgeSibling::Unavailable] with a
/// warning rather than failing: persist must come up even if hedging cannot.
/// A process that hits this keeps hedging unavailable until restart, visible
/// as `mz_persist_blob_hedges_skipped{reason="unavailable"}` and
/// `mz_persist_blob_hedge_armed` staying 0.
pub async fn open_hedge_sibling(
    url: &SensitiveUrl,
    knobs: Box<dyn BlobKnobs>,
    metrics: S3BlobMetrics,
) -> HedgeSibling {
    let config = match BlobConfig::try_from(url, knobs, metrics).await {
        Ok(config) => config,
        Err(err) => {
            warn!(
                "hedged blob gets unavailable, sibling config failed: {}",
                err
            );
            return HedgeSibling::Unavailable;
        }
    };
    match config {
        // A second S3/Azure config builds its own SDK client and therefore
        // its own connection pool, with DNS resolved per connect.
        config @ (BlobConfig::S3(_) | BlobConfig::Azure(_)) => match config.open().await {
            Ok(blob) => HedgeSibling::Isolated(blob),
            Err(err) => {
                warn!("hedged blob gets unavailable, sibling open failed: {}", err);
                HedgeSibling::Unavailable
            }
        },
        // File has no connection pool to isolate, so a second instance would
        // buy nothing. A second open of Mem (or of turmoil's simulated
        // store) would be actively wrong: it creates an INDEPENDENT store,
        // and a hedged get against a different store can return `Ok(None)`
        // for data that exists.
        BlobConfig::File(_) | BlobConfig::Mem(_) => HedgeSibling::SharedWithPrimary,
        #[cfg(feature = "turmoil")]
        BlobConfig::Turmoil(_) => HedgeSibling::SharedWithPrimary,
    }
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
    #[cfg(feature = "turmoil")]
    /// Config for [crate::turmoil::TurmoilBlob].
    Turmoil(crate::turmoil::BlobConfig),
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
            #[cfg(feature = "turmoil")]
            BlobConfig::Turmoil(config) => Ok(Arc::new(crate::turmoil::TurmoilBlob::open(config))),
        }
    }

    /// Parses a [Blob] config from a uri string.
    pub async fn try_from(
        url: &SensitiveUrl,
        knobs: Box<dyn BlobKnobs>,
        metrics: S3BlobMetrics,
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
                    .ok_or_else(|| anyhow!("missing bucket: {}", url))?
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
                .ok_or_else(|| anyhow!("missing protocol: {}", url))?
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
                        )?))
                    } else {
                        Err(anyhow!("unknown persist blob scheme: {}", url))
                    }
                }
                _ => Err(anyhow!("unknown persist blob scheme: {}", url)),
            },
            #[cfg(feature = "turmoil")]
            "turmoil" => {
                let cfg = crate::turmoil::BlobConfig::new(url);
                Ok(BlobConfig::Turmoil(cfg))
            }
            p => Err(anyhow!("unknown persist blob scheme {}: {}", p, url)),
        }?;

        if !query_params.is_empty() {
            return Err(ExternalError::from(anyhow!(
                "unknown blob location params {}: {}",
                query_params
                    .keys()
                    .map(|x| x.as_ref())
                    .collect::<Vec<_>>()
                    .join(" "),
                url,
            )));
        }

        Ok(config)
    }
}

/// Config for an implementation of [Consensus].
#[derive(Debug, Clone)]
pub enum ConsensusConfig {
    #[cfg(feature = "foundationdb")]
    /// Config for FoundationDB.
    FoundationDB(FdbConsensusConfig),
    /// Config for [PostgresConsensus].
    Postgres(PostgresConsensusConfig),
    /// Config for [MemConsensus], only available in testing.
    Mem,
    #[cfg(feature = "turmoil")]
    /// Config for [crate::turmoil::TurmoilConsensus].
    Turmoil(crate::turmoil::ConsensusConfig),
}

impl ConsensusConfig {
    /// Opens the associated implementation of [Consensus].
    pub async fn open(self) -> Result<Arc<dyn Consensus>, ExternalError> {
        match self {
            #[cfg(feature = "foundationdb")]
            ConsensusConfig::FoundationDB(config) => {
                Ok(Arc::new(FdbConsensus::open(config).await?))
            }
            ConsensusConfig::Postgres(config) => {
                Ok(Arc::new(PostgresConsensus::open(config).await?))
            }
            ConsensusConfig::Mem => Ok(Arc::new(MemConsensus::default())),
            #[cfg(feature = "turmoil")]
            ConsensusConfig::Turmoil(config) => {
                Ok(Arc::new(crate::turmoil::TurmoilConsensus::open(config)))
            }
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
            #[cfg(feature = "foundationdb")]
            "foundationdb" => Ok(ConsensusConfig::FoundationDB(FdbConsensusConfig::new(
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
            #[cfg(feature = "turmoil")]
            "turmoil" => {
                let cfg = crate::turmoil::ConsensusConfig::new(url);
                Ok(ConsensusConfig::Turmoil(cfg))
            }
            p => Err(anyhow!("unknown persist consensus scheme {}: {}", p, url)),
        }?;
        Ok(config)
    }
}
