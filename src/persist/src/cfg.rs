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
use crate::crypto::{BlobEncryptionConfig, EncryptedBlob, EncryptedConsensus, EncryptionConfig};
use crate::file::{FileBlob, FileBlobConfig};
#[cfg(feature = "foundationdb")]
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
    /// Config for [S3Blob], with optional envelope encryption.
    S3(S3BlobConfig, Option<BlobEncryptionConfig>),
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
            BlobConfig::S3(config, encryption) => {
                let blob: Arc<dyn Blob> = Arc::new(S3Blob::open(config).await?);
                match encryption {
                    None => Ok(blob),
                    Some(enc_cfg) => {
                        let kms_client = enc_cfg.build_kms_client().await?;
                        let customer_kms_client =
                            enc_cfg.build_customer_kms_client().await?;
                        let customer_kms_key_id = enc_cfg.customer_kms_key_id.clone();
                        Ok(Arc::new(
                            EncryptedBlob::new(
                                blob,
                                kms_client,
                                enc_cfg.kms_key_id,
                                enc_cfg.dek_rotation_interval,
                                customer_kms_client,
                                customer_kms_key_id,
                            )
                            .await?,
                        ))
                    }
                }
            }
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

                let kms_key_id = query_params.remove("kms_key_id").map(|x| x.into_owned());
                let kms_region = query_params.remove("kms_region").map(|x| x.into_owned());
                let dek_rotation_interval_secs = query_params
                    .remove("dek_rotation_interval_secs")
                    .and_then(|x| x.parse::<u64>().ok());

                let customer_kms_key_id =
                    query_params.remove("customer_kms_key_id").map(|x| x.into_owned());
                let customer_kms_region =
                    query_params.remove("customer_kms_region").map(|x| x.into_owned());
                let customer_kms_endpoint =
                    query_params.remove("customer_kms_endpoint").map(|x| x.into_owned());
                let customer_kms_role_arn =
                    query_params.remove("customer_kms_role_arn").map(|x| x.into_owned());

                let encryption = kms_key_id.map(|key_id| BlobEncryptionConfig {
                    kms_key_id: key_id,
                    kms_region: kms_region.or_else(|| region.clone()),
                    endpoint: endpoint.clone(),
                    role_arn: role_arn.clone(),
                    dek_rotation_interval: Duration::from_secs(
                        dek_rotation_interval_secs.unwrap_or(300),
                    ),
                    customer_kms_key_id,
                    customer_kms_region,
                    customer_kms_endpoint,
                    customer_kms_role_arn,
                });

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

                Ok(BlobConfig::S3(config, encryption))
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
            #[cfg(feature = "turmoil")]
            "turmoil" => {
                let cfg = crate::turmoil::BlobConfig::new(url);
                Ok(BlobConfig::Turmoil(cfg))
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
    #[cfg(feature = "foundationdb")]
    /// Config for FoundationDB.
    FoundationDB(FdbConsensusConfig),
    /// Config for [PostgresConsensus], with optional envelope encryption.
    Postgres(PostgresConsensusConfig, Option<EncryptionConfig>),
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
            ConsensusConfig::Postgres(config, encryption) => {
                let consensus: Arc<dyn Consensus> =
                    Arc::new(PostgresConsensus::open(config).await?);
                match encryption {
                    None => Ok(consensus),
                    Some(enc_cfg) => {
                        let kms_client = enc_cfg.build_kms_client().await?;
                        let customer_kms_client =
                            enc_cfg.build_customer_kms_client().await?;
                        let customer_kms_key_id = enc_cfg.customer_kms_key_id.clone();
                        Ok(Arc::new(
                            EncryptedConsensus::new(
                                consensus,
                                kms_client,
                                enc_cfg.kms_key_id,
                                enc_cfg.dek_rotation_interval,
                                customer_kms_client,
                                customer_kms_key_id,
                            )
                            .await?,
                        ))
                    }
                }
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
            "postgres" | "postgresql" => {
                let mut query_params = url.query_pairs().collect::<BTreeMap<_, _>>();
                let kms_key_id = query_params.remove("kms_key_id").map(|x| x.into_owned());
                let kms_region = query_params.remove("kms_region").map(|x| x.into_owned());
                let dek_rotation_interval_secs = query_params
                    .remove("dek_rotation_interval_secs")
                    .and_then(|x| x.parse::<u64>().ok());

                let customer_kms_key_id =
                    query_params.remove("customer_kms_key_id").map(|x| x.into_owned());
                let customer_kms_region =
                    query_params.remove("customer_kms_region").map(|x| x.into_owned());
                let customer_kms_role_arn =
                    query_params.remove("customer_kms_role_arn").map(|x| x.into_owned());

                let encryption = kms_key_id.map(|key_id| EncryptionConfig {
                    kms_key_id: key_id,
                    kms_region,
                    endpoint: None,
                    role_arn: None,
                    dek_rotation_interval: Duration::from_secs(
                        dek_rotation_interval_secs.unwrap_or(300),
                    ),
                    customer_kms_key_id,
                    customer_kms_region,
                    customer_kms_endpoint: None,
                    customer_kms_role_arn,
                });

                // Strip KMS params from URL before passing to Postgres driver.
                let clean_url = if encryption.is_some() {
                    let mut inner = url.0.clone();
                    inner.set_query(None);
                    for (k, v) in &query_params {
                        inner.query_pairs_mut().append_pair(k, v);
                    }
                    SensitiveUrl(inner)
                } else {
                    url.clone()
                };

                Ok(ConsensusConfig::Postgres(
                    PostgresConsensusConfig::new(&clean_url, knobs, metrics, dyncfg)?,
                    encryption,
                ))
            }
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
            p => Err(anyhow!(
                "unknown persist consensus scheme {}: {}",
                p,
                url.as_str()
            )),
        }?;
        Ok(config)
    }
}
