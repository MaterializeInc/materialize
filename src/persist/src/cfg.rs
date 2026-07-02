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
use std::future::Future;
use std::pin::Pin;
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

/// Minimum DEK rotation interval in seconds. Prevents accidental tight-loop
/// rotation that would flood KMS with `GenerateDataKey` calls.
const MIN_DEK_ROTATION_SECS: u64 = 60;

/// URL query params that configure KMS envelope encryption.
///
/// Must contain every param that [BlobConfig::try_from] and
/// [ConsensusConfig::try_from] consume for encryption, so that
/// [strip_kms_query_params] removes exactly the params persist understands.
pub const KMS_URL_QUERY_PARAMS: &[&str] = &[
    "kms_key_id",
    "kms_region",
    "dek_rotation_interval_secs",
    "customer_kms_key_id",
    "customer_kms_region",
    "customer_kms_endpoint",
    "customer_kms_role_arn",
];

/// Returns `url` with all [KMS_URL_QUERY_PARAMS] removed.
///
/// For callers that hand a URL carrying persist KMS encryption params to a
/// consumer that does not understand them, e.g. deriving a timestamp oracle
/// URL from a shared metadata backend URL.
///
/// NOTE: Surviving query segments are preserved byte-identically. Decoding and
/// re-encoding them is not an option: `append_pair` form-encodes spaces as
/// `+`, while libpq-style URL parsers (e.g. tokio-postgres) percent-decode
/// strictly and keep `+` literal, which corrupts values like the PEM in
/// `sslrootcert_inline`.
pub fn strip_kms_query_params(url: &SensitiveUrl) -> SensitiveUrl {
    let mut inner = url.0.clone();
    if let Some(query) = url.0.query() {
        let keep: Vec<&str> = query
            .split('&')
            .filter(|segment| {
                let key = segment.split('=').next().unwrap_or(segment);
                !KMS_URL_QUERY_PARAMS.contains(&key)
            })
            .collect();
        if keep.is_empty() {
            inner.set_query(None);
        } else {
            inner.set_query(Some(&keep.join("&")));
        }
    }
    SensitiveUrl(inner)
}

/// Adds the full set of all mz_persist `Config`s.
pub fn all_dyn_configs(configs: ConfigSet) -> ConfigSet {
    configs.add(&crate::postgres::USE_POSTGRES_TUNED_QUERIES)
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
                    // NOTE: The construction future is boxed and type-erased
                    // because the AWS config loader and KMS client futures it
                    // awaits have deeply nested types. Inlined, they inflate
                    // the async state machine layout of every transitive
                    // caller, overflowing rustc's recursion limit in
                    // downstream crates.
                    Some(enc_cfg) => {
                        let wrap: Pin<
                            Box<dyn Future<Output = Result<Arc<dyn Blob>, ExternalError>> + Send>,
                        > = Box::pin(async move {
                            let kms_client = enc_cfg.build_kms_client().await?;
                            let customer_kms_client = enc_cfg.build_customer_kms_client().await?;
                            let customer_kms_key_id = enc_cfg.customer_kms_key_id.clone();
                            let blob = EncryptedBlob::new(
                                blob,
                                kms_client,
                                enc_cfg.kms_key_id,
                                enc_cfg.dek_rotation_interval,
                                customer_kms_client,
                                customer_kms_key_id,
                            )
                            .await?;
                            let blob: Arc<dyn Blob> = Arc::new(blob);
                            Ok(blob)
                        });
                        wrap.await
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

                let customer_kms_key_id = query_params
                    .remove("customer_kms_key_id")
                    .map(|x| x.into_owned());
                let customer_kms_region = query_params
                    .remove("customer_kms_region")
                    .map(|x| x.into_owned());
                let customer_kms_endpoint = query_params
                    .remove("customer_kms_endpoint")
                    .map(|x| x.into_owned());
                let customer_kms_role_arn = query_params
                    .remove("customer_kms_role_arn")
                    .map(|x| x.into_owned());

                let encryption = kms_key_id.map(|key_id| {
                    let requested_secs = dek_rotation_interval_secs.unwrap_or(300);
                    let clamped_secs = requested_secs.max(MIN_DEK_ROTATION_SECS);
                    if requested_secs < MIN_DEK_ROTATION_SECS {
                        warn!(
                            requested_secs,
                            min_secs = MIN_DEK_ROTATION_SECS,
                            "dek_rotation_interval_secs below minimum, clamping"
                        );
                    }
                    BlobEncryptionConfig {
                        kms_key_id: key_id,
                        kms_region: kms_region.or_else(|| region.clone()),
                        endpoint: endpoint.clone(),
                        role_arn: role_arn.clone(),
                        dek_rotation_interval: Duration::from_secs(clamped_secs),
                        customer_kms_key_id,
                        customer_kms_region,
                        customer_kms_endpoint,
                        customer_kms_role_arn,
                    }
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
                    // NOTE: Boxed and type-erased for the same reason as the
                    // encrypted arm of [BlobConfig::open].
                    Some(enc_cfg) => {
                        let wrap: Pin<
                            Box<
                                dyn Future<Output = Result<Arc<dyn Consensus>, ExternalError>>
                                    + Send,
                            >,
                        > = Box::pin(async move {
                            let kms_client = enc_cfg.build_kms_client().await?;
                            let customer_kms_client = enc_cfg.build_customer_kms_client().await?;
                            let customer_kms_key_id = enc_cfg.customer_kms_key_id.clone();
                            let consensus = EncryptedConsensus::new(
                                consensus,
                                kms_client,
                                enc_cfg.kms_key_id,
                                enc_cfg.dek_rotation_interval,
                                customer_kms_client,
                                customer_kms_key_id,
                            )
                            .await?;
                            let consensus: Arc<dyn Consensus> = Arc::new(consensus);
                            Ok(consensus)
                        });
                        wrap.await
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

                let customer_kms_key_id = query_params
                    .remove("customer_kms_key_id")
                    .map(|x| x.into_owned());
                let customer_kms_region = query_params
                    .remove("customer_kms_region")
                    .map(|x| x.into_owned());
                let customer_kms_role_arn = query_params
                    .remove("customer_kms_role_arn")
                    .map(|x| x.into_owned());

                let encryption = kms_key_id.map(|key_id| {
                    let requested_secs = dek_rotation_interval_secs.unwrap_or(300);
                    let clamped_secs = requested_secs.max(MIN_DEK_ROTATION_SECS);
                    if requested_secs < MIN_DEK_ROTATION_SECS {
                        warn!(
                            requested_secs,
                            min_secs = MIN_DEK_ROTATION_SECS,
                            "dek_rotation_interval_secs below minimum, clamping"
                        );
                    }
                    EncryptionConfig {
                        kms_key_id: key_id,
                        kms_region,
                        endpoint: None,
                        role_arn: None,
                        dek_rotation_interval: Duration::from_secs(clamped_secs),
                        customer_kms_key_id,
                        customer_kms_region,
                        customer_kms_endpoint: None,
                        customer_kms_role_arn,
                    }
                });

                // Strip KMS params from URL before passing to Postgres driver.
                let clean_url = if encryption.is_some() {
                    strip_kms_query_params(url)
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

#[cfg(test)]
mod tests {
    use std::fmt::Formatter;
    use std::str::FromStr;

    use mz_ore::metrics::MetricsRegistry;

    use super::*;

    struct TestConsensusKnobs;
    impl std::fmt::Debug for TestConsensusKnobs {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestConsensusKnobs").finish_non_exhaustive()
        }
    }
    impl PostgresClientKnobs for TestConsensusKnobs {
        fn connection_pool_max_size(&self) -> usize {
            2
        }
        fn connection_pool_max_wait(&self) -> Option<Duration> {
            Some(Duration::from_secs(1))
        }
        fn connection_pool_ttl(&self) -> Duration {
            Duration::MAX
        }
        fn connection_pool_ttl_stagger(&self) -> Duration {
            Duration::MAX
        }
        fn connect_timeout(&self) -> Duration {
            Duration::MAX
        }
        fn tcp_user_timeout(&self) -> Duration {
            Duration::ZERO
        }
        fn keepalives_idle(&self) -> Duration {
            Duration::from_secs(10)
        }
        fn keepalives_interval(&self) -> Duration {
            Duration::from_secs(5)
        }
        fn keepalives_retries(&self) -> u32 {
            5
        }
        fn statement_timeout(&self) -> Duration {
            Duration::ZERO
        }
    }

    fn consensus_config(url: &str) -> ConsensusConfig {
        ConsensusConfig::try_from(
            &SensitiveUrl::from_str(url).expect("valid url"),
            Box::new(TestConsensusKnobs),
            PostgresClientMetrics::new(&MetricsRegistry::new(), "mz_persist"),
            Arc::new(ConfigSet::default()),
        )
        .expect("valid config")
    }

    #[mz_ore::test]
    fn consensus_config_try_from_enables_encryption() {
        let config = consensus_config(
            "postgres://host:5432/db?sslmode=require&kms_key_id=alias%2Fpersist_key_x&kms_region=us-east-1&dek_rotation_interval_secs=1",
        );
        let ConsensusConfig::Postgres(pg, Some(encryption)) = config else {
            panic!("expected Postgres config with encryption enabled");
        };
        assert_eq!(encryption.kms_key_id, "alias/persist_key_x");
        assert_eq!(encryption.kms_region.as_deref(), Some("us-east-1"));
        // A below-minimum rotation interval is clamped.
        assert_eq!(
            encryption.dek_rotation_interval,
            Duration::from_secs(MIN_DEK_ROTATION_SECS)
        );
        // The KMS params are stripped from the URL handed to the Postgres
        // driver, while other params survive.
        let pg_params: Vec<(String, String)> = pg
            .url()
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        assert_eq!(
            pg_params,
            vec![("sslmode".to_string(), "require".to_string())]
        );
    }

    #[mz_ore::test]
    fn consensus_config_try_from_no_kms_params_no_encryption() {
        let config = consensus_config("postgres://host/db?sslmode=require");
        let ConsensusConfig::Postgres(pg, encryption) = config else {
            panic!("expected Postgres config");
        };
        assert!(encryption.is_none());
        // The URL is passed through untouched.
        assert_eq!(pg.url().as_str(), "postgres://host/db?sslmode=require");
    }

    #[mz_ore::test]
    fn strip_kms_query_params_removes_only_kms_params() {
        let url = SensitiveUrl::from_str(
            "postgres://host:5432/db?sslmode=require&kms_key_id=alias%2Fpersist_key_x&kms_region=us-east-1&dek_rotation_interval_secs=300&customer_kms_key_id=k&customer_kms_region=r&customer_kms_endpoint=e&customer_kms_role_arn=a&options=--search_path%3Dconsensus",
        )
        .expect("valid url");
        let stripped = strip_kms_query_params(&url);
        let params: Vec<(String, String)> = stripped
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        assert_eq!(
            params,
            vec![
                ("sslmode".to_string(), "require".to_string()),
                ("options".to_string(), "--search_path=consensus".to_string()),
            ]
        );

        // A URL without KMS params is unchanged.
        let url = SensitiveUrl::from_str("postgres://host/db?sslmode=require").expect("valid url");
        assert_eq!(strip_kms_query_params(&url).to_string(), url.to_string());

        // All params stripped leaves no query string.
        let url = SensitiveUrl::from_str("s3://bucket/prefix?kms_key_id=k").expect("valid url");
        assert_eq!(strip_kms_query_params(&url).0.query(), None);
    }

    #[mz_ore::test]
    fn strip_kms_query_params_preserves_raw_encoding() {
        // Surviving params must not be decoded and re-encoded. The metadata
        // backend URL carries a strictly percent-encoded PEM in
        // sslrootcert_inline; form-re-encoding would turn its spaces into `+`,
        // which libpq-style parsers keep literal, corrupting the PEM.
        let pem_encoded =
            "-----BEGIN%20CERTIFICATE-----%0AMIIB%2Bw%3D%3D%0A-----END%20CERTIFICATE-----";
        let url = SensitiveUrl::from_str(&format!(
            "postgres://host:5432/db?sslmode=verify-full&sslrootcert_inline={pem_encoded}&kms_key_id=k&kms_region=r",
        ))
        .expect("valid url");
        let stripped = strip_kms_query_params(&url);
        assert_eq!(
            stripped.0.query(),
            Some(format!("sslmode=verify-full&sslrootcert_inline={pem_encoded}").as_str())
        );
    }
}
