// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Configuration for [crate::location] implementations.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use url::Url;

use crate::file::{FileBlobConfig, FileBlobMulti};
use crate::location::{BlobMulti, Consensus, ExternalError};
use crate::postgres::{PostgresConsensus, PostgresConsensusConfig};
use crate::s3::{S3BlobConfig, S3BlobMulti};

#[cfg(any(test, debug_assertions))]
use crate::mem::{MemBlobMulti, MemBlobMultiConfig, MemConsensus};

/// Config for an implementation of [BlobMulti].
#[derive(Debug, Clone)]
pub enum BlobMultiConfig {
    /// Config for [FileBlobMulti].
    File(FileBlobConfig),
    /// Config for [S3BlobMulti].
    S3(S3BlobConfig),
    /// Config for [MemBlobMulti], only available in testing to prevent
    /// footguns.
    #[cfg(any(test, debug_assertions))]
    Mem,
}

impl BlobMultiConfig {
    /// Opens the associated implementation of [BlobMulti].
    pub async fn open(self) -> Result<Arc<dyn BlobMulti + Send + Sync>, ExternalError> {
        match self {
            BlobMultiConfig::File(config) => FileBlobMulti::open(config)
                .await
                .map(|x| Arc::new(x) as Arc<dyn BlobMulti + Send + Sync>),
            BlobMultiConfig::S3(config) => S3BlobMulti::open(config)
                .await
                .map(|x| Arc::new(x) as Arc<dyn BlobMulti + Send + Sync>),
            #[cfg(any(test, debug_assertions))]
            BlobMultiConfig::Mem => Ok(Arc::new(MemBlobMulti::open(MemBlobMultiConfig::default()))
                as Arc<dyn BlobMulti + Send + Sync>),
        }
    }

    /// Parses a [BlobMulti] config from a uri string.
    pub async fn try_from(value: &str) -> Result<Self, ExternalError> {
        let url = Url::parse(value)
            .map_err(|err| anyhow!("failed to parse blob location {} as a url: {}", &value, err))?;
        let mut query_params = url.query_pairs().collect::<HashMap<_, _>>();

        let config = match url.scheme() {
            "file" => {
                let config = FileBlobConfig::from(url.path());
                Ok(BlobMultiConfig::File(config))
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
                let role_arn = query_params.remove("aws_role_arn").map(|x| x.into_owned());
                let config = S3BlobConfig::new(bucket, prefix, role_arn).await?;
                Ok(BlobMultiConfig::S3(config))
            }
            #[cfg(any(test, debug_assertions))]
            "mem" => {
                query_params.clear();
                Ok(BlobMultiConfig::Mem)
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
                    .map(|x| x.to_owned())
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
    #[cfg(any(test, debug_assertions))]
    Mem,
}

impl ConsensusConfig {
    /// Opens the associated implementation of [Consensus].
    pub async fn open(self) -> Result<Arc<dyn Consensus + Send + Sync>, ExternalError> {
        match self {
            ConsensusConfig::Postgres(config) => PostgresConsensus::open(config)
                .await
                .map(|x| Arc::new(x) as Arc<dyn Consensus + Send + Sync>),
            #[cfg(any(test, debug_assertions))]
            ConsensusConfig::Mem => {
                Ok(Arc::new(MemConsensus::default()) as Arc<dyn Consensus + Send + Sync>)
            }
        }
    }

    /// Parses a [Consensus] config from a uri string.
    pub async fn try_from(value: &str) -> Result<Self, ExternalError> {
        let url = Url::parse(value).map_err(|err| {
            anyhow!(
                "failed to parse consensus location {} as a url: {}",
                &value,
                err
            )
        })?;

        let config = match url.scheme() {
            "postgres" | "postgresql" => Ok(ConsensusConfig::Postgres(
                PostgresConsensusConfig::new(value).await?,
            )),
            #[cfg(any(test, debug_assertions))]
            "mem" => Ok(ConsensusConfig::Mem),
            p => Err(anyhow!(
                "unknown persist consensus scheme {}: {}",
                p,
                url.as_str()
            )),
        }?;
        Ok(config)
    }
}
