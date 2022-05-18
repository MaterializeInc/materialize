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
use crate::sqlite::SqliteConsensus;

/// Config for an implementation of [BlobMulti].
#[derive(Debug)]
pub enum BlobMultiConfig {
    /// Config for [FileBlobMulti].
    File(FileBlobConfig),
    /// Config for [S3BlobMulti].
    S3(S3BlobConfig),
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
#[derive(Debug)]
pub enum ConsensusConfig {
    /// Config for [SqliteConsensus].
    Sqlite(String),
    /// Config for [PostgresConsensus].
    Postgres(PostgresConsensusConfig),
}

impl ConsensusConfig {
    /// Opens the associated implementation of [Consensus].
    pub async fn open(self) -> Result<Arc<dyn Consensus + Send + Sync>, ExternalError> {
        match self {
            ConsensusConfig::Sqlite(config) => SqliteConsensus::open(config)
                .await
                .map(|x| Arc::new(x) as Arc<dyn Consensus + Send + Sync>),
            ConsensusConfig::Postgres(config) => PostgresConsensus::open(config)
                .await
                .map(|x| Arc::new(x) as Arc<dyn Consensus + Send + Sync>),
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
        let query_params = url.query_pairs().collect::<HashMap<_, _>>();

        let config = match url.scheme() {
            "sqlite" => {
                if !query_params.is_empty() {
                    return Err(ExternalError::from(anyhow!(
                        "unknown consensus location params {}: {}",
                        query_params
                            .keys()
                            .map(|x| x.to_owned())
                            .collect::<Vec<_>>()
                            .join(" "),
                        url.as_str(),
                    )));
                }
                Ok(ConsensusConfig::Sqlite(url.path().to_owned()))
            }
            "postgres" | "postgresql" => Ok(ConsensusConfig::Postgres(
                PostgresConsensusConfig::new(value).await?,
            )),
            p => Err(anyhow!(
                "unknown persist consensus scheme {}: {}",
                p,
                url.as_str()
            )),
        }?;
        Ok(config)
    }
}
