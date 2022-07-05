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
use tracing::warn;
use url::Url;

use crate::file::{FileBlob, FileBlobConfig};
use crate::location::{Blob, Consensus, ExternalError};
use crate::mem::{MemBlob, MemBlobConfig, MemConsensus};
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

impl BlobConfig {
    /// Opens the associated implementation of [Blob].
    pub async fn open(self) -> Result<Arc<dyn Blob + Send + Sync>, ExternalError> {
        match self {
            BlobConfig::File(config) => FileBlob::open(config)
                .await
                .map(|x| Arc::new(x) as Arc<dyn Blob + Send + Sync>),
            BlobConfig::S3(config) => S3Blob::open(config)
                .await
                .map(|x| Arc::new(x) as Arc<dyn Blob + Send + Sync>),
            BlobConfig::Mem => {
                Ok(Arc::new(MemBlob::open(MemBlobConfig::default()))
                    as Arc<dyn Blob + Send + Sync>)
            }
        }
    }

    /// Parses a [Blob] config from a uri string.
    pub async fn try_from(value: &str) -> Result<Self, ExternalError> {
        let url = Url::parse(value)
            .map_err(|err| anyhow!("failed to parse blob location {} as a url: {}", &value, err))?;
        let mut query_params = url.query_pairs().collect::<HashMap<_, _>>();

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
                let role_arn = query_params.remove("aws_role_arn").map(|x| x.into_owned());
                let config = S3BlobConfig::new(bucket, prefix, role_arn).await?;
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
    Mem,
}

impl ConsensusConfig {
    /// Opens the associated implementation of [Consensus].
    pub async fn open(self) -> Result<Arc<dyn Consensus + Send + Sync>, ExternalError> {
        match self {
            ConsensusConfig::Postgres(config) => PostgresConsensus::open(config)
                .await
                .map(|x| Arc::new(x) as Arc<dyn Consensus + Send + Sync>),
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
