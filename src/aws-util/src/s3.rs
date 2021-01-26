// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utility functions for AWS S3 clients

use std::time::Duration;

use anyhow::Context;
use log::info;
use rusoto_credential::{AutoRefreshingProvider, AwsCredentials, ChainProvider, StaticProvider};
use rusoto_s3::S3Client;

use crate::aws::{https_client, ConnectInfo, DEFAULT_AWS_TIMEOUT};

/// Construct an S3Client
///
/// If statically provided connection information information is not provided,
/// falls back to using credentials gathered by rusoto's [`ChainProvider`]
/// wrapped in an [`AutoRefreshingProvider`].
///
/// The [`AutoRefreshingProvider`] caches the underlying provider's AWS credentials,
/// automatically fetching updated credentials if they've expired.
pub async fn client(conn_info: ConnectInfo) -> Result<S3Client, anyhow::Error> {
    let s3_client = if let Some(creds) = conn_info.credentials {
        info!("Creating a new S3 client from provided access_key and secret_access_key");
        let provider = StaticProvider::from(AwsCredentials::from(creds));
        crate::aws::account(
            provider.clone(),
            conn_info.region.clone(),
            DEFAULT_AWS_TIMEOUT,
        )
        .await?;

        S3Client::new_with(https_client(), provider, conn_info.region)
    } else {
        info!(
            "AWS access_key_id and secret_access_key not provided, \
               creating a new S3 client using a chain provider."
        );
        let mut provider = ChainProvider::new();
        provider.set_timeout(Duration::from_secs(10));
        let provider =
            AutoRefreshingProvider::new(provider).context("generating AWS credentials")?;
        crate::aws::account(
            provider.clone(),
            conn_info.region.clone(),
            DEFAULT_AWS_TIMEOUT,
        )
        .await?;

        S3Client::new_with(https_client(), provider, conn_info.region)
    };
    Ok(s3_client)
}
