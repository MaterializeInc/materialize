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
use rusoto_core::{HttpClient, Region};
use rusoto_credential::{
    AutoRefreshingProvider, ChainProvider, ProvideAwsCredentials, StaticProvider,
};
use rusoto_s3::S3Client;

/// Construct an S3Client
///
/// If statically provided connection information information is not provided,
/// falls back to using credentials gathered by rusoto's [`ChainProvider]
/// wrapped in an [`AutoRefreshingProvider`].
///
/// The [`AutoRefreshingProvider`] caches the underlying provider's AWS credentials,
/// automatically fetching updated credentials if they've expired.
pub async fn client(
    region: Region,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    token: Option<String>,
) -> Result<S3Client, anyhow::Error> {
    let request_dispatcher = HttpClient::new().context("creating HTTP client for S3 client")?;
    let s3_client = match (access_key_id, secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => {
            info!("Creating a new S3 client from provided access_key and secret_access_key");
            S3Client::new_with(
                request_dispatcher,
                StaticProvider::new(access_key_id, secret_access_key, token, None),
                region,
            )
        }
        (None, None) => {
            info!(
                "AWS access_key_id and secret_access_key not provided, \
                 creating a new S3 client using a chain provider."
            );
            let mut provider = ChainProvider::new();
            provider.set_timeout(Duration::from_secs(10));
            provider.credentials().await?; // ensure that credentials exist
            let provider =
                AutoRefreshingProvider::new(provider).context("generating AWS credentials")?;

            S3Client::new_with(request_dispatcher, provider, region)
        }
        (_, _) => anyhow::bail!(
            "access_key_id and secret_access_key must either both be provided, or neither"
        ),
    };
    Ok(s3_client)
}
