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
use rusoto_core::HttpClient;
use rusoto_credential::{AutoRefreshingProvider, AwsCredentials, ChainProvider, StaticProvider};
use rusoto_sqs::SqsClient;

use crate::aws::ConnectInfo;

/// Construct an SqsClient
///
/// If statically provided connection information information is not provided,
/// falls back to using credentials gathered by rusoto's [`ChainProvider`]
/// wrapped in an [`AutoRefreshingProvider`].
///
/// The [`AutoRefreshingProvider`] caches the underlying provider's AWS credentials,
/// automatically fetching updated credentials if they've expired.
pub async fn client(conn_info: ConnectInfo) -> Result<SqsClient, anyhow::Error> {
    let request_dispatcher = HttpClient::new().context("creating HTTP client for SQS client")?;
    let s3_client = if let Some(creds) = conn_info.credentials {
        info!("Creating a new SQS client from provided access_key and secret_access_key");
        let provider = StaticProvider::from(AwsCredentials::from(creds));

        SqsClient::new_with(request_dispatcher, provider, conn_info.region)
    } else {
        info!(
            "AWS access_key_id and secret_access_key not provided, \
               creating a new SQS client using a chain provider."
        );
        let mut provider = ChainProvider::new();
        provider.set_timeout(Duration::from_secs(10));
        let provider =
            AutoRefreshingProvider::new(provider).context("generating AWS credentials")?;

        SqsClient::new_with(request_dispatcher, provider, conn_info.region)
    };
    Ok(s3_client)
}
