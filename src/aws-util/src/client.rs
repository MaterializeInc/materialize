// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS client builders
//!
//! The functions in this module all configure a client for an AWS service
//! using a uniform credentials pattern.

use std::time::Duration;

use anyhow::{anyhow, Context};
use log::info;
use rusoto_core::HttpClient;
use rusoto_credential::{AutoRefreshingProvider, AwsCredentials, ChainProvider, StaticProvider};
use rusoto_kinesis::KinesisClient;
use rusoto_s3::S3Client;
use rusoto_sqs::SqsClient;

use crate::aws::ConnectInfo;

/// Get an [`HttpClient`]  that respects the `http_proxy` environment variables
pub(crate) fn http() -> Result<HttpClient<http_util::ProxiedConnector>, anyhow::Error> {
    Ok(HttpClient::from_connector(
        http_util::connector().map_err(|e| anyhow!(e))?,
    ))
}

/// Create a function that calls <Client>::new() with credentials sources
///
/// Unfortunately there is no trait that we can rely on in Rusoto to allow us
/// to turn this into a generic function, and also we can't break most of the
/// internal complexity out into its own function because, despite the fact
/// that all clients in Rusoto box their credential providers to avoid being
/// type-parameterized, they expect a concrete type and so
/// StaticCredentials/RefreshingCredentials can't be returned from the same
/// function.
macro_rules! gen_client_builder(
    ($client:ident, $name:ident) => {
        gen_client_builder!($client, $name, stringify!($client));
    };

    ($client:ident, $name:ident, $client_name:expr) => {
#[doc = "Construct a "]
#[doc = $client_name]
#[doc = "

If statically provided connection information information is not provided,
falls back to using credentials gathered by rusoto's [`ChainProvider`]
wrapped in an [`AutoRefreshingProvider`].

The [`AutoRefreshingProvider`] caches the underlying provider's AWS credentials,
automatically fetching updated credentials if they've expired.
"]
pub async fn $name(conn_info: ConnectInfo) -> Result<$client, anyhow::Error> {
    let request_dispatcher = http().context(
        concat!("creating HTTP client for ", $client_name))?;
    let the_client = if let Some(creds) = conn_info.credentials {
        info!(concat!("Creating a new", $client_name, " from provided access_key and secret_access_key"));
        let provider = StaticProvider::from(AwsCredentials::from(creds));

        $client::new_with(request_dispatcher, provider, conn_info.region)
    } else {
        info!(
            concat!("AWS access_key_id and secret_access_key not provided, \
               creating a new ", $client_name, " using a chain provider.")
        );
        let mut provider = ChainProvider::new();

        provider.set_timeout(Duration::from_secs(10));
        let provider =
            AutoRefreshingProvider::new(provider).context(
                concat!("generating AWS credentials refreshing provider for ", $client_name))?;

        $client::new_with(request_dispatcher, provider, conn_info.region)
    };
    Ok(the_client)
}

    };
);

gen_client_builder!(S3Client, s3);
gen_client_builder!(SqsClient, sqs);
gen_client_builder!(KinesisClient, kinesis);
