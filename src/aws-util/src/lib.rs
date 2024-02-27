// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_config::{BehaviorVersion, ConfigLoader};
use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;
use aws_smithy_runtime_api::client::http::HttpClient;
use hyper_tls::HttpsConnector;

#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "s3")]
pub mod s3_uploader;

/// Creates an AWS SDK configuration loader with the defaults for the latest
/// behavior version plus some Materialize-specific overrides.
pub fn defaults() -> ConfigLoader {
    // Use the SDK's latest behavior version. We already pin the crate versions,
    // and CI puts version upgrades through rigorous testing, so we're happy to
    // take the latest behavior version. We can adjust this in the future as
    // necessary, if the AWS SDK ships a new behavior version that causes
    // trouble.
    let behavior_version = BehaviorVersion::latest();

    // This is the only method allowed to call `aws_config::defaults`.
    #[allow(clippy::disallowed_methods)]
    let loader = aws_config::defaults(behavior_version);

    // Install our custom HTTP client.
    let loader = loader.http_client(http_client());

    loader
}

/// Returns an HTTP client for use with the AWS SDK that is appropriately
/// configured for Materialize.
pub fn http_client() -> impl HttpClient {
    // The default AWS HTTP client uses rustls, while our company policy is to
    // use native TLS.
    HyperClientBuilder::new().build(HttpsConnector::new())
}
