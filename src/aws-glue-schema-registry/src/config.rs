// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Builder for [`Client`].
//!
//! The Glue Schema Registry client is configured from an
//! [`aws_types::SdkConfig`]. In Materialize, this `SdkConfig` is produced by
//! `AwsConnection::load_sdk_config`, which threads the region, endpoint
//! override (used in tests/LocalStack), and credential provider derived from
//! the user's `AWS CONNECTION`. The Glue client does not need any
//! Materialize-specific config beyond what `SdkConfig` already carries —
//! unlike CCSR there is no separate URL or basic-auth.

use aws_types::SdkConfig;

use crate::client::Client;

/// Builder for [`Client`].
#[derive(Debug, Clone)]
pub struct ClientConfig {
    sdk_config: SdkConfig,
}

impl ClientConfig {
    /// Construct a new client config from an AWS SDK config.
    pub fn new(sdk_config: SdkConfig) -> ClientConfig {
        ClientConfig { sdk_config }
    }

    /// Build the client. Infallible — the underlying `aws_sdk_glue::Client`
    /// constructor validates lazily on first request.
    pub fn build(self) -> Client {
        Client::from_sdk_config(self.sdk_config)
    }
}
