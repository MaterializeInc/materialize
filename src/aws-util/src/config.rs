// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS configuration.

use aws_config::default_provider::{credentials, region};
use aws_config::meta::region::ProvideRegion;
use aws_smithy_http::endpoint::Endpoint;
use aws_types::credentials::{ProvideCredentials, SharedCredentialsProvider};
use aws_types::region::Region;

/// Service agnostic configuration for AWS.
#[derive(Debug, Clone)]
pub struct AwsConfig {
    region: Option<Region>,
    credentials_provider: SharedCredentialsProvider,
    endpoint: Option<Endpoint>,
}

impl AwsConfig {
    /// Creates a configuration using the specified credentials provider.
    pub fn with_credentials_provider<P>(provider: P) -> AwsConfig
    where
        P: ProvideCredentials + 'static,
    {
        AwsConfig {
            region: None,
            credentials_provider: SharedCredentialsProvider::new(provider),
            endpoint: None,
        }
    }

    /// Loads the default configuration from the environment.
    pub async fn load_from_env() -> AwsConfig {
        let region = region::default_provider().region().await;
        let credentials_provider = {
            let mut builder = credentials::DefaultCredentialsChain::builder();
            builder.set_region(region.clone());
            builder.build().await
        };
        let mut config = AwsConfig::with_credentials_provider(credentials_provider);
        config.region = region;
        config
    }

    /// Sets the endpoint.
    pub fn set_endpoint(&mut self, endpoint: Endpoint) {
        self.endpoint = Some(endpoint);
    }

    /// Returns the currently configured endpoint, if any.
    pub fn endpoint(&self) -> Option<&Endpoint> {
        self.endpoint.as_ref()
    }

    /// Sets the region.
    pub fn set_region(&mut self, region: Region) {
        self.region = Some(region);
    }

    /// Returns the currently configured region, if any.
    pub fn region(&self) -> Option<&Region> {
        self.region.as_ref()
    }

    /// Sets the credentials provider.
    pub fn set_credentials_provider(&mut self, credentials_provider: SharedCredentialsProvider) {
        self.credentials_provider = credentials_provider;
    }

    /// Returns the currently configured credentials provider, if any.
    pub fn credentials_provider(&self) -> &SharedCredentialsProvider {
        &self.credentials_provider
    }
}
