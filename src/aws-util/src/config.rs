// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS configuration.

use std::sync::Arc;

use aws_config::{Config, ConfigLoader};
use aws_smithy_http::endpoint::Endpoint;
use aws_types::credentials::{Credentials, CredentialsError, ProvideCredentials};
use aws_types::region::Region;

/// Service agnostic configuration for AWS.
///
/// This wraps the upstream [`Config`] type to allow additionally configuring
/// a global endpoint (e.g., LocalStack) to be used for all services.
#[derive(Debug, Clone)]
pub struct AwsConfig {
    endpoint: Option<Endpoint>,
    // TODO(benesch): drop the `Arc` if awslabs/aws-sdk-rust#352 is resolved.
    inner: Arc<Config>,
}

impl AwsConfig {
    /// Creates a configuration from a [`ConfigLoader`].
    pub async fn from_loader(loader: ConfigLoader) -> AwsConfig {
        AwsConfig {
            endpoint: None,
            inner: Arc::new(loader.load().await),
        }
    }

    /// Loads the default configuration from the environment.
    pub async fn load_from_env() -> AwsConfig {
        AwsConfig::from_loader(aws_config::from_env()).await
    }

    /// Returns the inner [`Config`] object.
    pub fn inner(&self) -> &Config {
        &self.inner
    }

    /// Returns the currently configured endpoint, if any.
    ///
    /// This is a convenience method for `config.inner().region()`.
    pub fn region(&self) -> Option<&Region> {
        self.inner.region()
    }

    /// Sets the endpoint.
    pub fn set_endpoint(&mut self, endpoint: Endpoint) {
        self.endpoint = Some(endpoint);
    }

    /// Returns the currently configured endpoint, if any.
    pub fn endpoint(&self) -> Option<&Endpoint> {
        self.endpoint.as_ref()
    }

    /// Acquires credentials using the configured credentials provider.
    pub async fn provide_credentials(&self) -> Result<Credentials, CredentialsError> {
        match self.inner().credentials_provider() {
            None => Err(CredentialsError::not_loaded(
                "no credentials provider configured",
            )),
            Some(provider) => provider.provide_credentials().await,
        }
    }
}
