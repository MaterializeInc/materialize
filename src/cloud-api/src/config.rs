// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module defines the configuration and builder structs for the Materialize
//! cloud [`Client`].
//!
//! The main type exported from this module is the [`ClientBuilder`] struct,
//! which is used to configure and build instances of the [`Client`] struct. The
//! [`Client`] struct provides methods for interacting with various admin APIs,
//! such as creating and managing users, or listing passwords.
//!
use std::time::Duration;

use once_cell::sync::Lazy;
use url::Url;

use crate::client::Client;

/// The default endpoint the client will use to issue the requests.
pub static DEFAULT_ENDPOINT: Lazy<Url> =
    Lazy::new(|| "https://cloud.materialize.com".parse().unwrap());

/// Configures the required parameters of a [`Client`].
pub struct ClientConfig {
    /// The authorization client to get the authorization token.
    pub(crate) auth_client: mz_frontegg_client::client::Client,
}

/// A builder for a [`Client`].
pub struct ClientBuilder {
    /// Endpoint to issue the requests from the client.
    endpoint: Url,
}

impl Default for ClientBuilder {
    /// Returns a [`ClientBuilder`] using the default endpoint.
    fn default() -> ClientBuilder {
        ClientBuilder {
            endpoint: DEFAULT_ENDPOINT.clone(),
        }
    }
}

impl ClientBuilder {
    /// Overrides the default endpoint.
    pub fn endpoint(mut self, url: Url) -> ClientBuilder {
        self.endpoint = url;
        self
    }

    /// Creates a [`Client`] that incorporates the optional parameters
    /// configured on the builder and the specified required parameters.
    pub fn build(self, config: ClientConfig) -> Client {
        let inner = reqwest::ClientBuilder::new()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap();

        Client {
            auth_client: config.auth_client,
            endpoint: self.endpoint,
            inner,
        }
    }
}
