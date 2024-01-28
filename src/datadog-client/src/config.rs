// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module defines the configuration and builder structs for
//! the Datadog [`Client`].
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
    Lazy::new(|| "https://api.datadoghq.com/api".parse().unwrap());

/// Configures the required parameters of a [`Client`].
pub struct ClientConfig<'a> {
    /// API key to issue requests from the client.
    pub api_key: &'a str,
}

/// A builder for a [`Client`].
pub struct ClientBuilder {}

impl Default for ClientBuilder {
    /// Returns a [`ClientBuilder`] using the default endpoint.
    fn default() -> ClientBuilder {
        ClientBuilder {}
    }
}

impl ClientBuilder {
    /// Creates a [`Client`] that incorporates the optional parameters
    /// configured on the builder and the specified required parameters.
    pub fn build(self, config: ClientConfig) -> Client {
        let inner = reqwest::ClientBuilder::new()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap();

        Client {
            api_key: config.api_key,
            inner,
        }
    }
}
