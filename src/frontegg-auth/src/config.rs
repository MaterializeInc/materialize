// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use once_cell::sync::Lazy;
use reqwest::Url;

use crate::{app_password::AppPassword, client::Client};

pub static DEFAULT_ENDPOINT: Lazy<Url> = Lazy::new(|| {
    "https://admin.cloud.materialize.com"
        .parse()
        .expect("url known to be valid")
});

/// Configures the required parameters of a [`Client`].
pub struct ClientConfig {
    pub(crate) app_password: AppPassword,
}

/// A builder for a [`Client`].
pub struct ClientBuilder {
    endpoint: Url,
}

impl Default for ClientBuilder {
    fn default() -> ClientBuilder {
        ClientBuilder {
            endpoint: DEFAULT_ENDPOINT.clone(),
        }
    }
}

impl ClientBuilder {
    /// Overrides the default endpoint.
    pub fn endpoint(mut self, endpoint: Url) -> ClientBuilder {
        self.endpoint = endpoint;
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
            inner,
            app_password: config.app_password,
            endpoint: self.endpoint,
            auth: Default::default(),
        }
    }
}
