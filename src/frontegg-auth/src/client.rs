// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;

pub mod tokens;

/// Client for Frontegg auth requests.
///
/// Internally the client will attempt to de-dupe requests, e.g. if a single user tries to connect
/// many clients at once, we'll de-dupe the authentication requests.
#[derive(Clone, Debug)]
pub struct Client {
    pub client: reqwest_middleware::ClientWithMiddleware,
}

impl Default for Client {
    fn default() -> Self {
        // Re-use the envd defaults until there's a reason to use something else. This is a separate
        // function so it's clear that envd can always set its own policies and nothing should
        // change them, but also we probably want to re-use them for now.
        Self::environmentd_default()
    }
}

impl Client {
    /// The environmentd Client. Do not change these without a review from the surfaces team.
    pub fn environmentd_default() -> Self {
        let retry_policy = ExponentialBackoff::builder()
            .retry_bounds(Duration::from_millis(200), Duration::from_secs(2))
            .build_with_total_retry_duration(Duration::from_secs(30));

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("must build Client");
        let client = reqwest_middleware::ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        Self { client }
    }
}
