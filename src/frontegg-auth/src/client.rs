// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use mz_ore::collections::HashMap;
use tokio::sync::oneshot;

use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;

use crate::{ApiTokenArgs, ApiTokenResponse, Error};

pub mod tokens;

#[derive(Clone, Debug)]
pub struct Client {
    pub client: reqwest_middleware::ClientWithMiddleware,
    inflight_requests: Arc<Mutex<HashMap<InflightRequest, InflightRequestHandle>>>,
}

type InflightRequestHandle = Vec<oneshot::Sender<InflightResponse>>;

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
            .backoff_exponent(2)
            .build_with_total_retry_duration(Duration::from_secs(30));

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("must build Client");
        let client = reqwest_middleware::ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        let inflight_requests = Arc::new(Mutex::new(HashMap::new()));

        Self {
            client,
            inflight_requests,
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
enum InflightRequest {
    SecretForToken(ApiTokenArgs),
}

#[derive(Debug)]
enum InflightResponse {
    SecretForToken(Result<ApiTokenResponse, Error>),
}
