// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::time::Duration;

use mz_ore::retry::Retry;
use tracing::warn;

use crate::Error;

pub mod tokens;

#[derive(Clone, Debug)]
pub struct Client {
    pub client: reqwest::Client,
    pub retry_backoff: Duration,
    pub max_retry: Duration,
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
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("must build Client"),
            retry_backoff: Duration::from_secs(1),
            max_retry: Duration::from_secs(30),
        }
    }

    async fn request<T, F, U>(&self, f: F) -> Result<T, Error>
    where
        F: Copy + Fn(reqwest::Client) -> U,
        U: Future<Output = Result<T, Error>>,
    {
        Retry::default()
            .clamp_backoff(self.retry_backoff.clone())
            .max_duration(self.max_retry.clone())
            .retry_async_canceling(|state| async move {
                // Return a Result<Result<T, Error>, Error> so we can
                // differentiate a retryable or not error.
                match f(self.client.clone()).await {
                    Err(Error::ReqwestError(err)) if err.is_timeout() => {
                        warn!("frontegg timeout, attempt {}: {}", state.i, err);
                        Err(Error::from(err))
                    }
                    v => Ok(v),
                }
            })
            .await?
    }
}
