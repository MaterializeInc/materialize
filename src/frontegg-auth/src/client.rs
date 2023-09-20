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

use anyhow::Context;
use mz_ore::collections::HashMap;
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use tokio::sync::oneshot;

use crate::client::tokens::RefreshTokenResponse;
use crate::{ApiTokenArgs, ApiTokenResponse, Error, RefreshToken};

pub mod tokens;

/// Client for Frontegg auth requests.
///
/// Internally the client will attempt to de-dupe requests, e.g. if a single user tries to connect
/// many clients at once, we'll de-dupe the authentication requests.
#[derive(Clone, Debug)]
pub struct Client {
    pub client: reqwest_middleware::ClientWithMiddleware,
    inflight_requests: Arc<Mutex<HashMap<Request, ResponseHandle>>>,
}

type ResponseHandle = Vec<oneshot::Sender<Result<Response, Error>>>;

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

    /// Makes a request to the provided URL, possibly de-duping by attaching a listener to an
    /// already in-flight request.
    async fn make_request<Req, Resp>(&self, url: String, req: Req) -> Result<Resp, Error>
    where
        Req: AuthRequest,
        Resp: AuthResponse,
    {
        let req = req.into_request();

        // Note: we get the reciever in a block to scope the access to the mutex.
        let rx = {
            let mut inflight_requests = self
                .inflight_requests
                .lock()
                .expect("Frontegg Auth Client panicked");
            let (tx, rx) = tokio::sync::oneshot::channel();

            match inflight_requests.get_mut(&req) {
                // Already have an inflight request, add to our list of waiters.
                Some(senders) => {
                    tracing::debug!("reusing request, {req:?}");
                    senders.push(tx);
                    rx
                }
                // New request! Need to queue one up.
                None => {
                    tracing::debug!("spawning new request, {req:?}");

                    inflight_requests.insert(req.clone(), vec![tx]);

                    let client = self.client.clone();
                    let inflight = Arc::clone(&self.inflight_requests);
                    let req_ = req.clone();

                    mz_ore::task::spawn(move || "frontegg-auth-request", async move {
                        // Make the actual request.
                        let result = async {
                            let resp = client
                                .post(&url)
                                .json(&req_.into_json())
                                .send()
                                .await?
                                .error_for_status()?
                                .json::<Resp>()
                                .await?;
                            Ok::<_, Error>(resp)
                        }
                        .await;

                        // Get all of our waiters.
                        let mut inflight = inflight.lock().expect("Frontegg Auth Client panicked");
                        let Some(waiters) = inflight.remove(&req) else {
                            tracing::error!("Inflight entry already removed? {req:?}");
                            return;
                        };

                        // Tell all of our waiters about the result.
                        let response = result.map(|r| r.into_response());
                        for tx in waiters {
                            let _ = tx.send(response.clone());
                        }
                    });

                    rx
                }
            }
        };

        let resp = rx.await.context("waiting for inflight response")?;
        resp.map(|r| Resp::from_response(r))
    }
}

/// Boilerplate for de-duping requests.
///
/// We maintain an in-memory map of inflight requests, and that map needs to have keys of a single
/// type, so we wrap all of our request types an an enum to create that single type.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Request {
    ExchangeSecretForToken(ApiTokenArgs),
    RefreshToken(RefreshToken),
}

impl Request {
    fn into_json(self) -> serde_json::Value {
        match self {
            Request::ExchangeSecretForToken(arg) => serde_json::to_value(arg),
            Request::RefreshToken(arg) => serde_json::to_value(arg),
        }
        .expect("converting to JSON cannot fail")
    }
}

/// Boilerplate for de-duping requests.
///
/// Deduplicates the wrapping of request types into a [`Request`].
trait AuthRequest: serde::Serialize + Clone {
    fn into_request(self) -> Request;
}

impl AuthRequest for ApiTokenArgs {
    fn into_request(self) -> Request {
        Request::ExchangeSecretForToken(self)
    }
}

impl AuthRequest for RefreshToken {
    fn into_request(self) -> Request {
        Request::RefreshToken(self)
    }
}

/// Boilerplate for de-duping requests.
///
/// We maintain an in-memory map of inflight requests, the values of the map are a Vec of waiters
/// that listen for a response. These listeners all need to have the same type, so we wrap all of
/// our response types in an enum.
#[derive(Clone, Debug)]
enum Response {
    ExchangeSecretForToken(ApiTokenResponse),
    RefreshToken(RefreshTokenResponse),
}

/// Boilerplate for de-duping requests.
///
/// Deduplicates the wrapping and unwrapping between response types and [`Response`].
trait AuthResponse: serde::de::DeserializeOwned {
    fn into_response(self) -> Response;
    fn from_response(resp: Response) -> Self;
}

impl AuthResponse for ApiTokenResponse {
    fn into_response(self) -> Response {
        Response::ExchangeSecretForToken(self)
    }

    fn from_response(resp: Response) -> Self {
        let Response::ExchangeSecretForToken(result) = resp else {
            unreachable!("programming error!, didn't roundtrip {resp:?}")
        };
        result
    }
}

impl AuthResponse for RefreshTokenResponse {
    fn into_response(self) -> Response {
        Response::RefreshToken(self)
    }

    fn from_response(resp: Response) -> Self {
        let Response::RefreshToken(result) = resp else {
            unreachable!("programming error!, didn't roundtrip")
        };
        result
    }
}
