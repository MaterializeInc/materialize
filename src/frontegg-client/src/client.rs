// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_frontegg_auth::AppPassword;
use reqwest::{Method, RequestBuilder};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use url::Url;

use crate::{
    config::{ClientBuilder, ClientConfig},
    error::{ApiError, ErrorExtended},
};

const AUTH_PATH: [&str; 5] = ["identity", "resources", "auth", "v1", "api-token"];
const REFRESH_AUTH_PATH: [&str; 7] = [
    "identity",
    "resources",
    "auth",
    "v1",
    "api-token",
    "token",
    "refresh",
];

pub mod app_password;
pub mod user;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuthenticationResponse {
    access_token: String,
    expires: String,
    expires_in: i64,
    refresh_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuthenticationRequest<'a> {
    client_id: &'a str,
    secret: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RefreshRequest<'a> {
    refresh_token: &'a str,
}

#[derive(Debug, Clone)]
pub struct Auth {
    token: String,
    /// Refresh at indicates the time at which the token should be refreshed.
    /// It equals the expiring time / 2
    refresh_at: SystemTime,
    refresh_token: String,
}

/// An API client for Frontegg.
///
/// The API client is designed to be wrapped in an [`Arc`] and used from
/// multiple threads simultaneously. A successful authentication response is
/// shared by all threads.
///
/// [`Arc`]: std::sync::Arc
pub struct Client {
    pub inner: reqwest::Client,
    pub app_password: AppPassword,
    pub endpoint: Url,
    pub auth: Mutex<Option<Auth>>,
}

impl Client {
    /// Creates a new `Client` from its required configuration parameters.
    pub fn new(config: ClientConfig) -> Client {
        ClientBuilder::default().build(config)
    }

    /// Creates a builder for a `Client` that allows for customization of
    /// optional parameters.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Builds a request towards the `Client`'s endpoint
    fn build_request<P>(&self, method: Method, path: P) -> RequestBuilder
    where
        P: IntoIterator,
        P::Item: AsRef<str>,
    {
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .expect("builder validated URL can be a base")
            .clear()
            .extend(path);
        self.inner.request(method, url)
    }

    /// Sends a requests and adds the authorization bearer token.
    async fn send_request<T>(&self, req: RequestBuilder) -> Result<T, ErrorExtended>
    where
        T: DeserializeOwned,
    {
        let token = self.auth().await?;
        let req = req.bearer_auth(token);
        self.send_unauthenticated_request(req).await
    }

    async fn send_unauthenticated_request<T>(&self, req: RequestBuilder) -> Result<T, ErrorExtended>
    where
        T: DeserializeOwned,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ErrorResponse {
            #[serde(default)]
            message: Option<String>,
            #[serde(default)]
            errors: Vec<String>,
        }

        let res = req.send().await?;
        let status_code = res.status();
        if status_code.is_success() {
            Ok(res.json().await?)
        } else {
            match res.json::<ErrorResponse>().await {
                Ok(e) => {
                    let mut messages = e.errors;
                    messages.extend(e.message);
                    Err(ErrorExtended::Api(ApiError {
                        status_code,
                        messages,
                    }))
                }
                Err(_) => Err(ErrorExtended::Api(ApiError {
                    status_code,
                    messages: vec!["unable to decode error details".into()],
                })),
            }
        }
    }

    /// Authenticates with the server, if not already authenticated,
    /// and returns the authentication token.
    pub async fn auth(&self) -> Result<String, ErrorExtended> {
        let mut auth = self.auth.lock().await;
        let mut req;

        match &*auth {
            Some(auth) => {
                if SystemTime::now() < auth.refresh_at {
                    return Ok(auth.token.clone());
                } else {
                    // Auth is available in the client but needs a refresh request.
                    req = self.build_request(Method::POST, REFRESH_AUTH_PATH);
                    let refresh_request = RefreshRequest {
                        refresh_token: auth.refresh_token.as_str(),
                    };
                    req = req.json(&refresh_request);
                }
            }
            _ => {
                // No auth available in the client, request a new one.
                req = self.build_request(Method::POST, AUTH_PATH);
                let authentication_request = AuthenticationRequest {
                    client_id: &self.app_password.client_id.to_string(),
                    secret: &self.app_password.secret_key.to_string(),
                };
                req = req.json(&authentication_request);
            }
        }

        // Do the request
        let res: AuthenticationResponse = self.send_unauthenticated_request(req).await?;
        *auth = Some(Auth {
            token: res.access_token.clone(),
            // Refresh twice as frequently as we need to, to be safe.
            refresh_at: SystemTime::now()
                + (Duration::from_secs(res.expires_in.try_into().unwrap()) / 2),
            refresh_token: res.refresh_token,
        });
        Ok(res.access_token)
    }
}
