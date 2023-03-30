// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use reqwest::{Method, RequestBuilder};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use url::Url;

use crate::{
    app_password::AppPassword,
    config::{ClientBuilder, ClientConfig},
    error::{ApiError, FronteggError},
};

const AUTH_PATH: [&str; 5] = ["identity", "resources", "auth", "v1", "api-token"];
const USERS_PATH: [&str; 4] = ["identity", "resources", "users", "v3"];
const CREATE_USERS_PATH: [&str; 5] = ["frontegg", "identity", "resources", "users", "v2"];
const APP_PASSWORDS_PATH: [&str; 5] = ["identity", "resources", "users", "api-tokens", "v1"];
const CREATE_APP_PASSWORDS_PATH: [&str; 6] = [
    "frontegg",
    "identity",
    "resources",
    "users",
    "api-tokens",
    "v1",
];

pub mod app_password;
pub mod user;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationResponse {
    pub access_token: String,
    pub expires_in: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationRequest<'a> {
    pub client_id: &'a str,
    pub secret: &'a str,
}

#[derive(Debug, Clone)]
pub struct Auth {
    pub token: String,
    pub refresh_at: SystemTime,
}

/// An API client for Frontegg.
///
/// The API client is designed to be wrapped in an [`Arc`] and used from
/// multiple threads simultaneously. A successful authentication response is
/// shared by all threads.
///
/// [`Arc`]: std::sync::Arc
#[derive(Debug)]
pub struct Client {
    pub(crate) inner: reqwest::Client,
    pub(crate) app_password: AppPassword,
    pub(crate) endpoint: Url,
    pub(crate) auth: Mutex<Option<Auth>>,
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
    async fn send_request<T>(&self, req: RequestBuilder) -> Result<T, FronteggError>
    where
        T: DeserializeOwned,
    {
        let token = self.auth().await?.token;
        let req = req.bearer_auth(token);
        self.send_unauthenticated_request(req).await
    }

    async fn send_unauthenticated_request<T>(&self, req: RequestBuilder) -> Result<T, FronteggError>
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
                    Err(FronteggError::Api(ApiError {
                        status_code,
                        messages,
                    }))
                }
                Err(_) => Err(FronteggError::Api(ApiError  {
                    status_code,
                    messages: vec!["unable to decode error details".into()],
                })),
            }
        }
    }

    /// Authenticates with the server, if not already authenticated,
    /// and returns the authentication token.
    pub async fn auth(&self) -> Result<Auth, FronteggError> {
        let mut auth = self.auth.lock().await;
        match &*auth {
            Some(auth) if SystemTime::now() < auth.refresh_at => {
                return Ok(auth.clone());
            }
            _ => (),
        }
        let req = self.build_request(Method::POST, AUTH_PATH);
        let authentication_request = AuthenticationRequest {
            client_id: &self.app_password.client_id.to_string(),
            secret: &self.app_password.secret_key.to_string(),
        };
        let req = req.json(&authentication_request);
        let res: AuthenticationResponse = self.send_unauthenticated_request(req).await?;
        *auth = Some(Auth {
            token: res.access_token.clone(),
            // Refresh twice as frequently as we need to, to be safe.
            refresh_at: SystemTime::now() + (Duration::from_secs(res.expires_in) / 2),
        });
        Ok(auth.clone().unwrap())
    }
}
