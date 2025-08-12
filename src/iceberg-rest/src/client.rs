// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::time::SystemTime;

use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::http_request::{
    SignableBody, SignableRequest, SigningParams, SigningSettings, sign,
};
use aws_sigv4::sign::v4;
use http::StatusCode;
use iceberg::{Error, ErrorKind, Result};
use mz_ore::collections::HashSet;
use reqwest::header::HeaderMap;
use reqwest::{Client, IntoUrl, Method, Request, RequestBuilder, Response};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::catalog::RestCatalogConfig;
use crate::types::{ErrorResponse, TokenResponse};

pub(crate) struct HttpClient {
    client: Client,

    /// The token to be used for authentication.
    ///
    /// It's possible to fetch the token from the server while needed.
    token: Mutex<Option<String>>,
    /// The token endpoint to be used for authentication.
    token_endpoint: String,
    /// The credential to be used for authentication.
    credential: Option<(Option<String>, String)>,
    /// Extra headers to be added to each request.
    extra_headers: HeaderMap,
    /// Extra oauth parameters to be added to each authentication request.
    extra_oauth_params: BTreeMap<String, String>,
    /// Optional AWS Client to be used for signing requests.
    aws_config: Option<aws_types::SdkConfig>,
    /// The sigv4 signing name to be used for signing requests.
    sigv4_signing_name: Option<String>,
}

impl Debug for HttpClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient")
            .field("client", &self.client)
            .field("extra_headers", &self.extra_headers)
            .finish_non_exhaustive()
    }
}

impl HttpClient {
    /// Create a new http client.
    pub fn new(cfg: &RestCatalogConfig) -> Result<Self> {
        let extra_headers = cfg.extra_headers()?;
        Ok(HttpClient {
            client: cfg.client().unwrap_or_default(),
            token: Mutex::new(cfg.token()),
            token_endpoint: cfg.get_token_endpoint(),
            credential: cfg.credential(),
            extra_headers,
            extra_oauth_params: cfg.extra_oauth_params(),
            aws_config: cfg.aws_config(),
            sigv4_signing_name: cfg.sigv4_signing_name(),
        })
    }

    /// Update the http client with new configuration.
    ///
    /// If cfg carries new value, we will use cfg instead.
    /// Otherwise, we will keep the old value.
    pub fn update_with(self, cfg: &RestCatalogConfig) -> Result<Self> {
        let extra_headers = (!cfg.extra_headers()?.is_empty())
            .then(|| cfg.extra_headers())
            .transpose()?
            .unwrap_or(self.extra_headers);
        Ok(HttpClient {
            client: cfg.client().unwrap_or(self.client),
            token: Mutex::new(cfg.token().or_else(|| self.token.into_inner())),
            token_endpoint: if !cfg.get_token_endpoint().is_empty() {
                cfg.get_token_endpoint()
            } else {
                self.token_endpoint
            },
            credential: cfg.credential().or(self.credential),
            extra_headers,
            extra_oauth_params: if !cfg.extra_oauth_params().is_empty() {
                cfg.extra_oauth_params()
            } else {
                self.extra_oauth_params
            },
            aws_config: cfg.aws_config().or(self.aws_config),
            sigv4_signing_name: cfg.sigv4_signing_name().or(self.sigv4_signing_name),
        })
    }

    /// This API is testing only to assert the token.
    #[cfg(test)]
    pub(crate) async fn token(&self) -> Option<String> {
        let mut req = self
            .request(Method::GET, &self.token_endpoint)
            .build()
            .unwrap();
        self.authenticate(&mut req).await.ok();
        self.token.lock().await.clone()
    }

    async fn exchange_credential_for_token(&self) -> Result<String> {
        // Credential must exist here.
        let (client_id, client_secret) = self.credential.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Credential must be provided for authentication",
            )
        })?;

        let mut params = BTreeMap::new();
        params.insert("grant_type", "client_credentials");
        if let Some(client_id) = client_id {
            params.insert("client_id", client_id);
        }
        params.insert("client_secret", client_secret);
        params.extend(
            self.extra_oauth_params
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        );

        let mut auth_req = self
            .request(Method::POST, &self.token_endpoint)
            .form(&params)
            .build()?;
        // extra headers add content-type application/json header it's necessary to override it with proper type
        // note that form call doesn't add content-type header if already present
        auth_req.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let auth_url = auth_req.url().clone();
        let auth_resp = self.client.execute(auth_req).await?;

        let auth_res: TokenResponse = if auth_resp.status() == StatusCode::OK {
            let text = auth_resp
                .bytes()
                .await
                .map_err(|err| err.with_url(auth_url.clone()))?;
            Ok(serde_json::from_slice(&text).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to parse response from rest catalog server!",
                )
                .with_context("operation", "auth")
                .with_context("url", auth_url.to_string())
                .with_context("json", String::from_utf8_lossy(&text))
                .with_source(e)
            })?)
        } else {
            let code = auth_resp.status();
            let text = auth_resp
                .bytes()
                .await
                .map_err(|err| err.with_url(auth_url.clone()))?;
            let e: ErrorResponse = serde_json::from_slice(&text).map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Received unexpected response")
                    .with_context("code", code.to_string())
                    .with_context("operation", "auth")
                    .with_context("url", auth_url.to_string())
                    .with_context("json", String::from_utf8_lossy(&text))
                    .with_source(e)
            })?;
            Err(Error::from(e))
        }?;
        Ok(auth_res.access_token)
    }

    /// Invalidate the current token without generating a new one. On the next request, the client
    /// will attempt to generate a new token.
    pub(crate) async fn invalidate_token(&self) -> Result<()> {
        *self.token.lock().await = None;
        Ok(())
    }

    /// Invalidate the current token and set a new one. Generates a new token before invalidating
    /// the current token, meaning the old token will be used until this function acquires the lock
    /// and overwrites the token.
    ///
    /// If credential is invalid, or the request fails, this method will return an error and leave
    /// the current token unchanged.
    pub(crate) async fn regenerate_token(&self) -> Result<()> {
        let new_token = self.exchange_credential_for_token().await?;
        *self.token.lock().await = Some(new_token.clone());
        Ok(())
    }

    /// Authenticate the request by signing it with AWS SigV4.
    async fn authenticate_sigv4(&self, req: &mut Request) -> Result<()> {
        if let Some(aws_config) = &self.aws_config {
            let region = aws_config
                .region()
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "AWS region is not set"))?
                .as_ref();

            let credentials = aws_config
                .credentials_provider()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "AWS credentials provider is not set",
                    )
                })?
                .provide_credentials()
                .await
                .map_err(|e| {
                    Error::new(ErrorKind::DataInvalid, "Failed to provide AWS credentials")
                        .with_source(e)
                })?;

            let identity = credentials.into();

            let signing_name = self
                .sigv4_signing_name
                .as_ref()
                .map(|s| s.as_str())
                .unwrap_or("s3tables");

            let signing_params: SigningParams = v4::SigningParams::builder()
                .identity(&identity)
                .region(region)
                .name(signing_name)
                .time(SystemTime::now())
                .settings(SigningSettings::default())
                .build()
                .map_err(|e| {
                    Error::new(ErrorKind::DataInvalid, "Failed to create signing params")
                        .with_source(e)
                })?
                .into();

            let uri = req.url().clone();
            let headers: Vec<_> = req
                .headers()
                .iter()
                .map(|(k, v)| {
                    v.to_str()
                        .map(|v_str| (k.to_string(), v_str.to_string()))
                        .map_err(|e| {
                            Error::new(ErrorKind::DataInvalid, "Invalid UTF-8 in header value")
                                .with_source(e)
                        })
                })
                .collect::<Result<Vec<_>>>()?;

            let body = req.body().as_ref().and_then(|b| b.as_bytes());
            let body = match body {
                Some(body) => SignableBody::Bytes(body),
                None => SignableBody::Bytes(&[]),
            };

            let signable = SignableRequest::new(
                req.method().as_str(),
                uri.as_ref(),
                headers.iter().map(|(k, v)| (k.as_str(), v.as_str())),
                body,
            )
            .map_err(|e| {
                Error::new(ErrorKind::DataInvalid, "Failed to create signable request")
                    .with_source(e)
            })?;

            let (instructions, _signature) = sign(signable, &signing_params)
                .map_err(|e| {
                    Error::new(ErrorKind::DataInvalid, "Failed to sign request").with_source(e)
                })?
                .into_parts();

            let (new_headers, new_query) = instructions.into_parts();

            for header in new_headers {
                req.headers_mut().insert(
                    header.name(),
                    header.value().parse().map_err(|e| {
                        Error::new(ErrorKind::DataInvalid, "Failed to parse header value")
                            .with_source(e)
                    })?,
                );
            }

            if !new_query.is_empty() {
                // Merge existing query params with newly signed params, overwriting any
                // existing keys that appear in the signed output while preserving others.
                let replace_keys: HashSet<&str> = new_query.iter().map(|(k, _)| *k).collect();
                let preserved: Vec<(String, String)> = req
                    .url()
                    .query_pairs()
                    .filter(|(k, _)| !replace_keys.contains(k.as_ref()))
                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                    .collect();

                let url = req.url_mut();
                {
                    let mut qp = url.query_pairs_mut();
                    qp.clear();
                    for (k, v) in preserved {
                        qp.append_pair(&k, &v);
                    }
                    for (k, v) in new_query {
                        qp.append_pair(k, &v);
                    }
                }
            }
        }
        Ok(())
    }

    /// Authenticate the request by filling token.
    ///
    /// - If neither token nor credential is provided, this method will do nothing.
    /// - If only credential is provided, this method will try to fetch token from the server.
    /// - If token is provided, this method will use the token directly.
    ///
    /// # TODO
    ///
    /// Support refreshing token while needed.
    async fn authenticate_oauth(&self, req: &mut Request) -> Result<()> {
        // Clone the token from lock without holding the lock for entire function.
        let token = self.token.lock().await.clone();

        if self.credential.is_none() && token.is_none() {
            return Ok(());
        }

        // Use token if provided.
        if let Some(token) = &token {
            req.headers_mut().insert(
                http::header::AUTHORIZATION,
                format!("Bearer {token}").parse().map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Invalid token received from catalog server!",
                    )
                    .with_source(e)
                })?,
            );
            return Ok(());
        }

        let token = self.exchange_credential_for_token().await?;
        // Update token.
        *self.token.lock().await = Some(token.clone());
        // Insert token in request.
        req.headers_mut().insert(
            http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Invalid token received from catalog server!",
                )
                .with_source(e)
            })?,
        );

        Ok(())
    }

    /// If `aws_config` is set, this method will use it to sign the request.
    /// If `credential` is set, this method will use it to authenticate the request.
    /// If neither is set, this method will do nothing.
    async fn authenticate(&self, req: &mut Request) -> Result<()> {
        if self.aws_config.is_some() {
            return self.authenticate_sigv4(req).await;
        } else if self.credential.is_some() {
            return self.authenticate_oauth(req).await;
        }

        Ok(())
    }

    #[inline]
    pub fn request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        self.client
            .request(method, url)
            .headers(self.extra_headers.clone())
    }

    /// Executes the given `Request` and returns a `Response`.
    pub async fn execute(&self, request: Request) -> Result<Response> {
        Ok(self.client.execute(request).await?)
    }

    // Queries the Iceberg REST catalog after authentication with the given `Request` and
    // returns a `Response`.
    pub async fn query_catalog(&self, mut request: Request) -> Result<Response> {
        // It's important to add extra headers before authentication,
        // so that they are included in the signed request.
        request.headers_mut().extend(self.extra_headers.clone());
        self.authenticate(&mut request).await?;
        self.execute(request).await
    }
}

/// Deserializes a catalog response into the given [`DeserializedOwned`] type.
///
/// Returns an error if unable to parse the response bytes.
pub(crate) async fn deserialize_catalog_response<R: DeserializeOwned>(
    response: Response,
) -> Result<R> {
    let bytes = response.bytes().await?;

    serde_json::from_slice::<R>(&bytes).map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            "Failed to parse response from rest catalog server",
        )
        .with_context("json", String::from_utf8_lossy(&bytes))
        .with_source(e)
    })
}

/// Deserializes a unexpected catalog response into an error.
pub(crate) async fn deserialize_unexpected_catalog_error(response: Response) -> Error {
    let err = Error::new(
        ErrorKind::Unexpected,
        "Received response with unexpected status code",
    )
    .with_context("status", response.status().to_string())
    .with_context("headers", format!("{:?}", response.headers()));

    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => return err.into(),
    };

    if bytes.is_empty() {
        return err;
    }
    err.with_context("json", String::from_utf8_lossy(&bytes))
}
