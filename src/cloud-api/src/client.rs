// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! # Materialize cloud API client
//!
//! This module provides an API client with typed methods for
//! interacting with the Materialize cloud API. This client i,
//! token management, and basic requests against the API.
//!
//! The [`Client`] requires an [`mz_frontegg_client::client::Client`] as a parameter. The
//! Frontegg client is used to request and manage the access token.

use reqwest::{Method, RequestBuilder, Url};
use serde::{de::DeserializeOwned, Deserialize};

use crate::error::{ApiError, Error};

/// Represents the structure for the client.
#[allow(dead_code)]
pub struct Client {
    pub(crate) inner: reqwest::Client,
    pub(crate) auth_client: mz_frontegg_client::client::Client,
    pub(crate) endpoint: Url,
}

pub mod cloud_provider;
pub mod environment;
pub mod region;

/// Cloud endpoints architecture:
///
/// (CloudProvider)                         (Region)                                 (Environment)
///   ---------              --------------------------------------            ------------------------
///  |          |           |          Region Controller           |          | Environment Controller |
///  |  Cloud   |  api_url  |    ----------        -------------   |  ec_url  |                        |
///  |  Sync    | --------> |   | Provider | ---- |    Region   |  | -------> |       Environment      |
///  |          |           |   | (aws..)  |      |  (east-1..) |  |          |  (pgwire_address...)   |
///  |          |           |    ----------        -------------   |          |                        |
///   ----------             --------------------------------------            -----------------------
///
impl Client {
    /// Builds a request towards the `Client`'s endpoint
    async fn build_request<P>(
        &self,
        method: Method,
        path: P,
        subdomain: &str,
    ) -> Result<RequestBuilder, Error>
    where
        P: IntoIterator,
        P::Item: AsRef<str>,
    {
        let mut url = self.endpoint.clone();

        // Set the new host using a subdomain
        let host = format!("{}.{}", subdomain, url.host().unwrap().to_owned());
        url.set_host(Some(&host)).unwrap();

        url.path_segments_mut()
            .expect("builder validated URL can be a base")
            .clear()
            .extend(path);

        let req = self.inner.request(method, url);
        // let token = self.auth_client.auth().await.unwrap();
        let token = "";

        Ok(req.bearer_auth(token))
    }

    async fn send_request<T>(&self, req: RequestBuilder) -> Result<T, Error>
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
                    Err(Error::Api(ApiError {
                        status_code,
                        messages,
                    }))
                }
                Err(_) => Err(Error::Api(ApiError {
                    status_code,
                    messages: vec!["unable to decode error details".into()],
                })),
            }
        }
    }
}
