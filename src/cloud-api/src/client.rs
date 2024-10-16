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
//! interacting with the Materialize cloud API. This client includes,
//! token management, and basic requests against the API.
//!
//! The [`Client`] requires an [`mz_frontegg_client::client::Client`] as a parameter. The
//! Frontegg client is used to request and manage the access token.
use std::sync::Arc;

use reqwest::{header::HeaderMap, Method, RequestBuilder, StatusCode, Url};
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::config::API_VERSION_HEADER;
use crate::error::{ApiError, Error};

use self::cloud_provider::CloudProvider;

/// Represents the structure for the client.
pub struct Client {
    pub(crate) inner: reqwest::Client,
    pub(crate) auth_client: Arc<mz_frontegg_client::client::Client>,
    pub(crate) endpoint: Url,
}

pub mod cloud_provider;
pub mod region;

/// Cloud endpoints architecture:
///
/// (CloudProvider)                                (Region)
///   ---------                     --------------------------------------
///  |          |                  |              Region API              |
///  |  Cloud   |        url       |    ----------        -------------   |
///  |  Sync    | ---------------> |   | Provider | ---- |    Region   |  |
///  |          |                  |   | (aws..)  |      |  (east-1..) |  |
///  |          |                  |    ----------        -------------   |
///   ----------                    --------------------------------------
///
impl Client {
    /// Builds a request towards the `Client`'s endpoint
    async fn build_global_request<P>(
        &self,
        method: Method,
        path: P,
        query: Option<&[(&str, &str)]>,
    ) -> Result<RequestBuilder, Error>
    where
        P: IntoIterator,
        P::Item: AsRef<str>,
    {
        self.build_request(method, path, query, self.endpoint.clone(), None)
            .await
    }

    /// Builds a request towards the `Client`'s endpoint
    /// The function requires a [CloudProvider] as parameter
    /// since it contains the api url (Region API url)
    /// to interact with the region.
    /// Specify an api_version corresponding to the request/response
    /// schema your code will handle. Refer to the Region API docs
    /// for schema information.
    async fn build_region_request<P>(
        &self,
        method: Method,
        path: P,
        query: Option<&[(&str, &str)]>,
        cloud_provider: &CloudProvider,
        api_version: Option<u16>,
    ) -> Result<RequestBuilder, Error>
    where
        P: IntoIterator,
        P::Item: AsRef<str>,
    {
        self.build_request(
            method,
            path,
            query,
            cloud_provider.url.clone(),
            api_version.and_then(|api_ver| {
                let mut headers = HeaderMap::with_capacity(1);
                headers.insert(API_VERSION_HEADER, api_ver.into());
                Some(headers)
            }),
        )
        .await
    }

    /// Builds a request towards the `Client`'s endpoint
    async fn build_request<P>(
        &self,
        method: Method,
        path: P,
        query: Option<&[(&str, &str)]>,
        mut domain: Url,
        headers: Option<HeaderMap>,
    ) -> Result<RequestBuilder, Error>
    where
        P: IntoIterator,
        P::Item: AsRef<str>,
    {
        domain
            .path_segments_mut()
            .or(Err(Error::UrlBaseError))?
            .clear()
            .extend(path);

        let mut req = self.inner.request(method, domain);
        if let Some(header_map) = headers {
            req = req.headers(header_map);
        }
        if let Some(query_params) = query {
            req = req.query(&query_params);
        }
        let token = self.auth_client.auth().await?;

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
            if status_code == StatusCode::NO_CONTENT {
                Err(Error::SuccesfullButNoContent)
            } else {
                Ok(res.json().await?)
            }
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
