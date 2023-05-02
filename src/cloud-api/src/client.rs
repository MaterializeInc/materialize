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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use mz_frontegg_auth::AppPassword;

    use crate::{
        client::{cloud_provider::CloudProviderRegion, Client},
        config::{ClientBuilder, ClientConfig},
    };

    #[tokio::test]
    async fn test_cloud_endpoints() {
        struct TestCase {
            psw: &'static str,
        }

        let app_password: AppPassword = env!("MZ_PSW").parse().unwrap();
        let auth_client = mz_frontegg_client::config::ClientBuilder::default().build(
            mz_frontegg_client::config::ClientConfig {
                app_password: app_password,
            },
        );

        let passwords = auth_client.list_app_passwords().await.unwrap_or(vec![]);
        assert!(passwords.len() > 0);

        let client: Client = ClientBuilder::default().build(ClientConfig { auth_client });

        // List all the available providers
        let cloud_providers = client.list_cloud_providers().await.unwrap();
        assert!(cloud_providers.len() == 2);

        // Get all the environments
        let all_environments = client.get_all_environments().await.unwrap();
        assert!(all_environments.len() == 2);

        let user_input = "aws/us-east-1";
        let cloud_provider_region_selected: CloudProviderRegion =
            CloudProviderRegion::from_str(user_input)?;
        let cloud_provider = cloud_providers
            .iter()
            .find(|cp| cp.as_cloud_provider_region().unwrap() == cloud_provider_region_selected)
            .unwrap()
            .to_owned();

        // Get a a region using a cloud provider
        let region = client.get_region(cloud_provider).await.unwrap();

        assert!(
            region.environment_controller_url.to_string()
                == "https://ec.0.us-east-1.aws.cloud.materialize.com/"
        );

        let environment = client.get_environment(region).await.unwrap();

        let http_address: String = env!("MZ_ADDR").parse().unwrap();
        assert!(environment.environmentd_https_address == http_address);
    }
}
