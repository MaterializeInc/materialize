// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! # Datadog API client
//!
//! This module provides validation and metric methods
//! for interacting with the Datadog API.

use std::time::Duration;

use reqwest::{Method, RequestBuilder};
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::{
    config::DEFAULT_ENDPOINT,
    error::{ApiError, Error},
};

/// Represents the structure for the client.
pub struct Client<'a> {
    pub(crate) inner: reqwest::Client,
    pub(crate) api_key: &'a str,
}

/// Contains the metrics interface.
pub mod metrics;
/// Contains the validation interface.
pub mod validation;

impl<'a> Client<'a> {
    /// Builds a request towards the `Client`'s endpoint
    fn build_request<P>(&self, method: Method, path: P) -> Result<RequestBuilder, Error>
    where
        P: IntoIterator,
        P::Item: AsRef<str>,
    {
        let mut domain = DEFAULT_ENDPOINT.clone();
        domain
            .path_segments_mut()
            .or(Err(Error::UrlBaseError))?
            .clear()
            .extend(path);

        let req = self
            .inner
            .request(method, domain)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("DD-API-KEY", self.api_key)
            .timeout(Duration::from_secs(60));

        Ok(req)
    }

    async fn send_request<T>(&self, req: RequestBuilder) -> Result<T, Error>
    where
        T: DeserializeOwned,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ErrorResponse {
            #[serde(default)]
            errors: Vec<String>,
        }

        let res = req.send().await?;
        let status_code = res.status();
        if status_code.is_success() {
            Ok(res.json().await?)
        } else {
            match res.json::<ErrorResponse>().await {
                Ok(e) => Err(Error::Api(ApiError {
                    status_code,
                    errors: e.errors,
                })),
                Err(_) => Err(Error::Api(ApiError {
                    status_code,
                    errors: vec!["unable to decode error details".into()],
                })),
            }
        }
    }
}

// TODO: This is a temporal test and should not be merged.
#[cfg(test)]
mod tests {
    use crate::client::metrics::MetricType;
    use crate::client::metrics::{Point, Series, SubmitMetricsParams};
    use crate::config::{ClientBuilder, ClientConfig};
    use crate::error::Error;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    pub async fn submit_a_metric() -> Result<(), Error> {
        let start = SystemTime::now();
        let since_the_epoch: i64 = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as i64;

        let client = ClientBuilder::default().build(ClientConfig {
            api_key: env!("DATADOG_API_KEY"),
        });

        client
            .submit_metric(SubmitMetricsParams {
                series: vec![Series {
                    metric: "test_metric",
                    points: vec![Point {
                        timestamp: since_the_epoch.into(),
                        value: 80.0,
                    }],
                    source_type_name: "Materialize",
                    typ: MetricType::Gauge,
                    interval: None,
                    metadata: None,
                    resources: None,
                    tags: None,
                    unit: None,
                }],
            })
            .await?;

        Ok(())
    }
}
