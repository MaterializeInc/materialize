// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Instant;

use mz_ore::instrument;
use uuid::Uuid;

use crate::metrics::Metrics;
use crate::{Client, Error};

/// Frontegg includes a trace id in the headers of a response to aid in debugging.
const FRONTEGG_TRACE_ID_HEADER: &str = "frontegg-trace-id";

impl Client {
    /// Exchanges a client id and secret for a jwt token.
    #[instrument]
    pub async fn exchange_client_secret_for_token(
        &self,
        request: ApiTokenArgs,
        admin_api_token_url: &str,
        metrics: &Metrics,
    ) -> Result<ApiTokenResponse, Error> {
        let name = "exchange_secret_for_token";
        let histogram = metrics.request_duration_seconds.with_label_values(&[name]);

        let start = Instant::now();
        let response = self
            .client
            .post(admin_api_token_url)
            .json(&request)
            .send()
            .await?;
        let duration = start.elapsed();

        // Authentication is on the blocking path for connection startup so we
        // want to make sure it stays fast.
        histogram.observe(duration.as_secs_f64());

        let status = response.status().to_string();
        metrics
            .http_request_count
            .with_label_values(&[name, &status])
            .inc();

        let frontegg_trace_id = response
            .headers()
            .get(FRONTEGG_TRACE_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_string());

        match response.error_for_status_ref() {
            Ok(_) => {
                tracing::debug!(
                    ?request.client_id,
                    frontegg_trace_id,
                    ?duration,
                    "request success",
                );
                Ok(response.json().await?)
            }
            Err(e) => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "failed to deserialize body".to_string());
                tracing::warn!(frontegg_trace_id, body, "request failed");
                return Err(e.into());
            }
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiTokenArgs {
    pub client_id: Uuid,
    pub secret: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiTokenResponse {
    pub expires: String,
    pub expires_in: i64,
    pub access_token: String,
    pub refresh_token: String,
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use axum::{Router, routing::post};
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::{assert_err, assert_ok};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;
    use uuid::Uuid;

    use super::ApiTokenResponse;
    use crate::metrics::Metrics;
    use crate::{ApiTokenArgs, Client};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    async fn response_retries() {
        let count = Arc::new(AtomicUsize::new(0));
        let count_ = Arc::clone(&count);

        // Fake server that returns the provided status code a few times before returning success.
        let app = Router::new().route(
            "/{:status_code}",
            post(
                |axum::extract::Path(code): axum::extract::Path<u16>| async move {
                    let cnt = count_.fetch_add(1, Ordering::Relaxed);
                    println!("cnt: {cnt}");

                    let resp = ApiTokenResponse {
                        expires: "test".to_string(),
                        expires_in: 0,
                        access_token: "test".to_string(),
                        refresh_token: "test".to_string(),
                    };
                    let resp = serde_json::to_string(&resp).unwrap();

                    if cnt >= 2 {
                        Ok(resp.clone())
                    } else {
                        Err(StatusCode::from_u16(code).unwrap())
                    }
                },
            ),
        );

        // Use port 0 to get a dynamically assigned port.
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let tcp = TcpListener::bind(addr).await.expect("able to bind");
        let addr = tcp.local_addr().expect("valid addr");
        mz_ore::task::spawn(|| "test-server", async move {
            axum::serve(tcp, app.into_make_service()).await.unwrap();
        });

        let client = Client::default();
        async fn test_case(
            client: &Client,
            addr: &SocketAddr,
            count: &Arc<AtomicUsize>,
            code: u16,
            should_retry: bool,
        ) -> Result<(), String> {
            let registry = MetricsRegistry::new();
            let metrics = Metrics::register_into(&registry);

            let args = ApiTokenArgs {
                client_id: Uuid::new_v4(),
                secret: Uuid::new_v4(),
            };
            let exchange_result = client
                .exchange_client_secret_for_token(args, &format!("http://{addr}/{code}"), &metrics)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string());
            let prev_count = count.swap(0, Ordering::Relaxed);
            let expected_count = should_retry.then_some(3).unwrap_or(1);
            assert_eq!(prev_count, expected_count);

            exchange_result
        }

        // Should get retried which results in eventual success.
        assert_ok!(test_case(&client, &addr, &count, 500, true).await);
        assert_ok!(test_case(&client, &addr, &count, 502, true).await);
        assert_ok!(test_case(&client, &addr, &count, 429, true).await);
        assert_ok!(test_case(&client, &addr, &count, 408, true).await);

        // Should not get retried, and thus return an error.
        assert_err!(test_case(&client, &addr, &count, 404, false).await);
        assert_err!(test_case(&client, &addr, &count, 400, false).await);
    }
}
