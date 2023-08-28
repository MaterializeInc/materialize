// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use uuid::Uuid;

use crate::{Client, Error};

impl Client {
    /// Exchanges a client id and secret for a jwt token.
    pub async fn exchange_client_secret_for_token(
        &self,
        client_id: Uuid,
        secret: Uuid,
        admin_api_token_url: &str,
    ) -> Result<ApiTokenResponse, Error> {
        let res = self
            .client
            .post(admin_api_token_url)
            .json(&ApiTokenArgs { client_id, secret })
            .send()
            .await?
            .error_for_status()?
            .json::<ApiTokenResponse>()
            .await?;
        Ok(res)
    }

    /// Exchanges a client id and secret for a jwt token.
    pub async fn refresh_token(
        &self,
        refresh_url: &str,
        refresh_token: &str,
    ) -> Result<ApiTokenResponse, Error> {
        let res = self
            .client
            .post(refresh_url)
            .json(&RefreshToken { refresh_token })
            .send()
            .await?
            .error_for_status()?
            .json::<ApiTokenResponse>()
            .await?;
        Ok(res)
    }
}

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiTokenResponse {
    pub expires: String,
    pub expires_in: i64,
    pub access_token: String,
    pub refresh_token: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiTokenArgs {
    pub client_id: Uuid,
    pub secret: Uuid,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshToken<'a> {
    pub refresh_token: &'a str,
}

#[cfg(test)]
mod tests {
    use axum::{routing::post, Router};
    use reqwest::StatusCode;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use uuid::Uuid;

    use super::ApiTokenResponse;
    use crate::Client;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    async fn response_retries() {
        let count = Arc::new(AtomicUsize::new(0));
        let count_ = Arc::clone(&count);

        // Fake server that returns the provided status code a few times before returning success.
        let app = Router::new().route(
            "/:status_code",
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
        let tcp = std::net::TcpListener::bind(addr).expect("able to bind");
        let addr = tcp.local_addr().expect("valid addr");
        mz_ore::task::spawn(|| "test-server", async move {
            axum::Server::from_tcp(tcp)
                .expect("able to start")
                .serve(app.into_make_service())
                .await
                .unwrap();
        });

        let client = Client::default();
        async fn test_case(
            client: &Client,
            addr: &SocketAddr,
            count: &Arc<AtomicUsize>,
            code: u16,
            should_retry: bool,
        ) -> Result<(), String> {
            let exchange_result = client
                .exchange_client_secret_for_token(
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    &format!("http://{addr}/{code}"),
                )
                .await
                .map(|_| ())
                .map_err(|e| e.to_string());
            let prev_count = count.swap(0, Ordering::Relaxed);
            let expected_count = should_retry.then_some(3).unwrap_or(1);
            assert_eq!(prev_count, expected_count);

            let refresh_result = client
                .refresh_token(&format!("http://{addr}/{code}"), "test")
                .await
                .map(|_| ())
                .map_err(|e| e.to_string());
            let prev_count = count.swap(0, Ordering::Relaxed);
            let expected_count = should_retry.then_some(3).unwrap_or(1);
            assert_eq!(prev_count, expected_count);

            assert_eq!(exchange_result, refresh_result);
            exchange_result
        }

        // Should get retried which results in eventual success.
        assert!(test_case(&client, &addr, &count, 500, true).await.is_ok());
        assert!(test_case(&client, &addr, &count, 502, true).await.is_ok());
        assert!(test_case(&client, &addr, &count, 429, true).await.is_ok());
        assert!(test_case(&client, &addr, &count, 408, true).await.is_ok());

        // Should not get retried, and thus return an error.
        assert!(test_case(&client, &addr, &count, 404, false).await.is_err());
        assert!(test_case(&client, &addr, &count, 400, false).await.is_err());
    }
}
