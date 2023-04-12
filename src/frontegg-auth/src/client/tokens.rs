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
        self.request(|client| async move {
            let res: ApiTokenResponse = client
                .post(admin_api_token_url)
                .json(&ApiTokenArgs { client_id, secret })
                .send()
                .await?
                .error_for_status()?
                .json::<ApiTokenResponse>()
                .await?;
            Ok(res)
        })
        .await
    }

    /// Exchanges a client id and secret for a jwt token.
    pub async fn refresh_token(
        &self,
        refresh_url: &str,
        refresh_token: &str,
    ) -> Result<ApiTokenResponse, Error> {
        self.request(|client| async move {
            let res: ApiTokenResponse = client
                .post(refresh_url)
                .json(&RefreshToken { refresh_token })
                .send()
                .await?
                .error_for_status()?
                .json::<ApiTokenResponse>()
                .await?;
            Ok(res)
        })
        .await
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
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
