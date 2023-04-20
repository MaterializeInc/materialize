// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_frontegg_auth::AppPassword;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ErrorExtended;

use super::Client;

const APP_PASSWORDS_PATH: [&str; 5] = ["identity", "resources", "users", "api-tokens", "v1"];
const CREATE_APP_PASSWORDS_PATH: [&str; 6] = [
    "frontegg",
    "identity",
    "resources",
    "users",
    "api-tokens",
    "v1",
];

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct FronteggAppPassword {
    description: String,
    created_at: String,
}

#[derive(Serialize)]
struct AppPasswordCreateRequest {
    description: String,
}

impl Client {
    /// Lists all existing app passwords.
    pub async fn list_app_passwords(&self) -> Result<Vec<FronteggAppPassword>, ErrorExtended> {
        let req = self.build_request(Method::GET, APP_PASSWORDS_PATH);
        let passwords: Vec<FronteggAppPassword> = self.send_request(req).await?;
        Ok(passwords)
    }

    /// Creates a new app password with the provided description.
    pub async fn create_app_password(
        &self,
        description: String,
    ) -> Result<AppPassword, ErrorExtended> {
        let req = self.build_request(Method::POST, CREATE_APP_PASSWORDS_PATH);
        let req = req.json(&AppPasswordCreateRequest { description });

        // Temp AppPassword structure implementing Deserialization to avoid having any impact in `frontegg-auth`.
        #[derive(Debug, Deserialize)]
        pub struct AppPassword {
            /// The client ID embedded in the app password.
            pub client_id: Uuid,
            /// The secret key embedded in the app password.
            pub secret_key: Uuid,
        }
        let password: AppPassword = self.send_request(req).await?;
        Ok(mz_frontegg_auth::AppPassword {
            client_id: password.client_id,
            secret_key: password.secret_key,
        })
    }
}
