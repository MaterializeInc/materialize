// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module implements the client's functions for interacting with the Frontegg passwords API.

use mz_frontegg_auth::AppPassword as AuthAppPassword;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::client::Client;
use crate::error::Error;

const APP_PASSWORDS_PATH: [&str; 6] = [
    "frontegg",
    "identity",
    "resources",
    "users",
    "api-tokens",
    "v1",
];
const CREATE_APP_PASSWORDS_PATH: [&str; 6] = [
    "frontegg",
    "identity",
    "resources",
    "users",
    "api-tokens",
    "v1",
];

/// A structure that represents an app-password _metadata_.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppPassword {
    /// Description of the password.
    ///
    /// E.g.: "CI/CD Password", "Production password",
    pub description: String,
    /// Creation date in ISO 8601 of the password.
    ///
    /// E.g.: 2023-04-26T08:37:03.000Z
    pub created_at: String,
}

/// Describes a request to create a new app password.
#[derive(Serialize)]
pub struct CreateAppPasswordRequest<'a> {
    /// The description of the app password.
    pub description: &'a str,
}

impl Client {
    /// Lists all existing app passwords.
    pub async fn list_app_passwords(&self) -> Result<Vec<AppPassword>, Error> {
        let req = self.build_request(Method::GET, APP_PASSWORDS_PATH);
        let passwords: Vec<AppPassword> = self.send_request(req).await?;
        Ok(passwords)
    }

    /// Creates a new app password with the provided description.
    pub async fn create_app_password(
        &self,
        app_password: CreateAppPasswordRequest<'_>,
    ) -> Result<AuthAppPassword, Error> {
        let req = self.build_request(Method::POST, CREATE_APP_PASSWORDS_PATH);
        let req = req.json(&app_password);

        // Temp AppPassword structure implementing Deserialization to avoid
        // having any impact in `frontegg-auth`.
        #[derive(Debug, Deserialize)]
        struct AppPassword {
            /// The client ID embedded in the app password.
            #[serde(rename = "clientId")]
            client_id: Uuid,
            /// The secret key embedded in the app password.
            #[serde(rename = "secret")]
            secret_key: Uuid,
        }

        let password: AppPassword = self.send_request(req).await?;
        Ok(AuthAppPassword {
            client_id: password.client_id,
            secret_key: password.secret_key,
        })
    }
}
