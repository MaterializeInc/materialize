// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use reqwest::Method;
use serde::Deserialize;

use crate::{app_password::AppPassword, error::{FronteggError}};

use super::{Client, APP_PASSWORDS_PATH, CREATE_APP_PASSWORDS_PATH};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct FronteggAppPassword {
    description: String,
    created_at: String,
}

impl Client {
    /// Lists all existing app passwords.
    pub async fn list_app_passwords(&self) -> Result<Vec<FronteggAppPassword>, FronteggError> {
        let req = self.build_request(Method::GET, APP_PASSWORDS_PATH);
        let passwords: Vec<FronteggAppPassword> = self.send_request(req).await?;
        Ok(passwords)
    }

    /// Lists all existing app passwords.
    pub async fn create_app_password(&self, description: String) -> Result<AppPassword, FronteggError> {
        let req = self.build_request(Method::POST, CREATE_APP_PASSWORDS_PATH);
        let mut body = BTreeMap::new();
        body.insert("description", description);

        let req = req.json(&body);

        let password: AppPassword = self.send_request(req).await?;
        Ok(password)
    }
}
