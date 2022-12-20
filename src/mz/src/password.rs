// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::configuration::ValidProfile;
use crate::FronteggAppPassword;
use anyhow::{Context, Result};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use reqwest::Client;

/// Get Frontegg API tokens using an access token
pub(crate) async fn list_passwords(
    client: &Client,
    valid_profile: &ValidProfile<'_>,
) -> Result<Vec<FronteggAppPassword>> {
    let authorization: String = format!("Bearer {}", valid_profile.frontegg_auth.access_token);

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("reqwest"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(authorization.as_str()).unwrap(),
    );
    let mut body = HashMap::new();
    body.insert("description", &"App password for the CLI");

    client
        .get(valid_profile.profile.endpoint().api_token_url())
        .headers(headers)
        .json(&body)
        .send()
        .await
        .context("failed to communicate with server")?
        .json::<Vec<FronteggAppPassword>>()
        .await
        .context("failed to parse results from server")
}
