// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::{FronteggAppPassword, ValidProfile, API_FRONTEGG_TOKEN_AUTH_URL};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use reqwest::Client;

/// Get Frontegg API tokens using an access token
pub(crate) async fn list_passwords(
    client: &Client,
    valid_profile: &ValidProfile,
) -> Result<Vec<FronteggAppPassword>, reqwest::Error> {
    let authorization: String = format!("Bearer {}", valid_profile.frontegg_auth.access_token);

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("reqwest"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(authorization.as_str()).unwrap(),
    );
    let mut body = HashMap::new();
    body.insert("description", &"Token for the CLI");

    client
        .get(API_FRONTEGG_TOKEN_AUTH_URL)
        .headers(headers)
        .json(&body)
        .send()
        .await?
        .json::<Vec<FronteggAppPassword>>()
        .await
}
