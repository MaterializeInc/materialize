// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use reqwest::Client;

use mz::configuration::ValidProfile;
use mz::utils::RequestBuilderExt;

use crate::FronteggAppPassword;

/// Get Frontegg API tokens using an access token
pub(crate) async fn list_passwords(
    client: &Client,
    valid_profile: &ValidProfile<'_>,
) -> Result<Vec<FronteggAppPassword>> {
    let mut body = BTreeMap::new();
    body.insert("description", &"App password for the CLI");

    client
        .get(valid_profile.profile.endpoint().api_token_url())
        .authenticate(&valid_profile.frontegg_auth)
        .json(&body)
        .send()
        .await
        .context("failed to communicate with server")?
        .json::<Vec<FronteggAppPassword>>()
        .await
        .context("failed to parse results from server")
}
