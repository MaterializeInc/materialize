// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! An API client for the Materialize admin API.
//!
//! The admin API is the Frontegg user API, usually hosted at
//! admin.cloud.materialize.com.

// TODO: this module should be shared with the `frontegg-auth` crate, and
// possibly parts of it should live in the docs.rs/frontegg crate.

use anyhow::anyhow;
use url::Url;
use uuid::Uuid;

pub struct AdminClient {
    client: reqwest::Client,
    endpoint: Url,
}

impl AdminClient {
    pub async fn auth(endpoint: Url, app_password: &str) -> Result<AdminClient, anyhow::Error> {
        let client = reqwest::Client::new();
        let (client_id, secret) = parse_app_password("mzp_", app_password)
            .ok_or_else(|| anyhow!("invalid app password"))?;
        Ok(AdminClient { client, endpoint })
    }
}

/// Parses an app password.
///
/// The expected `prefix` must be provided. If parsing is successful, the client
/// ID and secret key are returned.
pub fn parse_app_password(prefix: &str, app_password: &str) -> Option<(Uuid, Uuid)> {
    let app_password = app_password.strip_prefix(&prefix)?;
    if app_password.len() == 43 || app_password.len() == 44 {
        // If it's exactly 43 or 44 bytes, assume we have base64-encoded
        // UUID bytes without or with padding, respectively.
        let buf = base64::decode_config(app_password, base64::URL_SAFE).ok()?;
        let client_id = Uuid::from_slice(&buf[..16]).ok()?;
        let secret = Uuid::from_slice(&buf[16..]).ok()?;
        Some((client_id, secret))
    } else if app_password.len() >= 64 {
        // If it's more than 64 bytes, assume we have concatenated
        // hex-encoded UUIDs, possibly with some special characters mixed
        // in.
        let mut chars = app_password.chars().filter(|c| c.is_alphanumeric());
        let client_id = Uuid::parse_str(&chars.by_ref().take(32).collect::<String>()).ok()?;
        let secret = Uuid::parse_str(&chars.take(32).collect::<String>()).ok()?;
        Some((client_id, secret))
    } else {
        // Otherwise it's definitely not a password format we understand.
        None
    }
}
