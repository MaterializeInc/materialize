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

//! Reusable arguments that can be mixed in to commands.

use url::Url;

/// Validate if a profile name consist of only ASCII letters, ASCII digits, underscores, and dashes.
pub fn validate_profile_name(val: &str) -> Result<String, String> {
    if val.is_empty() {
        return Err(String::from("Profile name should not be empty"));
    }
    for c in val.chars() {
        if !(c.is_ascii_alphabetic() || c.is_ascii_digit() || c == '_' || c == '-') {
            return Err(format!("Invalid character '{}' in profile name.\nThe profile name must consist of only ASCII letters, ASCII digits, underscores, and dashes.", c));
        }
    }
    Ok(String::from(val))
}

#[derive(Debug, clap::Args)]
pub struct EndpointArgs {
    /// Use the specified cloud endpoint.
    #[clap(long, hide = true, env = "CLOUD_ENDPOINT")]
    pub cloud_endpoint: Option<Url>,
    /// Use the specified admin endpoint.
    #[clap(long, hide = true, env = "ADMIN_ENDPOINT")]
    pub admin_endpoint: Option<Url>,
}
