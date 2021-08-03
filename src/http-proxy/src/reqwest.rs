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

//! Proxy adapters for [`reqwest`].

use reqwest::ClientBuilder;

use crate::proxy::PROXY_CONFIG;

/// Creates a `reqwest` client builder that obeys the system proxy
/// configuration.
///
/// For details about the system proxy configuration, see the
/// [crate documentation](crate).
pub fn client_builder() -> ClientBuilder {
    let proxy = reqwest::Proxy::custom(move |url| {
        if PROXY_CONFIG.exclude(Some(url.scheme()), url.host_str(), url.port()) {
            return None;
        }
        if let Some(http_proxy) = PROXY_CONFIG.http_proxy() {
            if url.scheme() == "http" {
                return Some(http_proxy.to_string());
            }
        }
        if let Some(https_proxy) = PROXY_CONFIG.https_proxy() {
            if url.scheme() == "https" {
                return Some(https_proxy.to_string());
            }
        }
        if let Some(all_proxy) = PROXY_CONFIG.all_proxy() {
            return Some(all_proxy.to_string());
        }
        None
    });
    reqwest::ClientBuilder::new().proxy(proxy)
}

/// Creates a `reqwest` client that obeys the system proxy configuration.
///
/// For details about the system proxy configuration, see the
/// [crate documentation](crate).
pub fn client() -> reqwest::Client {
    client_builder()
        .build()
        .expect("reqwest::Client known to be valid")
}
