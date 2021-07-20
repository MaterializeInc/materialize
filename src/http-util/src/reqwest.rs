// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Adapters for `reqwest`.

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
