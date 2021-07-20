// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Adapters for [`hyper`].

use std::error::Error;

use hyper::client::HttpConnector;
use hyper_proxy::{Proxy, ProxyConnector};
use hyper_tls::HttpsConnector;

use crate::proxy::PROXY_CONFIG;

/// A proxying HTTPS connector for hyper.
pub type Connector = ProxyConnector<HttpsConnector<HttpConnector>>;

/// Create a `hyper` connector that obeys the system proxy configuration.
///
/// For details about the system proxy configuration, see the
/// [crate documentation](crate).
pub fn connector() -> Result<Connector, Box<dyn Error + Send + Sync>> {
    let mut connector = ProxyConnector::new(HttpsConnector::new())?;

    if let Some(http_proxy) = PROXY_CONFIG.http_proxy() {
        let matches = move |scheme: Option<&str>, host: Option<&str>, port| {
            scheme == Some("http") && !PROXY_CONFIG.exclude(scheme, host, port)
        };
        connector.add_proxy(Proxy::new(matches, http_proxy.clone()));
    }

    if let Some(https_proxy) = PROXY_CONFIG.https_proxy() {
        let matches = move |scheme: Option<&str>, host: Option<&str>, port| {
            scheme == Some("https") && !PROXY_CONFIG.exclude(scheme, host, port)
        };
        connector.add_proxy(Proxy::new(matches, https_proxy.clone()));
    }

    if let Some(all_proxy) = PROXY_CONFIG.all_proxy() {
        let matches = move |scheme: Option<&str>, host: Option<&str>, port| {
            !PROXY_CONFIG.exclude(scheme, host, port)
        };
        connector.add_proxy(Proxy::new(matches, all_proxy.clone()));
    }

    Ok(connector)
}
