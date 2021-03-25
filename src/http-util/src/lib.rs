// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for constructing HTTP Clients

use hyper::Uri;
use hyper_proxy::{Intercept, Proxy, ProxyConnector};
use hyper_tls::HttpsConnector;
use log::debug;

/// A proxied HTTP Connector
pub type ProxiedConnector = ProxyConnector<HttpsConnector<hyper::client::HttpConnector>>;

/// Get an [`ProxiedConnector`] that respects the `http_proxy` environment variables
///
/// This can be used with [`hyper::client::Builder::build`] to create a client that uses the proxy.
/// Anywhere that expects a [`hyper::client::HttpConnector`] might be able to use the output of this, as
/// well.
pub fn connector() -> Result<ProxiedConnector, Box<dyn std::error::Error + Send + Sync>> {
    let inner = HttpsConnector::new();
    let mut connector = ProxyConnector::new(inner)?;
    for var in &["http_proxy", "HTTP_PROXY"] {
        if let Ok(proxy_url) = std::env::var(var) {
            let proxy = Proxy::new(Intercept::Http, proxy_url.parse::<Uri>()?);
            connector.add_proxy(proxy);
            debug!("Using HTTP proxy: {}", proxy_url);
            break;
        }
    }

    for var in &["https_proxy", "HTTPS_PROXY"] {
        if let Ok(proxy_url) = std::env::var(var) {
            let proxy = Proxy::new(Intercept::Https, proxy_url.parse::<Uri>()?);
            connector.add_proxy(proxy);
            debug!("Using HTTPS proxy: {}", proxy_url);
            break;
        }
    }

    for var in &["all_proxy", "ALL_PROXY"] {
        if let Ok(proxy_url) = std::env::var(var) {
            let proxy = Proxy::new(Intercept::All, proxy_url.parse::<Uri>()?);
            connector.add_proxy(proxy);
            debug!("Using HTTP/HTTPS proxy: {}", proxy_url);
            break;
        }
    }

    Ok(connector)
}

/// Create a `reqwest` client builder that obeys `http_proxy` environment variables
pub fn reqwest_client_builder() -> Result<reqwest::ClientBuilder, reqwest::Error> {
    let mut builder = reqwest::ClientBuilder::new();
    for var in &["http_proxy", "HTTP_PROXY"] {
        if let Ok(proxy_url) = std::env::var(var) {
            debug!("Using HTTP proxy for reqwest: {}", proxy_url);
            builder = builder.proxy(reqwest::Proxy::http(proxy_url)?);
            break;
        }
    }

    for var in &["https_proxy", "HTTPS_PROXY"] {
        if let Ok(proxy_url) = std::env::var(var) {
            debug!("Using HTTPS proxy for reqwest: {}", proxy_url);
            builder = builder.proxy(reqwest::Proxy::https(proxy_url)?);
            break;
        }
    }

    for var in &["all_proxy", "ALL_PROXY"] {
        if let Ok(proxy_url) = std::env::var(var) {
            debug!("Using HTTP/HTTPS proxy for reqwest: {}", proxy_url);
            builder = builder.proxy(reqwest::Proxy::all(proxy_url)?);
            break;
        }
    }

    Ok(builder)
}

/// Create a `reqwest` client that obeys `http_proxy` environment variables
pub fn reqwest_client() -> Result<reqwest::Client, reqwest::Error> {
    Ok(reqwest_client_builder()?.build()?)
}
