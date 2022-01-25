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

//! Proxy adapters for [`hyper`](hyper_dep).

use hyper_dep::client::HttpConnector;
use hyper_proxy::{Proxy, ProxyConnector};
use hyper_tls::HttpsConnector;

use crate::proxy::PROXY_CONFIG;

/// A proxying HTTPS connector for hyper.
pub type Connector = ProxyConnector<HttpsConnector<HttpConnector>>;

/// Create a `hyper` connector that obeys the system proxy configuration.
///
/// For details about the system proxy configuration, see the
/// [crate documentation](crate).
pub fn connector() -> Connector {
    // `ProxyConnector::new` only errors if creating a TLS context fails, and if
    // that's broken, `HttpsConnector::new()` has already panicked. So no point
    // returning an error here instead of panicking. It's much more convenient
    // downstream and more consistent with the rest of the Rust ecosystem if
    // creating a connector is infallible.
    let mut connector = ProxyConnector::new(HttpsConnector::new())
        .unwrap_or_else(|e| panic!("hyper_proxy::ProxyConnector::new failure: {}", e));

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

    connector
}
