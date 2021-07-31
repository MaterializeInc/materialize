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

#![deny(missing_docs)]
#![cfg_attr(nightly_doc_features, feature(doc_cfg))]

//! [<img src="https://materialize.com/wp-content/uploads/2020/01/materialize_logo_primary.png" width=180 align=right>](https://materialize.com)
//!
//! HTTP proxy adapters.
//!
//! This crate constructs HTTP clients that respect the system's proxy
//! configuration.
//!
//! # Maintainership
//!
//! This crate is developed as part of [Materialize], the streaming data
//! warehouse. Contributions are encouraged:
//!
//! * [View source code](https://github.com/MaterializeInc/materialize/tree/main/src/http-proxy)
//! * [Report an issue](https://github.com/MaterializeInc/materialize/issues/new/choose)
//! * [Submit a pull request](https://github.com/MaterializeInc/materialize/compare)
//!
//! # Features
//!
//! All features are disabled by default. You will likely want to enable at
//! least one of the following features depending on the HTTP client library you
//! use:
//!
//! * The **`hyper`** feature enables the proxy adapters for use with the
//!   [`hyper` crate](https://docs.rs/hyper).
//!
//! * The **`reqwest`** feature enables the proxy adapters for use with the
//!   [`reqwest` crate](https://docs.rs/reqwest).
//!
//! Note that `reqwest` by default will perform its own determination of the
//! system proxy configuration, but its support for `no_proxy` is not as
//! complete as the implementation in this crate.
//!
//! # System proxy configuration
//!
//! The system's proxy configuration is governed by four environment variables,
//! `http_proxy`, `https_proxy`, `all_proxy`, and `no_proxy`, whose meanings are
//! nonstandard and vary widely from tool to tool. Materialize implements a
//! subset of the behavior that has been empirically determined to be common to
//! many other HTTP clients.
//!
//! With the exception of `http_proxy`, environment variables may be specified
//! in all lowercase, as written above, or in all uppercase (e.g,
//! `HTTPS_PROXY`). `http_proxy` is accepted in lowercase only because the
//! variable `HTTP_PROXY` can be controlled by attackers in CGI environments, as
//! in [golang/go#16405]. If an environment variable is specified in both
//! lowercase and uppercase, the lowercase variable takes precedence.
//!
//! ## Proxy selection
//!
//! The `http_proxy` and `https_proxy` environment variables specify the URL of
//! a proxy server to use when routing HTTP and HTTPS traffic, respectively. The
//! `all_proxy` environment variable specifies a proxy server that applies to
//! both HTTP and HTTPS traffic. `http_proxy` and `https_proxy` take precedence
//! over `all_proxy`.
//!
//! ## Proxy exclusions
//!
//! The `no_proxy` environment variable is a comma-separated list specifying
//! hosts to exclude from proxying. It takes precedence over the other
//! environment variables. Each entry in the list must be:
//!
//!   * an IP address followed by an optional port (e.g., `1.2.3.4`,
//!     `1.2.3.4:80`, `::1`, `[::1]`, or `[::1]:80`),
//!   * an IP address prefix in CIDR notation (e.g., `1.1.0.0/16`), or
//!   * a domain name followed by an optional port (e.g., `foo.com`).
//!
//! Whitespace surrounding an entry is ignored.
//!
//! IPv6 addresses cannot contain whitespace inside the `[` and `]` characters
//! or they will be treated as domains. IPv6 addresses that are followed by a
//! port specification must be surrounded by `[` and `]` or the port will be
//! considered part of the IPv6 address. (The implementation technically allows
//! IPv4 addresses to be wrapped in square brackets as well, for compatibility
//! with other tools, but this should not be relied upon.)
//!
//! `no_proxy` matching never involves DNS resolution, so a `no_proxy` value of
//! `1.2.3.4` will exclude requests that literally mention the IP in the URL
//! (e.g., `http://1.2.3.4`) from proxying, but not requests to
//! `http://domainthatresolvesto1234`.
//!
//! Domain names in `no_proxy` match all subdomains, so a `no_proxy` value of
//! `materialize.com` will exclude requests to both `materialize.com` and
//! `cloud.materialize.com` from proxying. For compatibility with other tools,
//! domain names can include one optional `.` character at the start, which is
//! ignored.
//!
//! Invalid entries in the list are silently ignored.
//!
//! If the `no_proxy` environment variable is set to the special value `*`, then
//! all addresses will be excluded from proxying.
//!
//! ## See also
//!
//! For further details on these environment variables, see the GitLab blog
//! article ["We need to talk: can we standardize NO_PROXY?"][gitlab-blog].
//!
//! [golang/go#16405]: https://github.com/golang/go/issues/16405
//! [gitlab-blog]: https://about.gitlab.com/blog/2021/01/27/we-need-to-talk-no-proxy/
//! [Materialize]: https://materialize.com
//! [repo]: https://github.com/MaterializeInc/materialize

#[cfg(any(feature = "hyper", feature = "reqwest"))]
mod proxy;

#[cfg_attr(nightly_doc_features, doc(cfg(feature = "hyper")))]
#[cfg(feature = "hyper")]
pub mod hyper;

#[cfg_attr(nightly_doc_features, doc(cfg(feature = "reqwest")))]
#[cfg(feature = "reqwest")]
pub mod reqwest;
