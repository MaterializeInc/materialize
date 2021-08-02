// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![deny(missing_docs)]

//! HTTP utilities.
//!
//! This crate constructs HTTP clients that respect the system's proxy
//! configuration.
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
//! in all lowercase, as written above, or in all uppercase (e.g, `HTTPS_PROXY`)
//! (http_proxy` is accepted in lowercase only because the variable `HTTP_PROXY`
//! can be controlled by attackers in CGI environments, as in
//! [golang/go#16405].) If an environment variable is specified in both
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

mod proxy;

pub mod hyper;
pub mod reqwest;
