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

//! System proxy configuration guts.
//!
//! This module analyzes the system proxy configuration as described in the
//! crate root. If you update the behavior here, please update the description
//! there.

use std::env;
use std::net::IpAddr;
use std::str::FromStr;

use http::uri::{Authority, Uri};
use ipnet::IpNet;
use lazy_static::lazy_static;

use self::matchers::{DomainMatcher, IpMatcher};

lazy_static! {
    pub static ref PROXY_CONFIG: ProxyConfig = load_system_config();
}

fn load_system_config() -> ProxyConfig {
    fn get_any_env(names: &[&str]) -> Option<String> {
        let val = names
            .iter()
            .map(env::var)
            .find(|v| *v != Err(env::VarError::NotPresent));
        match val {
            Some(Ok(val)) => Some(val),
            Some(Err(e)) => {
                tracing::warn!("ignoring invalid configuration for {}: {}", names[0], e);
                None
            }
            None => None,
        }
    }

    fn parse_env_uri(names: &[&str]) -> Option<Uri> {
        match get_any_env(names)?.parse() {
            Ok(uri) => Some(uri),
            Err(e) => {
                tracing::warn!("ignoring invalid configuration for {}: {}", names[0], e);
                None
            }
        }
    }

    let http_proxy = parse_env_uri(&["http_proxy"]);
    let https_proxy = parse_env_uri(&["https_proxy", "HTTPS_PROXY"]);
    let all_proxy = parse_env_uri(&["all_proxy", "ALL_PROXY"]);
    let no_proxy = get_any_env(&["no_proxy", "NO_PROXY"])
        .map(|s| NoProxy::parse(&s))
        .unwrap_or(NoProxy::None);

    ProxyConfig {
        http_proxy,
        https_proxy,
        all_proxy,
        no_proxy,
    }
}

/// The analyzed proxy configuration.
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    http_proxy: Option<Uri>,
    https_proxy: Option<Uri>,
    all_proxy: Option<Uri>,
    no_proxy: NoProxy,
}

impl ProxyConfig {
    /// Returns the proxy to use for HTTP requests, if any.
    pub fn http_proxy(&self) -> Option<&Uri> {
        self.http_proxy.as_ref()
    }

    /// Returns the proxy to use for HTTPS requests, if any.
    pub fn https_proxy(&self) -> Option<&Uri> {
        self.https_proxy.as_ref()
    }

    /// Returns the proxy to use for HTTP or HTTPS requests, if any.
    ///
    /// The proxies returned by [`ProxyConfig::http_proxy`] and
    /// [`ProxyConfig::https_proxy`] take precedence if present.
    pub fn all_proxy(&self) -> Option<&Uri> {
        self.all_proxy.as_ref()
    }

    /// Reports whether the request composed of the given URL scheme, host, and
    /// port should be excluded from proxying.
    pub fn exclude(&self, scheme: Option<&str>, host: Option<&str>, port: Option<u16>) -> bool {
        self.no_proxy.matches(scheme, host, port)
    }
}

#[derive(Debug, Clone)]
enum NoProxy {
    None,
    Some {
        ips: Vec<matchers::IpMatcher>,
        hosts: Vec<matchers::DomainMatcher>,
    },
    All,
}

impl NoProxy {
    fn parse(s: &str) -> NoProxy {
        match s.trim() {
            "" => NoProxy::None,
            "*" => NoProxy::All,
            _ => {
                let mut ips = vec![];
                let mut hosts = vec![];
                for host in s.split(',') {
                    let host = host.trim();

                    // If the entire entry is a CIDR-formatted IP prefix,
                    // we're done. To match Go, we don't allow these to carry
                    // port constraints.
                    if let Ok(net) = IpNet::from_str(host) {
                        ips.push(IpMatcher::from_net(net));
                        continue;
                    }

                    // See if we can split into a host and port. Ignore errors,
                    // because bare IPv6 addresses are not valid authorities but
                    // they *are* valid in `no_proxy`.
                    let authority = Authority::from_str(host).ok();
                    let (mut host, port) = match &authority {
                        Some(authority) => (authority.host(), authority.port_u16()),
                        None => (host, None),
                    };

                    // Trim the brackets surrounding an IPv6 address, if
                    // present.
                    if let Some(h) = host.strip_prefix('[') {
                        if let Some(h) = h.strip_suffix(']') {
                            host = h;
                        }
                    }

                    // If we've trimmed off so much that the host is now empty,
                    // just ignore this entry.
                    if host.is_empty() {
                        continue;
                    }

                    // If it parses as an IP address, treat it as such.
                    // Otherwise treat it as a domain name.
                    if let Ok(addr) = IpAddr::from_str(host) {
                        ips.push(IpMatcher::from_addr(addr, port))
                    } else {
                        hosts.push(DomainMatcher::new(host, port))
                    }
                }
                NoProxy::Some { ips, hosts }
            }
        }
    }

    fn matches(&self, scheme: Option<&str>, host: Option<&str>, port: Option<u16>) -> bool {
        match self {
            NoProxy::None => false,
            NoProxy::All => true,
            NoProxy::Some { ips, hosts } => {
                // If the host is missing, no proxying is required.
                //
                // NOTE(benesch): is the host ever missing in practice?
                let host = match host {
                    Some(host) => host.to_lowercase(),
                    None => return false,
                };

                // Trim the brackets surrounding an IPv6 address, if
                // present.
                let mut host = host.as_str();
                if let Some(h) = host.strip_prefix('[') {
                    if let Some(h) = h.strip_suffix(']') {
                        host = h;
                    }
                }

                // We need a port, but we can infer it from the scheme if
                // necessary. If we have an unknown scheme and port, just give
                // up, though it doesn't seem like this will happen in practice.
                let port = match (scheme, port) {
                    (_, Some(port)) => port,
                    (Some("https"), None) => 443,
                    (Some("http"), None) => 80,
                    _ => return false,
                };

                // If it parses as an IP address, try to find an IP address
                // exclusion that applies. Otherwise try to find a domain
                // exclusion that applies.
                if let Ok(addr) = IpAddr::from_str(host) {
                    ips.iter().any(|m| m.matches(addr, port))
                } else {
                    hosts.iter().any(|m| m.matches(host, port))
                }
            }
        }
    }
}

mod matchers {
    use std::net::IpAddr;

    use ipnet::IpNet;

    #[derive(Debug, Clone)]
    pub struct IpMatcher {
        net: IpNet,
        port: Option<u16>,
    }

    impl IpMatcher {
        pub fn from_net(net: IpNet) -> IpMatcher {
            IpMatcher { net, port: None }
        }

        pub fn from_addr(mut addr: IpAddr, port: Option<u16>) -> IpMatcher {
            Self::normalize_addr(&mut addr);
            IpMatcher {
                net: addr.into(),
                port,
            }
        }

        pub fn matches(&self, mut addr: IpAddr, port: u16) -> bool {
            Self::normalize_addr(&mut addr);
            self.net.contains(&addr) && (self.port.is_none() || self.port == Some(port))
        }

        fn normalize_addr(addr: &mut IpAddr) {
            // Normalize IPv4-mapped IPv6 addresses to IPv4.
            if let IpAddr::V6(v6_addr) = addr {
                if let Some(v4_addr) = v6_addr.to_ipv4() {
                    *addr = IpAddr::V4(v4_addr);
                }
            }
        }
    }

    #[derive(Debug, Clone)]
    pub struct DomainMatcher {
        domain: String,
        port: Option<u16>,
    }

    impl DomainMatcher {
        pub fn new(host: &str, port: Option<u16>) -> DomainMatcher {
            let mut domain = host.to_lowercase();
            // We ensure that the domain starts with a `.`, as in
            // `.materialize.com`, so that we can match `re.materialize.com` but
            // not `rematerialize.com`.
            if !domain.starts_with('.') {
                domain.insert(0, '.')
            }
            DomainMatcher { domain, port }
        }

        pub fn matches(&self, host: &str, port: u16) -> bool {
            debug_assert_eq!(&self.domain[0..1], ".");
            (host == &self.domain[1..] || host.ends_with(&self.domain))
                && (self.port.is_none() || self.port == Some(port))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use http::Uri;

    use super::NoProxy;

    #[test]
    fn test_no_proxy() {
        struct TestCase {
            no_proxy: &'static str,
            matches: &'static [&'static str],
            nonmatches: &'static [&'static str],
        }

        let test_cases = &[
            // Adapted from Python's urllib.
            TestCase {
                no_proxy: "localhost, anotherdomain.com, newdomain.com:1234, .d.o.t",
                matches: &[
                    "http://localhost",
                    "http://LocalHost",
                    "http://LOCALHOST",
                    "http://newdomain.com:1234",
                    "http://foo.d.o.t",
                    "http://d.o.t",
                    "http://anotherdomain.com:8888",
                    "http://www.newdomain.com:1234",
                ],
                nonmatches: &[
                    "http://prelocalhost",
                    "http://newdomain.com",
                    "http://newdomain.com:1235",
                ],
            },
            // Adapted from Go's golang.org/x/net/http/httpproxy package.
            TestCase {
                no_proxy: "foobar.com, .barbaz.net, \
                           192.168.1.1, 192.168.1.2:81, 192.168.1.3:80, 10.0.0.0/30, \
                           2001:db8::52:0:1, [2001:db8::52:0:2]:443, [2001:db8::52:0:3]:80, \
                           2002:db8:a::45/64",
                matches: &[
                    "http://192.168.1.1",
                    "http://192.168.1.3",
                    "http://10.0.0.2",
                    "http://[2001:db8::52:0:1]",
                    "http://[2001:db8::52:0:3]",
                    "http://[2002:db8:a::123]",
                    "http://www.barbaz.net",
                    "http://barbaz.net",
                    "http://foobar.com",
                    "http://www.foobar.com",
                ],
                nonmatches: &[
                    "http://192.168.1.2",
                    "http://192.168.1.4",
                    "http://[2001:db8::52:0:2]",
                    "http://[fe80::424b:c8be:1643:a1b6]",
                    "http://foofoobar.com",
                    "http://baz.com",
                    "http://localhost.net",
                    "http://local.localhost",
                    "http://barbarbaz.net",
                ],
            },
            // SSL port inference.
            TestCase {
                no_proxy: "example.com:443",
                matches: &["https://example.com"],
                nonmatches: &["http://example.com"],
            },
            // Wildcards. Also adapted from Python's urllib.
            TestCase {
                no_proxy: "*",
                matches: &["http://newdomain.com", "http://newdomain.com:1234"],
                nonmatches: &[],
            },
            TestCase {
                no_proxy: "*, anotherdomain.com",
                matches: &["http://anotherdomain.com"],
                nonmatches: &["http://newdomain.com", "http://newdomain.com:1234"],
            },
            // Empty entries.
            TestCase {
                no_proxy: ",  , []",
                matches: &[],
                nonmatches: &["http://anydomain.com"],
            },
            // IPv4-mapped IPv6 addresses.
            TestCase {
                no_proxy: "::ffff:192.168.1.1",
                matches: &["http://192.168.1.1", "http://[::ffff:192.168.1.1]"],
                nonmatches: &["http://192.168.1.2", "http://[::ffff:192.168.1.2]"],
            },
            TestCase {
                no_proxy: "192.168.1.1",
                matches: &["http://192.168.1.1", "http://[::ffff:192.168.1.1]"],
                nonmatches: &["http://192.168.1.2", "http://[::ffff:192.168.1.2]"],
            },
        ];

        for test_case in test_cases {
            let no_proxy = NoProxy::parse(test_case.no_proxy);

            for uri in test_case.matches {
                let uri = Uri::from_str(uri).unwrap();
                assert!(
                    no_proxy.matches(uri.scheme_str(), uri.host(), uri.port_u16()),
                    "no_proxy '{}' did not match '{}' as expected",
                    test_case.no_proxy,
                    uri,
                );
            }

            for uri in test_case.nonmatches {
                let uri = Uri::from_str(uri).unwrap();
                assert!(
                    !no_proxy.matches(uri.scheme_str(), uri.host(), uri.port_u16()),
                    "no_proxy '{}' unexpectedly matched '{}'",
                    test_case.no_proxy,
                    uri,
                );
            }
        }
    }
}
