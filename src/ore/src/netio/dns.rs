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

use std::collections::BTreeSet;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::LazyLock;

use ipnet::{Ipv4Net, Ipv6Net};
use tokio::net::lookup_host;

/// An error returned by `resolve_address`.
#[derive(thiserror::Error, Debug)]
pub enum DnsResolutionError {
    /// private ip
    #[error(
        "Address resolved to a private IP. The provided host is not routable on the public internet"
    )]
    PrivateAddress,
    /// no addresses
    #[error("Address did not resolve to any IPs")]
    NoAddressesFound,
    /// io error
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Resolves a host address and ensures it is a global address when `enforce_global` is set.
/// This parameter is useful when connecting to user-defined unverified addresses.
pub async fn resolve_address(
    mut host: &str,
    enforce_global: bool,
) -> Result<BTreeSet<IpAddr>, DnsResolutionError> {
    let mut port = 0;
    // If a port is already specified, use it and remove it from the host.
    if let Some(idx) = host.find(':') {
        if let Ok(p) = host[idx + 1..].parse() {
            port = p;
            host = &host[..idx];
        }
    }

    let mut addrs = lookup_host((host, port)).await?;
    let mut ips = BTreeSet::new();
    while let Some(addr) = addrs.next() {
        let ip = addr.ip();
        if enforce_global && !is_global(ip) {
            Err(DnsResolutionError::PrivateAddress)?
        } else {
            ips.insert(ip);
        }
    }

    if ips.len() == 0 {
        Err(DnsResolutionError::NoAddressesFound)?
    }
    Ok(ips)
}

/// If `url`'s host is an IP literal, validates that it is a globally routable
/// address, returning [`DnsResolutionError::PrivateAddress`] if not. Hostnames
/// and URLs without a host are passed through; they are expected to be
/// validated by a connector-level DNS resolver at connect time.
///
/// This closes the gap that reqwest/hyper custom DNS resolvers are only
/// invoked for hostnames, so IP-literal URLs (e.g. `http://127.0.0.1`) would
/// otherwise bypass global-address enforcement.
pub fn ensure_url_ip_global(url: &url::Url) -> Result<(), DnsResolutionError> {
    let ip = match url.host() {
        Some(url::Host::Ipv4(ip)) => IpAddr::V4(ip),
        Some(url::Host::Ipv6(ip)) => IpAddr::V6(ip),
        Some(url::Host::Domain(_)) | None => return Ok(()),
    };
    if is_global(ip) {
        Ok(())
    } else {
        Err(DnsResolutionError::PrivateAddress)
    }
}

/// IPv4 CIDR blocks that are not globally routable. Anything outside this set
/// is treated as a public address.
// TODO: Switch to `Ipv4Addr::is_global()` once stable:
// https://github.com/rust-lang/rust/issues/27709
static V4_NON_GLOBAL: LazyLock<Vec<Ipv4Net>> = LazyLock::new(|| {
    [
        "0.0.0.0/8",       // unspecified / "this network"
        "10.0.0.0/8",      // private (RFC 1918)
        "100.64.0.0/10",   // shared address space / CGNAT (RFC 6598)
        "127.0.0.0/8",     // loopback
        "169.254.0.0/16",  // link-local
        "172.16.0.0/12",   // private (RFC 1918)
        "192.0.0.0/24",    // IETF protocol assignments
        "192.0.2.0/24",    // documentation (TEST-NET-1)
        "192.168.0.0/16",  // private (RFC 1918)
        "198.18.0.0/15",   // benchmarking
        "198.51.100.0/24", // documentation (TEST-NET-2)
        "203.0.113.0/24",  // documentation (TEST-NET-3)
        "224.0.0.0/4",     // multicast
        "240.0.0.0/4",     // reserved (includes broadcast 255.255.255.255)
    ]
    .iter()
    .map(|s| s.parse().expect("valid CIDR"))
    .collect()
});

/// IPv6 CIDR blocks that are not globally routable.
// TODO: Switch to `Ipv6Addr::is_global()` once stable:
// https://github.com/rust-lang/rust/issues/27709
static V6_NON_GLOBAL: LazyLock<Vec<Ipv6Net>> = LazyLock::new(|| {
    [
        "::/128",    // unspecified
        "::1/128",   // loopback
        "fc00::/7",  // unique local
        "fe80::/10", // link-local
    ]
    .iter()
    .map(|s| s.parse().expect("valid CIDR"))
    .collect()
});

fn is_global(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(ip) => is_global_v4(ip),
        IpAddr::V6(ip) => is_global_v6(ip),
    }
}

fn is_global_v4(ip: Ipv4Addr) -> bool {
    !V4_NON_GLOBAL.iter().any(|net| net.contains(&ip))
}

fn is_global_v6(ip: Ipv6Addr) -> bool {
    // Treat IPv4-mapped IPv6 addresses (`::ffff:a.b.c.d`) as their underlying
    // IPv4 address — connecting to `::ffff:127.0.0.1` reaches loopback on
    // dual-stack sockets.
    if let Some(v4) = ip.to_ipv4_mapped() {
        return is_global_v4(v4);
    }
    !V6_NON_GLOBAL.iter().any(|net| net.contains(&ip))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Use IP literals so the tests don't depend on /etc/hosts or DNS.
    const PRIVATE_V4: &str = "127.0.0.1";
    const PUBLIC_V4: &str = "8.8.8.8";
    const LOOPBACK_V6: &str = "::1";

    #[crate::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolve_address_rejects_loopback_v4_when_enforced() {
        let err = resolve_address(PRIVATE_V4, true)
            .await
            .expect_err("loopback should be rejected");
        assert!(
            matches!(err, DnsResolutionError::PrivateAddress),
            "got {err:?}"
        );
    }

    #[crate::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolve_address_rejects_loopback_v6_when_enforced() {
        let err = resolve_address(LOOPBACK_V6, true)
            .await
            .expect_err("::1 should be rejected");
        assert!(
            matches!(err, DnsResolutionError::PrivateAddress),
            "got {err:?}"
        );
    }

    #[crate::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolve_address_allows_public_v4_when_enforced() {
        let ips = resolve_address(PUBLIC_V4, true)
            .await
            .expect("public IP should resolve");
        assert!(ips.contains(&PUBLIC_V4.parse::<IpAddr>().unwrap()));
    }

    #[crate::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolve_address_allows_loopback_when_not_enforced() {
        let ips = resolve_address(PRIVATE_V4, false)
            .await
            .expect("loopback should resolve when enforcement is off");
        assert!(ips.contains(&PRIVATE_V4.parse::<IpAddr>().unwrap()));
    }

    #[crate::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolve_address_rejects_ipv4_mapped_loopback() {
        let err = resolve_address("::ffff:127.0.0.1", true)
            .await
            .expect_err("IPv4-mapped loopback must be rejected");
        assert!(
            matches!(err, DnsResolutionError::PrivateAddress),
            "got {err:?}"
        );
    }

    #[crate::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolve_address_rejects_ipv6_unique_local() {
        let err = resolve_address("fc00::1", true)
            .await
            .expect_err("ULA must be rejected");
        assert!(
            matches!(err, DnsResolutionError::PrivateAddress),
            "got {err:?}"
        );
    }

    #[crate::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolve_address_rejects_ipv6_link_local() {
        let err = resolve_address("fe80::1", true)
            .await
            .expect_err("link-local must be rejected");
        assert!(
            matches!(err, DnsResolutionError::PrivateAddress),
            "got {err:?}"
        );
    }

    #[crate::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolve_address_rejects_ipv4_cgnat() {
        // 100.64.0.0/10 is the IETF shared-address-space range used for
        // carrier-grade NAT — `Ipv4Addr::is_global` rejects it.
        let err = resolve_address("100.64.0.1", true)
            .await
            .expect_err("CGNAT must be rejected");
        assert!(
            matches!(err, DnsResolutionError::PrivateAddress),
            "got {err:?}"
        );
    }

    #[crate::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolve_address_strips_port() {
        let ips = resolve_address(&format!("{PUBLIC_V4}:443"), true)
            .await
            .expect("host:port form should parse");
        assert!(ips.contains(&PUBLIC_V4.parse::<IpAddr>().unwrap()));
    }
}
