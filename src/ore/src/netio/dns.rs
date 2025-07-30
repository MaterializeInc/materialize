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
use std::net::IpAddr;

use tokio::net::lookup_host;

/// We must provide a port for dns resolution but
/// dns resolution ignores the port so we use a port
/// that is easy to spot in logs.
pub const DUMMY_DNS_PORT: u16 = 11111;

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
    // `net::lookup_host` requires a port to be specified, but we don't care about the port.
    let mut port = DUMMY_DNS_PORT;
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

fn is_global(addr: IpAddr) -> bool {
    // TODO: Switch to `addr.is_global()` once stable: https://github.com/rust-lang/rust/issues/27709
    match addr {
        IpAddr::V4(ip) => {
            !(ip.is_unspecified() || ip.is_private() || ip.is_loopback() || ip.is_link_local())
        }
        IpAddr::V6(ip) => !(ip.is_loopback() || ip.is_unspecified()),
    }
}
