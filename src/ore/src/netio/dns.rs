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

use std::io;
use std::net::IpAddr;

use tokio::net::lookup_host;

const DUMMY_PORT: u16 = 11111;

/// Resolves a host address and ensures it is a global address when `enforce_global` is set.
/// This parameter is useful when connecting to user-defined unverified addresses.
pub async fn resolve_address(
    mut host: &str,
    enforce_global: bool,
) -> Result<Vec<IpAddr>, io::Error> {
    // `net::lookup_host` requires a port to be specified, but we don't care about the port.
    let mut port = DUMMY_PORT;
    // If a port is already specified, use it and remove it from the host.
    if let Some(idx) = host.find(':') {
        if let Ok(p) = host[idx + 1..].parse() {
            port = p;
            host = &host[..idx];
        }
    }

    let mut addrs = lookup_host((host, port)).await?;
    let mut ips = Vec::new();
    while let Some(addr) = addrs.next() {
        let ip = addr.ip();
        if enforce_global && !is_global(ip) {
            Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "address is not global",
            ))?
        } else {
            ips.push(ip);
        }
    }

    if ips.len() == 0 {
        Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "no addresses found",
        ))?
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
