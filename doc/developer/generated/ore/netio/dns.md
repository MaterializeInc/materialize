---
source: src/ore/src/netio/dns.rs
revision: 98469193f5
---

# mz-ore::netio::dns

Provides `resolve_address`, an async function that resolves a hostname to a set of IP addresses and optionally rejects private/non-global addresses, and `ensure_url_ip_global`, a synchronous helper that validates that a URL's IP-literal host is globally routable (closing the gap that custom DNS resolvers are bypassed for IP literals).
Defines `DnsResolutionError` with variants for private addresses, no addresses found, and underlying I/O errors.
Used when connecting to user-supplied hostnames where routing to private infrastructure must be prevented.
