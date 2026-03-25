---
source: src/ore/src/netio/dns.rs
revision: 1c4808846b
---

# mz-ore::netio::dns

Provides `resolve_address`, an async function that resolves a hostname to a set of IP addresses and optionally rejects private/non-global addresses.
Defines `DnsResolutionError` with variants for private addresses, no addresses found, and underlying I/O errors.
Used when connecting to user-supplied hostnames where routing to private infrastructure must be prevented.
