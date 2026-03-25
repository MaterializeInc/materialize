---
source: src/foundationdb/src/lib.rs
revision: 4a1aeff959
---

# mz-foundationdb

Thin wrapper around the `foundationdb` crate that adds safe, process-global network lifecycle management and URL-based configuration parsing.
`init_network` and `shutdown_network` guard the unsafe `boot()` call with a mutex so the FDB network is initialized at most once per process; re-initialization after shutdown panics.
`FdbConfig::parse` decodes a `foundationdb:?prefix=…` URL into a directory-layer prefix vector, with backward-compatible support for the deprecated `options=--search_path=…` form.
