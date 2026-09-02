---
source: src/clusterd-test-driver/src/persist_host.rs
revision: 6d4c0fbb2b
---

# mz-clusterd-test-driver::persist_host

Hosts persist infrastructure for the headless test driver: a PubSub gRPC server and a `PersistClientCache` wired to it.

`PersistHost` holds an `Arc<PersistClientCache>`, the PubSub server's port, and a `PersistLocation`. `clusterd` connects to `pubsub_url()` (which returns the gRPC URL for the embedded PubSub server).

`start(location)` binds to an ephemeral localhost port (for unit tests). `start_on(addr, location)` binds to a specific address (for integration tests where clusterd is launched externally and needs a known port). `client()` returns a `PersistClient` from the cache. `location()` returns the `PersistLocation` for injecting into clusterd's `--scratch-directory` equivalent.
