---
source: src/adapter/src/coord/introspection.rs
revision: 473c1bde8a
---

# adapter::coord::introspection

Implements the auto-routing logic for introspection queries: `auto_run_on_catalog_server` detects when a SELECT exclusively reads from per-replica introspection log sources and rewrites the target cluster to `mz_catalog_server` to avoid requiring a user cluster.
Also provides `user_privilege_hack` which grants temporary introspection privileges to the session when accessing per-replica logs.
