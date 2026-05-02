---
source: src/persist-client/src/cli/inspect.rs
revision: 5f785f23fd
---

# persist-client::cli::inspect

Implements read-only CLI introspection commands: dumping shard state (consensus diffs, rollups, batch parts, since/upper), scanning blob keys, and pretty-printing proto-encoded state.
All commands operate without modifying any persistent state.
