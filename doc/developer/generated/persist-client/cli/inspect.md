---
source: src/persist-client/src/cli/inspect.rs
revision: 901d0526a1
---

# persist-client::cli::inspect

Implements read-only CLI introspection commands: dumping shard state (consensus diffs, rollups, batch parts, since/upper), scanning blob keys, and pretty-printing proto-encoded state.
All commands operate without modifying any persistent state.
