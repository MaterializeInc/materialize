---
source: src/persist-client/src/cli/admin.rs
revision: b95afe8b5c
---

# persist-client::cli::admin

Implements administrative CLI commands that can mutate persist state, such as triggering compaction on a shard, force-expiring readers or writers, and managing since/upper advancement.
Commands are gated on an explicit `--commit` flag to prevent accidental writes in inspection workflows.
`info_log_non_zero_metrics` formats metric label sets as `{name:value, ...}` strings for log output.
