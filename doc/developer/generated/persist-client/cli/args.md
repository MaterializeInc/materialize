---
source: src/persist-client/src/cli/args.rs
revision: b89a9e0ec5
---

# persist-client::cli::args

Defines shared CLI argument types (`StoreArgs`, `StateArgs`) and helper functions (`make_blob`, `make_consensus`) used by all persist CLI subcommands to connect to blob and consensus storage.
`StateArgs` combines store connection details with a `ShardId` and constructs a `StateVersions` for reading shard metadata.
Both helpers accept a `commit` flag; when `false`, writes are intercepted by a `ReadOnly` wrapper that logs and discards them, preventing accidental mutations during inspection.
