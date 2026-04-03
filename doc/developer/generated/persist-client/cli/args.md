---
source: src/persist-client/src/cli/args.rs
revision: 181b1e7efc
---

# persist-client::cli::args

Defines shared CLI argument types (`StoreArgs`, `StateArgs`) and helper functions (`make_blob`, `make_consensus`) used by all persist CLI subcommands to connect to blob and consensus storage.
`StateArgs` combines store connection details with a `ShardId` and constructs a `StateVersions` for reading shard metadata.
