---
source: src/persist-client/src/cli/bench.rs
revision: 5680493e7d
---

# persist-client::cli::bench

Provides the `bench` CLI subcommand with a `s3fetch` subcommand that measures raw blob fetch throughput for a shard by repeatedly downloading all its parts.
Used for performance investigations and capacity planning.
