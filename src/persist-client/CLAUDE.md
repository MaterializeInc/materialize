# mz-persist-client

`mz-persist-client` builds on `mz-persist` to provide the persist state
machine: readers, writers, compaction, shard state, `Machine`, `BatchBuilder`,
etc.

## Abstraction boundaries

Key-format knowledge belongs here, not in `mz-persist`:

- `WriterKey` prefixes, `PartialBatchKey` structure, shard key conventions
- Routing logic that inspects keys to decide behavior (e.g., which blob tier
  to use for a given write)
- Source-specific or compaction-specific terminology

When adding infrastructure that spans both crates, push the generic mechanism
down to `mz-persist` and keep the policy/routing decision here. For example,
a blob wrapper that routes writes to different tiers lives in `mz-persist`,
but the closure that decides "this key is latency-sensitive" is defined here.
