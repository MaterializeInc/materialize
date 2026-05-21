# mz-persist

`mz-persist` provides storage abstractions (`Blob`, `Consensus`) and generic
wrappers over them. It is the lower-level persist crate.

## Abstraction boundaries

This crate does **not** know about its callers. It should never reference:

- `WriterKey`, `ShardId` key formats, `PartialBatchKey` structure
- Sources, sinks, compaction, shard state, the persist state machine
- How callers configure or wire up the abstractions defined here

When describing write patterns, use generic terminology — "latency-sensitive"
vs "background" writes — not "source writes" vs "compaction writes."

## Wrappers should accept injected behavior

Wrappers like `TieredStorageBlob` take closures or trait objects to make
routing decisions. Don't hardcode higher-level logic (e.g., parsing key
prefixes to decide routing). The caller provides the policy; the wrapper
provides the mechanism.

Similarly, doc comments on wrappers should describe what the wrapper *does*,
not how callers configure or deploy it — that's an abstraction leak.
