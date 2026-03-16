---
source: src/testdrive/src/action/persist.rs
revision: db271c31b1
---

# testdrive::action::persist

Implements the `persist-force-compaction` builtin command, which directly calls the persist client to compact a named shard.
Requires the `persist-consensus` and `persist-blob` URLs to be configured in the testdrive state; used in tests that need to verify compaction behavior without waiting for the normal background GC cycle.
