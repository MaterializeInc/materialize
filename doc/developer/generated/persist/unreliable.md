---
source: src/persist/src/unreliable.rs
revision: 4a1aeff959
---

# persist::unreliable

Test utility that wraps `Blob` and `Consensus` implementations and probabilistically injects failures and timeouts using a configurable RNG.
`UnreliableHandle` holds `should_happen` and `should_timeout` probabilities that can be changed at runtime to drive chaos scenarios.
Both `UnreliableBlob` and `UnreliableConsensus` delegate to the underlying implementation only when the random draw succeeds.
