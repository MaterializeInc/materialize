---
source: src/persist-cli/src/maelstrom/services.rs
revision: 82d92a7fad
---

# persistcli::maelstrom::services

Provides persist `Blob` and `Consensus` implementations backed by the Maelstrom lin-kv service, enabling fully in-process Maelstrom testing without external storage.
`MaelstromConsensus` implements `Consensus` by encoding `VersionedData` as JSON and issuing lin-kv CaS operations; it caches seqno-to-data mappings to avoid extra head reads.
`MaelstromBlob` implements `Blob` using lin-kv reads/writes with base64-encoded byte values.
`CachingBlob` wraps any `Blob` and caches successful `get` responses (but not `set`) to reduce Maelstrom service calls.
`MaelstromOracle` and `MemTimestampOracle` provide `TimestampOracle` implementations: `MaelstromOracle` stores timestamps in lin-kv, while `MemTimestampOracle` is an in-memory fallback.
