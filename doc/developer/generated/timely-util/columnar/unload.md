---
source: src/timely-util/src/columnar/unload.rs
revision: a32ac9b3e9
---

# timely-util::columnar::unload

`UnloadChunk` and `UnloadBatch`: the bulk, copy-out interface for looking up a sorted set of keys in a chunk or batch.

## `UnloadChunk` trait

The chunk-level capability for probe-key extraction. Implementors choose opaque `Staging` and `Probes` types; the only key comparison the batch driver needs is delegated to the chunk via `locate`, which must be answerable from resident metadata (never loading a spilled body).

Key methods:

- **`locate(probes, probe_index) -> Ordering`** — where `probes[probe_index]` falls relative to this chunk's key span: `Less` before the first key, `Equal` within `[first, last]`, `Greater` past the last key. Resident metadata only.
- **`extract_into(probes, probe_index, staging)`** — appends this chunk's updates for probes at and after `*probe_index` into `staging`. Advances `*probe_index` past every probe strictly below this chunk's last key. A probe *equal* to the last key is extracted but not consumed (the straddle re-offer protocol: the probe's group may continue in the next chunk).
- **`fetch_into(staging)`** — appends the whole chunk into `staging` (the scan path).

The straddle re-offer invariant ensures that updates for a key spanning consecutive chunks are fully collected: the batch driver re-presents the unconsumed probe to the next chunk.

## `UnloadBatch` trait

The batch-level driver over `UnloadChunk`, implemented on `ChunkBatch<C>` as an extension trait (rather than inherent methods on the foreign type):

- **`extract_into(probes, staging)`** — gallops the chunk list using `locate` (resident metadata only) to skip chunks whose key span does not contain any probe, then calls `extract_into` only on chunks that do. Probes falling in the gap between two chunks' spans are consumed against resident metadata alone, so untouched chunk bodies remain unopened.
- **`fetch_into(staging)`** — calls `fetch_into` on every chunk in sequence (the scan path).

The gallop implements exponential search from the current chunk followed by binary search within the bracket, matching the access pattern of differential's merge operators.
