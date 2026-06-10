---
source: src/persist-client/src/critical.rs
revision: b33ffcb977
---

# persist-client::critical

Defines `CriticalReaderId` (a stable UUID-based reader identity that survives process restarts), `Opaque` (a fencing token encoding a codec name and 8-byte value), and `SinceHandle`, the capability handle that allows a critical reader to durably hold the shard's since frontier down.
Unlike leased readers, critical readers do not expire on heartbeat timeout; their since hold persists until explicitly downgraded.
`SinceHandle` uses `compare_and_downgrade_since` with the `Opaque` token to prevent races when multiple processes attempt to advance the since.
Users are advised to durably record the `CriticalReaderId` before registration, since a lost handle permanently prevents the shard's since from advancing.
