---
source: src/persist-client/src/critical.rs
revision: 901d0526a1
---

# persist-client::critical

Defines `CriticalReaderId` (a stable UUID-based reader identity that survives process restarts) and `SinceHandle`, the capability handle that allows a critical reader to durably hold the shard's since frontier down.
Unlike leased readers, critical readers do not expire on heartbeat timeout; their since hold is associated with an `Opaque` token that must be matched to prevent races on compare-and-set downgrade.
