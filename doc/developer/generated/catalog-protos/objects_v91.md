---
source: src/catalog-protos/src/objects_v91.rs
revision: f3b4f3f
---

# mz-catalog-protos::objects_v91

Frozen snapshot of catalog object type definitions at schema version 91, identical to `objects.rs` at the time v91 was declared current.
This snapshot serves as the migration target for v90->v91 upgrades and as the migration source for v91->v92.

Relative to v90, this snapshot adds an `ephemeral_owner_session: Option<Uuid>` field to `ItemValue`:

- `ItemValue` — `Some(uuid)` marks a temporary item owned by and only visible to the session with that UUID. `None` denotes a normal durable item.

This field is backfilled as `None` for all existing records by the v90->v91 migration (`catalog::durable::upgrade::v90_to_v91`). All other catalog types are identical to v90.

`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
