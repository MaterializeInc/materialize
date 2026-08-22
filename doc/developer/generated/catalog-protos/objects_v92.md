---
source: src/catalog-protos/src/objects_v92.rs
revision: 39dcae2fba
---

# mz-catalog-protos::objects_v92

Frozen snapshot of catalog object type definitions at schema version 92, identical to `objects.rs` at the time v92 was declared current.
This snapshot serves as the migration target for v91->v92 upgrades and as the migration source for v92->v93.

Relative to v91, this snapshot adds a `MetricSink` variant to four existing enums:

- `CatalogItemType` — new `MetricSink` variant.
- `ObjectType` — new `MetricSink` variant.
- `CommentObject` — new `MetricSink` variant.
- Audit log `ObjectType` — new `MetricSink` variant.

Metric sinks are stored as ordinary `Item` records; `durable::objects::item_type` recovers the type from `create_sql`. No new top-level record type is introduced, and no existing record is affected by the new variants, so the v91->v92 migration (`catalog::durable::upgrade::v91_to_v92`) is a no-op.

`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
