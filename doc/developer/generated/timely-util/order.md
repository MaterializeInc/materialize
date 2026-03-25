---
source: src/timely-util/src/order.rs
revision: 4267863081
---

# timely-util::order

Defines `Partitioned<P, T>`, a timely `Timestamp` that represents the product order of an `Interval<P>` and an inner timestamp `T`, enabling efficient frontier representation when data is partitioned across an ordered key space (e.g., Kafka partitions keyed by offset or UUID).
`Interval<P>` captures an inclusive range of partition keys ordered by the subset relation; `Partitioned` exposes `split` for dividing a range at a point.
Also defines `Extrema` and `Step` traits (with implementations for `u64`, `i32`, `Uuid`), a `Reverse<T>` wrapper for maintaining maximum-inclusive antichains, and the `refine_antichain` utility for lifting an `Antichain<T>` into `Antichain<Inner>` via `Refines`.
