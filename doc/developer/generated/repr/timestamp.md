---
source: src/repr/src/timestamp.rs
revision: c642b63c77
---

# mz-repr::timestamp

Defines `Timestamp`, Materialize's system-wide timestamp type (a `u64` milliseconds-since-epoch value), implementing `timely::progress::Timestamp`, differential `Lattice`, and `TimestampManipulation` for step-forward operations.
Includes protobuf-generated code for timestamp serialization and the `BucketTimestamp` implementation for temporal bucketing.
