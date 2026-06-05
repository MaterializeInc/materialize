---
source: src/repr/src/timestamp.rs
revision: 225aeaa79f
---

# mz-repr::timestamp

Defines `Timestamp`, Materialize's system-wide timestamp type (a `u64` milliseconds-since-epoch value), implementing `timely::progress::Timestamp`, differential `Lattice`, and `TimestampManipulation` for step-forward operations.
Includes protobuf-generated code for timestamp serialization and the `BucketTimestamp` implementation for temporal bucketing.
