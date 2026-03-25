---
source: src/proto/src/chrono.rs
revision: a6d6c38fee
---

# mz-proto::chrono

Implements `RustType` conversions between chrono datetime types (`NaiveTime`, `NaiveDateTime`, `DateTime<Utc>`, `FixedOffset`, `chrono_tz::Tz`) and their generated Protobuf representations (`ProtoNaiveTime`, `ProtoNaiveDateTime`, `ProtoFixedOffset`, `ProtoTz`).
Also provides proptest strategies (`any_naive_date`, `any_naive_datetime`, `any_datetime`, `any_fixed_offset`, `any_timezone`) for generating arbitrary chrono values in property-based tests.
Only compiled when the `chrono` feature flag is enabled.
