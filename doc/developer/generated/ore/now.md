---
source: src/ore/src/now.rs
revision: bc3258f4d1
---

# mz-ore::now

Defines the `EpochMillis` type alias (`u64`), the `AsEpochMillis` trait (implemented only for types whose values represent milliseconds since the Unix epoch, enabling meaningful comparison against a `NowFn`), and `NowFn<T>`, a clonable, thread-safe wrapper around a `Fn() -> T` closure used to abstract over real and mocked time sources.
Provides two static instances: `SYSTEM_TIME` (returns the current wall-clock time in milliseconds since the Unix epoch) and `NOW_ZERO` (always returns zero, for testing).
Also includes `to_datetime` (converts epoch milliseconds to `chrono::DateTime<Utc>`, gated on the `chrono` feature) and `epoch_to_uuid_v7` (converts an `EpochMillis` to a UUID v7, gated on the `id_gen` feature).
