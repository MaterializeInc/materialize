---
source: src/repr/src/adt/timestamp.rs
revision: 26305d8cb0
---

# mz-repr::adt::timestamp

Defines `CheckedTimestamp<T>` wrapping `chrono::NaiveDateTime` or `chrono::DateTime<Utc>` with validation against PostgreSQL timestamp bounds (`LOW_DATE`/`HIGH_DATE`), and `TimestampPrecision` for the optional sub-second scale.
