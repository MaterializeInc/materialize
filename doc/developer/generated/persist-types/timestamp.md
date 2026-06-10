---
source: src/persist-types/src/timestamp.rs
revision: 7c315f6a44
---

# persist-types::timestamp

Provides `try_parse_monotonic_iso8601_timestamp`, which parses a strict `YYYY-MM-DDTHH:MM:SS.mmmZ` timestamp string into a `NaiveDateTime`.
The format is designed so that lexicographic ordering of the string matches chronological ordering, enabling string-based pushdown on JSON timestamp columns.
