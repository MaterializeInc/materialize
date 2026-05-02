---
source: src/pgtz/src/timezone.rs
revision: 4267863081
---

# mz-pgtz::timezone

Defines the `Timezone` enum (either a `FixedOffset` or an IANA `Tz`) and `Timezone::parse`, which tokenizes a timezone string and resolves it against known abbreviations, IANA names, and numeric offset formats.
Parsing supports ISO 8601 and POSIX offset conventions (which invert the sign of numeric offsets); accepted formats range from `+HH`, `+HHMM`, `+HH:MM:SS` to bare names like `UTC` or `America/New_York`.
`MZ_CATALOG_TIMEZONE_NAMES_SQL` exposes the full IANA name list for the `mz_timezone_names` catalog view.
