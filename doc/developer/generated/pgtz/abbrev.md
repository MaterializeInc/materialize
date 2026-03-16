---
source: src/pgtz/src/abbrev.rs
revision: e0d75f5c5e
---

# mz-pgtz::abbrev

Defines `TimezoneAbbrev` and `TimezoneAbbrevSpec`, which map a timezone abbreviation string (e.g., `EST`, `PDT`) to either a fixed UTC offset or a named IANA timezone entry.
The `TIMEZONE_ABBREVS` table is generated at build time via `abbrev.gen.rs` and `abbrev.gen.sql`; the SQL form is exposed as `MZ_CATALOG_TIMEZONE_ABBREVIATIONS_SQL` for populating the `mz_timezone_abbreviations` catalog view.
