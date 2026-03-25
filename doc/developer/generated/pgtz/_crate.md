---
source: src/pgtz/src/lib.rs
revision: 30d929249e
---

# mz-pgtz

Provides PostgreSQL-compatible timezone parsing and the data needed to populate the `mz_timezone_abbreviations` and `mz_timezone_names` catalog views.
The crate resolves timezone strings (numeric offsets, IANA names, and PostgreSQL abbreviations such as `EST`) to Materialize's `Timezone` type, following the same precedence rules as PostgreSQL.

## Module structure

* `abbrev` — compile-time-generated abbreviation table (`TIMEZONE_ABBREVS`) and the SQL for `mz_timezone_abbreviations`.
* `timezone` — `Timezone` enum, `Timezone::parse`, tokenizer, and the SQL for `mz_timezone_names`.

## Key dependencies

* `chrono` / `chrono-tz` — UTC offset arithmetic and IANA timezone database.
* `phf` — perfect-hash table used for the generated abbreviation lookup.
* `uncased` — case-insensitive string comparison for abbreviation and name lookup.
* `mz-lowertest` — `MzReflect` derive used on `Timezone`.
