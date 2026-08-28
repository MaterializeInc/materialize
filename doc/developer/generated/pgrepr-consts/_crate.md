---
source: src/pgrepr-consts/src/lib.rs
revision: c317ceee3c
---

# mz-pgrepr-consts

Provides a single, stable registry of OID constants for all PostgreSQL and Materialize-specific data types, functions, operators, roles, schemas, and catalog objects used in Materialize's PostgreSQL-compatible wire protocol and catalog.

## Module structure

* `oid` — every OID constant organized into PostgreSQL builtin types/functions, Materialize-specific builtins (starting at OID 16384), and boundary sentinels (`FIRST_UNPINNED_OID`, `FIRST_MATERIALIZE_OID`, `FIRST_USER_OID`).
* `regproc` — static OID-to-name table (`NAMES`) for text rendering of `regproc` values, with `name(oid)` and `oid(name)` lookup functions. The table covers all builtin function OIDs and is generated from the builtin function registry via `REWRITE=1 cargo test -p mz-catalog test_regproc_names_match_builtin_functions`; do not hand-edit entries.

## Key dependencies and consumers

The crate has no runtime dependencies.
Downstream consumers include crates that implement the PostgreSQL representation layer (e.g., `mz-pgrepr`) and the catalog (e.g., `mz-catalog`), which reference these constants to map Materialize types and built-in objects to stable OIDs.
