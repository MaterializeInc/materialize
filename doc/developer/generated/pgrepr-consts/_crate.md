---
source: src/pgrepr-consts/src/lib.rs
revision: 30d929249e
---

# mz-pgrepr-consts

Provides a single, stable registry of OID constants for all PostgreSQL and Materialize-specific data types, functions, operators, roles, schemas, and catalog objects used in Materialize's PostgreSQL-compatible wire protocol and catalog.

## Module structure

* `oid` — the only module; contains every OID constant organized into PostgreSQL builtin types/functions, Materialize-specific builtins (starting at OID 16384), and boundary sentinels (`FIRST_UNPINNED_OID`, `FIRST_MATERIALIZE_OID`, `FIRST_USER_OID`).

## Key dependencies and consumers

The crate has no runtime dependencies beyond the `workspace-hack` feature shim.
Downstream consumers include crates that implement the PostgreSQL representation layer (e.g., `mz-pgrepr`) and the catalog (e.g., `mz-catalog`), which reference these constants to map Materialize types and built-in objects to stable OIDs.
