---
source: src/pgrepr-consts/src/oid.rs
revision: d45bbaa9b4
---

# mz-pgrepr-consts::oid

Defines all PostgreSQL OID constants used across Materialize.
The file is organized into four groups: a sentinel `INVALID_OID` (0), standard PostgreSQL builtin type OIDs (e.g., `TYPE_BOOL_OID`, `TYPE_TEXT_OID`, range types), a set of PostgreSQL builtin function OIDs assigned from the unpinned range starting at 12000, and a large block of Materialize-specific OIDs starting at `FIRST_MATERIALIZE_OID` (16384) covering custom types (`TYPE_LIST_OID`, `TYPE_MAP_OID`, `TYPE_UINT*_OID`, `TYPE_MZ_TIMESTAMP_OID`, `TYPE_MZ_ACL_ITEM_OID`), functions, operators, roles, schemas, catalog tables, views, indexes, sources, and continual tasks.
Three boundary sentinels — `FIRST_UNPINNED_OID` (12000), `FIRST_MATERIALIZE_OID` (16384), and `FIRST_USER_OID` (20000) — mark the transitions between the PostgreSQL pinned range, the Materialize builtin range, and the user-object range respectively.
