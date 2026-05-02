---
source: src/pgrepr-consts/src/oid.rs
revision: 886e88a1bb
---

# mz-pgrepr-consts::oid

Defines all PostgreSQL OID constants used across Materialize.
The file is organized into four groups: a sentinel `INVALID_OID` (0), standard PostgreSQL builtin type OIDs (e.g., `TYPE_BOOL_OID`, `TYPE_TEXT_OID`, range types), a set of PostgreSQL builtin function OIDs assigned from the unpinned range starting at 12000, and a large block of Materialize-specific OIDs starting at `FIRST_MATERIALIZE_OID` (16384) covering custom types (`TYPE_LIST_OID`, `TYPE_MAP_OID`, `TYPE_UINT*_OID`, `TYPE_MZ_TIMESTAMP_OID`, `TYPE_MZ_ACL_ITEM_OID`), functions, operators, roles, schemas, catalog tables, views, indexes, and sources.
Three boundary sentinels — `FIRST_UNPINNED_OID` (12000), `FIRST_MATERIALIZE_OID` (16384), and `FIRST_USER_OID` (20000) — mark the transitions between the PostgreSQL pinned range, the Materialize builtin range, and the user-object range respectively.
`FUNC_REPEAT_OID` (16413) is named `FUNC_REPEAT_ROW_OID`; `FUNC_REPEAT_ROW_NON_NEGATIVE_OID` (17075) is a separate constant for the non-negative repeat variant. `ROLE_MZ_JWT_SYNC_OID` (17076) is the OID for the `mz_jwt_sync` system role.
