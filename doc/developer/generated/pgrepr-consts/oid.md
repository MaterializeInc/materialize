---
source: src/pgrepr-consts/src/oid.rs
revision: 3df8ae2fd8
---

# mz-pgrepr-consts::oid

Defines all PostgreSQL OID constants used across Materialize.
The file is organized into four groups: a sentinel `INVALID_OID` (0), standard PostgreSQL builtin type OIDs (e.g., `TYPE_BOOL_OID`, `TYPE_TEXT_OID`, range types), a set of PostgreSQL builtin function OIDs assigned from the unpinned range starting at 12000, and a large block of Materialize-specific OIDs starting at `FIRST_MATERIALIZE_OID` (16384) covering custom types (`TYPE_LIST_OID`, `TYPE_MAP_OID`, `TYPE_UINT*_OID`, `TYPE_MZ_TIMESTAMP_OID`, `TYPE_MZ_ACL_ITEM_OID`), functions, operators, roles, schemas, catalog tables, views, indexes, and sources.
Three boundary sentinels — `FIRST_UNPINNED_OID` (12000), `FIRST_MATERIALIZE_OID` (16384), and `FIRST_USER_OID` (20000) — mark the transitions between the PostgreSQL pinned range, the Materialize builtin range, and the user-object range respectively.
`FUNC_REPEAT_OID` (16413) is named `FUNC_REPEAT_ROW_OID`; `FUNC_REPEAT_ROW_NON_NEGATIVE_OID` (17075) is a separate constant for the non-negative repeat variant. `ROLE_MZ_JWT_SYNC_OID` (17076) is the OID for the `mz_jwt_sync` system role. Four ontology view OIDs follow: `VIEW_MZ_ONTOLOGY_ENTITY_TYPES_OID` (17077), `VIEW_MZ_ONTOLOGY_SEMANTIC_TYPES_OID` (17078), `VIEW_MZ_ONTOLOGY_PROPERTIES_OID` (17079), and `VIEW_MZ_ONTOLOGY_LINK_TYPES_OID` (17080). Later entries include `SOURCE_MZ_OBJECT_ARRANGEMENT_SIZES_OID` (17081), related arrangement-size index and table OIDs (17082–17085), and `FUNC_MZ_SESSION_ROLE_MEMBERSHIPS_OID` (17086). `VIEW_MZ_MCP_DATA_PRODUCTS_OID` (17066) and `VIEW_MZ_MCP_DATA_PRODUCT_DETAILS_OID` (17071) are the OIDs for the MCP-agent data-product views that are exempted from `restrict_to_user_objects` blocking.
