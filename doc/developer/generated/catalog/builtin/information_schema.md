---
source: src/catalog/src/builtin/information_schema.rs
revision: 36df04b4d9
---

# catalog::builtin::information_schema

Defines all built-in catalog objects for the `information_schema` SQL schema.

This module contains 14 `BuiltinView` definitions exported as `pub static LazyLock` values. Each view implements a standard SQL information schema relation using queries over `mz_catalog` system tables. The views exposed include:

- `applicable_roles` — roles applicable to the current session user, joining `mz_role_members` and `mz_roles`.
- `columns` — column metadata for all user-visible relations.
- `enabled_roles` — roles currently enabled for the session.
- `key_column_usage` — columns that are part of table constraints.
- `referential_constraints` — foreign key constraint metadata.
- `role_table_grants` — table privileges granted to roles.
- `routines` — stub view for function/procedure metadata.
- `schemata` — all schemas visible to the current role.
- `table_constraints` — primary key and unique constraints.
- `table_privileges` — privilege grants on tables and views.
- `tables` — all tables and views visible to the current role.
- `triggers` — stub view for trigger metadata (always empty).
- `views` — views and materialized views visible to the current role.
- `character_sets` — character set metadata (returns a single UTF-8 row).

All views grant `PUBLIC_SELECT` access.
