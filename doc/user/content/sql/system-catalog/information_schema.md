---
title: "information_schema"
description: "information_schema is a system catalog that presents metadata in the format used by SQL standard."
menu:
  main:
    parent: 'system-catalog'
    weight: 3
---

Materialize has compatibility shims for the following relations from the
SQL standard [`information_schema`](https://www.postgresql.org/docs/current/infoschema-schema.html)
schema, which is automatically available in all databases:

  * [`applicable_roles`](https://www.postgresql.org/docs/current/infoschema-applicable-roles.html)
  * [`character_sets`](https://www.postgresql.org/docs/current/infoschema-character-sets.html)
  * [`columns`](https://www.postgresql.org/docs/current/infoschema-columns.html)
  * [`enabled_roles`](https://www.postgresql.org/docs/current/infoschema-enabled-roles.html)
  * [`key_column_usage`](https://www.postgresql.org/docs/current/infoschema-key-column-usage.html)
  * [`referential_constraints`](https://www.postgresql.org/docs/current/infoschema-referential-constraints.html)
  * [`role_table_grants`](https://www.postgresql.org/docs/current/infoschema-role-table-grants.html)
  * [`routines`](https://www.postgresql.org/docs/current/infoschema-routines.html)
  * [`schemata`](https://www.postgresql.org/docs/current/infoschema-schemata.html)
  * [`tables`](https://www.postgresql.org/docs/current/infoschema-tables.html)
  * [`table_constraints`](https://www.postgresql.org/docs/current/infoschema-table-constraints.html)
  * [`table_privileges`](https://www.postgresql.org/docs/current/infoschema-table-privileges.html)
  * [`triggers`](https://www.postgresql.org/docs/current/infoschema-triggers.html)
  * [`views`](https://www.postgresql.org/docs/current/infoschema-views.html)

These compatibility shims are largely incomplete. Most are lacking some columns
that are present in the SQL standard, or if they do include the column the
result set its value may always be `NULL`. The precise nature of the
incompleteness is intentionally undocumented. New tools developed against
Materialize should use the documented [`mz_catalog`](../mz_catalog) API instead.
