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
  * [`columns`](https://www.postgresql.org/docs/current/infoschema-columns.html)
  * [`enabled_roles`](https://www.postgresql.org/docs/current/infoschema-enabled-roles.html)
  * [`routines`](https://www.postgresql.org/docs/current/infoschema-routines.html)
  * [`schemata`](https://www.postgresql.org/docs/current/infoschema-schemata.html)
  * [`tables`](https://www.postgresql.org/docs/current/infoschema-tables.html)
  * [`triggers`](https://www.postgresql.org/docs/current/infoschema-triggers.html)
  * [`views`](https://www.postgresql.org/docs/current/infoschema-views.html)

These compatibility shims are largely incomplete. Most are lacking some columns
that are present in the SQL standard, or if they do include the column the
result set its value may always be `NULL`. The precise nature of the
incompleteness is intentionally undocumented. New tools developed against
Materialize should use the documented [`mz_catalog`](../mz_catalog) API instead.
