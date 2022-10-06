---
title: "pg_catalog"
description: "pg_catalog is a system catalog that presents metadata in the format used by PostgreSQL."
menu:
  main:
    parent: 'system-catalog'
    weight: 2
---

Materialize has compatibility shims for the following relations from [PostgreSQL's
system catalog](https://www.postgresql.org/docs/current/catalogs.html):

  * [`pg_am`](https://www.postgresql.org/docs/current/catalog-pg-am.html)
  * [`pg_attribute`](https://www.postgresql.org/docs/current/catalog-pg-attribute.html)
  * [`pg_class`](https://www.postgresql.org/docs/current/catalog-pg-class.html)
  * [`pg_collation`](https://www.postgresql.org/docs/current/catalog-pg-collation.html)
  * [`pg_constraint`](https://www.postgresql.org/docs/current/catalog-pg-constraint.html)
  * [`pg_database`](https://www.postgresql.org/docs/current/catalog-pg-database.html)
  * [`pg_description`](https://www.postgresql.org/docs/current/catalog-pg-description.html)
  * [`pg_enum`](https://www.postgresql.org/docs/current/catalog-pg-enum.html)
  * [`pg_index`](https://www.postgresql.org/docs/current/catalog-pg-index.html)
  * [`pg_inherits`](https://www.postgresql.org/docs/current/catalog-pg-inherits.html)
  * [`pg_matviews`](https://www.postgresql.org/docs/current/view-pg-matviews.html)
  * [`pg_namespace`](https://www.postgresql.org/docs/current/catalog-pg-namespace.html)
  * [`pg_policy`](https://www.postgresql.org/docs/current/catalog-pg-policy.html)
  * [`pg_proc`](https://www.postgresql.org/docs/current/catalog-pg-proc.html)
  * [`pg_range`](https://www.postgresql.org/docs/current/catalog-pg-range.html)
  * [`pg_roles`](https://www.postgresql.org/docs/current/view-pg-roles.html)
  * [`pg_settings`](https://www.postgresql.org/docs/current/view-pg-settings.html)
  * [`pg_tables`](https://www.postgresql.org/docs/current/view-pg-tables.html)
  * [`pg_type`](https://www.postgresql.org/docs/current/catalog-pg-type.html)
  * [`pg_views`](https://www.postgresql.org/docs/current/view-pg-views.html)
  * [`pg_authid`](https://www.postgresql.org/docs/current/catalog-pg-authid.html)

These compatibility shims are largely incomplete. Most are lacking some columns
that are present in PostgreSQL, or if they do include the column the result set
its value may always be `NULL`. The precise nature of the incompleteness is
intentionally undocumented. New tools developed against Materialize should use
the documented [`mz_catalog`](../mz_catalog) API instead.

If you are having trouble making a PostgreSQL tool work with Materialize, please
[file a GitHub issue][gh-issue]. Many PostgreSQL tools can be made to work with
Materialize with minor changes to the `pg_catalog` compatibility shim.

[gh-issue]: https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md
