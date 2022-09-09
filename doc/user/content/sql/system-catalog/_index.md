---
title: "System catalog"
description: "The system catalog stores metadata about your Materialize instance."
aliases:
  - /sql/system-tables
menu:
  main:
    parent: reference
    name: System catalog
    identifier: 'system-catalog'
    weight: 160
disable_list: true
---

Materialize exposes a system catalog that contains metadata about the running
Materialize instance.

The system catalog consists of three schemas that are implicitly available in
all databases:

  * [`mz_catalog`](mz_catalog), which exposes metadata in Materialize's
    native format.

  * [`pg_catalog`](pg_catalog), which presents the data in `mz_catalog` in
    the format used by PostgreSQL.

  * [`information_schema`](information_schema), which presents the data in
    `mz_catalog` in the format used by the SQL standard's information_schema.

These schemas contain sources, tables, and views that expose different
types of metadata.

Whenever possible, applications should prefer to query `mz_catalog` over
`pg_catalog` or `information_schema`. The mapping between Materialize concepts
and PostgreSQL concepts is not one-to-one, and so the data in `pg_catalog`
and `information_schema` cannot accurately represent the particulars of Materialize.