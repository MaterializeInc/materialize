---
title: "oid type"
description: "Express a PostgreSQL-compatible object identifier"
menu:
  main:
    parent: 'sql-types'
---

`oid` expresses a PostgreSQL-compatible object identifier.

Detail | Info
-------|------
**Size** | 4 bytes
**Catalog name** | `pg_catalog.oid`
**OID** | 26

## Details

`oid` types in Materialize are provided for compatibility with PostgreSQL . You
typically will not interact with the `oid` type unless you are working with a
tool that was developed for PostgreSQL.

See the [Object Identifier Types][pg-oid] section of the PostgreSQL
documentation for more details.

### Valid casts

#### From `oid`

You can [cast](../../functions/cast) `oid` to:

- [`integer`](../integer) (by assignment)
- [`bigint`](../integer) (by assignment)
- [`text`](../text) (explicitly)

#### To `oid`

You can [cast](../../functions/cast) from the following types to `oid`:

- [`integer`](../integer) (implicitly)
- [`bigint`](../integer) (implicitly)
- [`text`](../text) (explicitly)

[pg-oid]: https://www.postgresql.org/docs/current/datatype-oid.html
