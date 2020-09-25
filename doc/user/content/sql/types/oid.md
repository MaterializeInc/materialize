---
title: "oid Data Type"
description: "Express a PostgreSQL-compatible object identifier"
menu:
  main:
    parent: 'sql-types'
---

`oid` expresses a PostgreSQL-compatible object identifier.

## Details

`oid` types in Materialize are provided for compatibility with PostgreSQL . You
typically will not interact with the `oid` type unless you are working with a
tool that was developed for PostgreSQL.

See the [Object Identifier Types][pg-oid] section of the PostgreSQL
documentation for more details.

### Valid casts

You can [cast](../../functions/cast) `oid` to and from:

- [`int`](../integer)
- [`text`](../text)

[pg-oid]: https://www.postgresql.org/docs/current/datatype-oid.html
