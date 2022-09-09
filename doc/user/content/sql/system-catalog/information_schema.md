---
title: "information_schema"
description: "information_schema is a system catalog that presents metadata in the format used by SQL standard."
menu:
  main:
    parent: 'system-catalog'
    name: information_schema
    weight: 3
---

Materialize has compatibility shims for the following relations from the
SQL standard [`information_schema`](https://www.postgresql.org/docs/current/infoschema-schema.html)
schema, which is automatically available in all databases:

  * [`columns`](https://www.postgresql.org/docs/current/infoschema-columns.html)
  * [`tables`](https://www.postgresql.org/docs/current/infoschema-tables.html)

These compatibility shims are largely incomplete. Most are lacking some columns
that are present in the SQL standard, or if they do include the column the
result set its value may always be `NULL`. The precise nature of the
incompleteness is intentionally undocumented. New tools developed against
Materialize should use the documented [`mz_catalog`](../mz_catalog) API instead.

[`bigint`]: /sql/types/bigint
[`bigint list`]: /sql/types/list
[`boolean`]: /sql/types/boolean
[`bytea`]: /sql/types/bytea
[`double precision`]: /sql/types/double-precision
[`jsonb`]: /sql/types/jsonb
[`numeric`]: /sql/types/numeric
[`oid`]: /sql/types/oid
[`text`]: /sql/types/text
[`timestamp`]: /sql/types/timestamp
[`timestamp with time zone`]: /sql/types/timestamp
[`uuid`]: /sql/types/uuid
[gh-issue]: https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md
[oid]: /sql/types/oid
[`text array`]: /sql/types/array
[arrangement]: /overview/arrangements/#arrangements
[dataflow]: /overview/arrangements/#dataflows
[librdkafka]: https://github.com/edenhill/librdkafka/tree/v{{< librdkafka-version >}}
[`STATISTICS.md`]: https://github.com/edenhill/librdkafka/tree/v{{< librdkafka-version >}}/STATISTICS.md