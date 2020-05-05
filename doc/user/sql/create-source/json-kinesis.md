---
title: "CREATE SOURCE: JSON over Kinesis"
description: "Learn how to connect Materialize to an Avro-formatted Kafka topic"
menu:
  main:
    parent: 'create-source'
aliases:
    - /docs/sql/create-source/kinesis
    - /docs/sql/create-source/kinesis-source
---

{{% create-source/intro format="JSON" connector="Kinesis streams" %}}

## Syntax

{{< diagram "create-source-kinesis.html" >}}

{{% create-source/syntax-details connector="kinesis" formats="json-bytes" envelopes="append-only" %}}

## Examples

```sql
CREATE SOURCE kinesis_source
FROM KINESIS ARN ...
WITH (access_key=...,
      secret_access_key=...)
FORMAT BYES;
```

This creates a source that...

- Is append-only.
- Has one column, `data`, which represents the stream's incoming bytes.

To use this data in views, you can decode its bytes into
[`jsonb`](/docs/sql/types/jsonb). For example:

```sql
CREATE MATERIALIZED VIEW jsonified_kinesis_source AS
SELECT CAST(data AS JSONB) AS data
FROM (
    SELECT CONVERT_FROM(data, 'utf8') AS data
    FROM kinesis_source
)
```

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
