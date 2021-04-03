---
title: "CREATE SOURCE: JSON over Kinesis"
description: "Learn how to connect Materialize to JSON-formatted Kinesis streams"
menu:
  main:
    parent: 'create-source'
aliases:
    - /sql/create-source/kinesis
    - /sql/create-source/kinesis-source
---

{{% create-source/intro %}}
This document details how to connect Materialize to JSON-formatted Kinesis
streams.

{{< kinesis-alpha >}}

{{< volatility-warning >}}Kinesis{{< /volatility-warning >}}

{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-json-kinesis.svg" >}}

{{% create-source/syntax-details connector="kinesis" formats="json-bytes" envelopes="append-only" %}}

## Examples

```sql
CREATE SOURCE kinesis_source
FROM KINESIS ARN ... WITH (
    access_key_id = ...,
    secret_access_key = ...
)
FORMAT BYTES;
```

This creates a source that...

- Is append-only.
- Has one column, `data`, which represents the stream's incoming bytes.

To use this data in views, you can decode its bytes into
[`jsonb`](/sql/types/jsonb). For example:

```sql
CREATE MATERIALIZED VIEW jsonified_kinesis_source AS
  SELECT CAST(data AS jsonb) AS data
  FROM (
      SELECT convert_from(data, 'utf8') AS data
      FROM kinesis_source
  )
```

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
