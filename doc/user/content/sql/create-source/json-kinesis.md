---
title: "CREATE SOURCE: JSON over Kinesis (Beta)"
description: "Learn how to connect Materialize to JSON-formatted Kinesis streams"
menu:
  main:
    parent: 'create-source'
aliases:
    - /docs/sql/create-source/kinesis
    - /docs/sql/create-source/kinesis-source
---

{{% create-source/intro %}}
This document details how to connect Materialize to JSON-formatted Kinesis
streams.

Because this source is still in Beta status, please be sure to test
functionality prior to releasing it to production.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-kinesis.html" >}}

{{% create-source/syntax-details connector="kinesis" formats="json-bytes" envelopes="append-only" %}}

## Examples

```sql
CREATE SOURCE kinesis_source
FROM KINESIS ARN ... WITH (
    access_key_id = ...,
    secret_access_key = ...
)
FORMAT BYES;
```

This creates a source that...

- Is append-only.
- Has one column, `data`, which represents the stream's incoming bytes.

To use this data in views, you can decode its bytes into
[`jsonb`](/docs/sql/types/jsonb). For example:

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
