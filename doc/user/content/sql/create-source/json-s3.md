---
title: "CREATE SOURCE: JSON from an S3 bucket"
description: "Learn how to connect Materialize to an S3 Bucket"
menu:
  main:
    parent: 'create-source'
---
{{% create-source/intro %}}
This document details how to connect Materialize to an S3 Bucket that contains
multiple objects, and to listen for new object creation. Each S3 object can
contain multiple records serialized as JSON, separated by newlines.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-s3-json.svg" >}}

#### `with_options`

{{< diagram "with-options.svg" >}}

{{% create-source/syntax-details connector="s3" formats="text bytes" envelopes="append-only" keyConstraint=false %}}

## Example

Assuming there is an S3 bucket "analytics" that contains the following keys and
associated content:

**users/2021/usage.json**
```json
{"user_id": 9999, "disks_used": 2, "cpu_used_minutes": 3600}
{"user_id": 888, "disks_used": 0}
{"user_id": 777, "disks_used": 25, "cpu_used_minutes": 9200}
```

**users/2020/usage.json**
```json
{"user_id": 9999, "disks_used": 150, "cpu_used_minutes": 400100}
{"user_id": 888, "disks_used": 0}
{"user_id": 777, "disks_used": 12, "cpu_used_minutes": 999900}
```

We can load all these keys with the following command:

```sql
> CREATE SOURCE json_source
  FROM S3 DISCOVER OBJECTS MATCHING '**/*.json' USING BUCKET SCAN 'analytics'
  WITH (region = 'us-east-2')
  FORMAT TEXT;
```

This creates a source that...

- Lazily scans the entire `analytics` bucket looking for objects that have keys that end with
  `*.json` -- this source is not **MATERIALIZED**, so nothing has happened yet and it cannot be
  queried.
- Has one *text* column: `text` and one automatically-generated *integer* column `mz_record` which
  reflects the order that materialized first encountered that row in.

To access the data as JSON we can use standard JSON [functions](/sql/functions/#json-func) and
[operators](/sql/functions/#json):

```sql
> CREATE MATERIALIZED VIEW json_example AS
  SELECT
    jsonified.data->>'user_id' AS user_id,
    jsonified.data->>'disks_used' AS disks_used,
    jsonified.data->>'cpu_used_minutes' AS cpu_used_minutes
  FROM (SELECT text::JSONB AS data FROM json_source) AS jsonified;
```

This creates a source that...

- Has three *string* columns (`user_id`, `disks_used`, and `cpu_used_minutes`)
- Is immediately materialized, so will be cached in memory and is immediately queryable

## Related pages

- S3 with [`CSV`](../csv-s3)/[`TEXT`](../text-s3) encoded data
- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
