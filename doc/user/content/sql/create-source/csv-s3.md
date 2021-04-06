---
title: "CREATE SOURCE: CSV from an S3 bucket"
description: "Learn how to connect Materialize to an S3 Bucket"
menu:
  main:
    parent: 'create-source'
---
{{% create-source/intro %}}
This document details how to connect Materialize to an S3 Bucket that contains
multiple objects, and to listen for new object creation. Each S3 object can
contain multiple records serialized as CSV, separated by newlines.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-s3-csv.svg" >}}

{{% create-source/syntax-details connector="s3" formats="csv" envelopes="append-only" %}}

## Example

Assuming there is an S3 bucket "analytics" that contains the following keys and
associated content:

**users/2021/engagement.csv**
```csv
9999,active,8 hours
888,inactive,
777,active,3 hours
```

**users/2020/engagement.csv**
```csv
9999,active,750 hours
888,inactive,
777,active,1002 hours
```

We can load all these keys with the following command:

```sql
CREATE MATERIALIZED SOURCE csv_example (user_id, status, usage)
FROM S3 DISCOVER OBJECTS MATCHING '**/*.csv' USING BUCKET SCAN 'analytics'
WITH (region = 'us-east-2')
FORMAT CSV WITH 3 COLUMNS;
```

This creates a source that...

- Scans the entire `analytics` bucket looking for objects that have keys that end with `*.csv`
- Has three *text* columns: `user_id`, `status`, and `usage` and one automatically-generated
  *integer* column `mz_record` which reflects the order that materialized first encountered that
  row in.

  Lines in any object that do not have three columns will be ignored and an error-level message
  will be written to the Materialize log.
- Materializes the contents in memory immediately upon issuing the command.

If we want to handle well-typed data while stripping out some uninteresting columns, we can
instead write an unmaterialized source and parse columns in a view materialization:

```sql
CREATE SOURCE csv_source (user_id, status, usage)
FROM S3 DISCOVER OBJECTS MATCHING '**/*.csv' USING BUCKET SCAN 'analytics'
WITH (region = 'us-east-2')
FORMAT CSV WITH 3 COLUMNS;
```
```sql
CREATE MATERIALIZED VIEW csv_example AS
SELECT user_id::int4, usage::interval FROM csv_source;
```

This creates a view that has the same properties as above, except it:

* Has two columns (one *integer*, one *interval*)
* Does not store the string data in memory after it has been parsed

## Related Pages

- S3 with [`TEXT`](../text-s3)/[`JSON`](../json-s3) encoded data
- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
