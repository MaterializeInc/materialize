---
title: "CREATE SOURCE: JSON from local file"
description: "Learn how to connect Materialize to a JSON-formatted text file"
menu:
  main:
    parent: 'create-source'
aliases:
    - /sql/create-source/json
---

{{% create-source/intro %}}
This document details how to connect Materialize to a JSON-formatted local text file.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-json.svg" >}}

{{% create-source/syntax-details connector="file" formats="json-bytes" envelopes="append-only" %}}

## Examples

### Creating a JSON source from a local file

1. Generate a local JSON file. For example:

    ```shell
    echo '{"a":1,"b":2}\n{"a":3,"b":4}' > source.json
    ```

1. Create a materialized source from the file:

    ```sql
    CREATE SOURCE local_json_file
    FROM FILE '/Users/sean/materialize/materialize/source.json'
    FORMAT BYTES;
    ```

1. Create a [materialized view](../../create-materialized-view) to convert the
   source's bytes into [`jsonb`](../../types/jsonb):

    ```sql
    CREATE MATERIALIZED VIEW jsonified_bytes AS
    SELECT CAST(data AS JSONB) AS data
    FROM (
        SELECT CONVERT_FROM(data, 'utf8') AS data
        FROM local_json_file
    );
    ```

    You can see that this column contains `jsonb` data using the field accessor:

    ```sql
    SELECT data->'a' AS field_access FROM jsonified_bytes ORDER BY field_access;
    ```
    ```nofmt
     field_access
    --------------
    1.0
    3.0
    ```

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
