---
title: "CREATE SOURCE: Kinesis Data Streams"
description: "Connecting Materialize to a Kinesis data stream"
menu:
  main:
    parent: 'create-source'
    name: Kinesis
    weight: 40
aliases:
    - /sql/create-source/kinesis-source
    - /sql/create-source/json-kinesis
    - /sql/create-source/protobuf-kinesis
    - /sql/create-source/text-kinesis
    - /sql/create-source/csv-kinesis
---

{{< beta />}}

{{% create-source/intro %}}
This page details how to connect Materialize to Kinesis Data Streams to read data from individual streams.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-kinesis.svg" >}}

#### `format_spec`

{{< diagram "kinesis-format-spec.svg" >}}

#### `with_options`

{{< diagram "with-options-aws.svg" >}}

#### `static_credentials`

{{< diagram "with-options-aws-static.svg" >}}

{{% create-source/syntax-connector-details connector="kinesis" envelopes="append-only" %}}

### `WITH` options

Field                                | Value     | Description
-------------------------------------|-----------|-------------------------------------
`timestamp_frequency_ms`             | `int`     | Default: `1000`. Sets the timestamping frequency in `ms`. Reflects how frequently the source advances its timestamp. This measure reflects how stale data in views will be. Lower values result in more-up-to-date views but may reduce throughput.


## Supported formats

|<div style="width:290px">Format</div> | [Append-only envelope] | [Upsert envelope] | [Debezium envelope] |
---------------------------------------|:----------------------:|:-----------------:|:-------------------:|
| [JSON]                               | ✓                      |                   |                     |
| [Protobuf]                           | ✓                      |                   |                     |
| [Text/bytes]                         | ✓                      |                   |                     |
| [CSV]                                | ✓                      |                   |                     |

## Features

### Using enhanced fan-out (EFO)

Not supported yet. If you're interested in this feature, please leave a comment in [#2192](https://github.com/MaterializeInc/materialize/issues/2192).

### Setting start sequence numbers

Not supported yet. If you're interested in this feature, please leave a comment in [#5972](https://github.com/MaterializeInc/materialize/issues/5972).

## Authentication

{{% specifying-aws-credentials %}}

#### Permissions Required

The IAM User or Role used by `materialized` requires `kinesis-read` permissions and access to `ListStreams` and `Read`.
## Examples

### Creating a source

{{< tabs tabID="1" >}}
{{< tab "JSON">}}

```sql
CREATE SOURCE json_source
  FROM KINESIS ARN 'arn:aws:kinesis:aws-region::stream/fake-stream'
  WITH ( access_key_id = 'access_key_id',
         secret_access_key = 'secret_access_key' )
  FORMAT BYTES;
```

```sql
CREATE MATERIALIZED VIEW jsonified_kinesis_source AS
  SELECT
    data->>'field1' AS field_1,
    data->>'field2' AS field_2,
    data->>'field3' AS field_3
  FROM (SELECT CONVERT_FROM(data, 'utf8')::jsonb AS data FROM json_source);
```

{{< /tab >}}
{{< tab "Protobuf">}}

```sql
CREATE SOURCE proto_source
  FROM KINESIS ARN 'arn:aws:kinesis:aws-region::stream/fake-stream'
  WITH ( access_key_id = 'access_key_id',
         secret_access_key = 'secret_access_key' )
  FORMAT PROTOBUF MESSAGE 'billing.Batch'
    USING SCHEMA FILE '[path to schema]';
```

{{< /tab >}}
{{< tab "Text/bytes">}}

```sql
CREATE SOURCE text_source
  FROM KINESIS ARN 'arn:aws:kinesis:aws-region::stream/fake-stream'
  WITH ( access_key_id = 'access_key_id',
         secret_access_key = 'secret_access_key' )
  FORMAT TEXT;
```

{{< /tab >}}
{{< tab "CSV">}}

```sql
CREATE SOURCE csv_source (col_foo, col_bar, col_baz)
  FROM KINESIS ARN 'arn:aws:kinesis:aws-region::stream/fake-stream'
  WITH ( access_key_id = 'access_key_id',
         secret_access_key = 'secret_access_key' )
  FORMAT CSV WITH 3 COLUMNS;
```

{{< /tab >}}
{{< /tabs >}}

## Known limitations

- **Resharding:** adjusting the number of shards in the source stream is not supported {{% gh 8776 %}}. If you reshard the stream, you'll need to drop and recreate the source.

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE MATERIALIZED VIEW`](../../create-view)
- [`SELECT`](../../select)

[JSON]: /sql/create-source/#json
[Protobuf]: /sql/create-source/#protobuf
[Text/bytes]: /sql/create-source/#textbytes
[CSV]: /sql/create-source/#csv

[Append-only envelope]: /sql/create-source/#append-only-envelope
[Upsert envelope]: /sql/create-source/#upsert-envelope
[Debezium envelope]: /sql/create-source/#debezium-envelope
