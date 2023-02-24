---
title: "CREATE SOURCE: Kinesis Data Streams"
description: "Connecting Materialize to a Kinesis data stream"
draft: true
#menu:
#  main:
#    parent: 'create-source'
#    name: Kinesis Data Streams
#    weight: 40
aliases:
    - /sql/create-source/kinesis-source
    - /sql/create-source/json-kinesis
    - /sql/create-source/protobuf-kinesis
    - /sql/create-source/text-kinesis
    - /sql/create-source/csv-kinesis
---

[//]: # "NOTE(morsapaes) Once we're ready to bring the KDS source back, check #12991 to restore the previous docs state."

{{< beta />}}

{{% create-source/intro %}}
This page describes how to connect Materialize to Kinesis Data Streams to read data from individual streams.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-kinesis.svg" >}}

Field | Use
------|-----
_src_name_  | The name for the source.
**EXPOSE PROGRESS AS** _progress_subsource_name_ | Name this source's progress collection `progress_subsource_name`; if this is not specified, Materialize names the progress collection `<src_name>_progress`. For details about the progress collection, see [Progress collection](#progress-collection).

#### `format_spec`

{{< diagram "kinesis-format-spec.svg" >}}

#### `with_options`

{{< diagram "with-options-aws.svg" >}}

#### `static_credentials`

{{< diagram "with-options-aws-static.svg" >}}

{{% create-source/syntax-connector-details connector="kinesis" envelopes="append-only" %}}

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

### Progress collection

Each source exposes its progress as a separate progress collection. You can
choose a name for this collection using **EXPOSE PROGRESS AS**
_progress_subsource_name_ or Materialize will automatically name the collection
`<source_name>_progress`. You can find the collection's name using [`SHOW
SOURCES`](/sql/show-sources).

The progress collection schema depends on your source type. For Postgres
sources, we return the greatest `lsn` ([`uint8`](/sql/types/uint)) we have
consumed from your Postgres server's replication stream.

As long as as the LSN continues to change, Materialize is consuming data.

## Known limitations

##### Resharding

Adjusting the number of shards in the source stream is not supported {{% gh 8776 %}}. If you reshard the stream, you'll need to drop and recreate the source.

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
