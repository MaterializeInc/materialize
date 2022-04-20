---
title: "Materialize CDC"
description: "The Materialize CDC format is a format for change datafeeds that has been designed especially to prevent errors resulting from record duplication or missequencing."
menu:
  main:
    parent: "advanced"
aliases:
  - /connect/materialize-cdc/
---

{{< beta />}}

Change data capture (CDC) tools provide feeds that record any changes to a database. Typically, the feeds are then saved to another platform, like Kafka, for storage or processing. However, sometimes the stream can have missing or duplicate records, or records can be received out of order. For example, if a CDC tool crashes while writing a record, it may retry and write the record again, resulting in a duplicate entry.

The **Materialize CDC format** has been designed to provide a downstream data consumer (like Materialize) with enough information to recognize when records are duplicated or out of order. For a technical deep dive on the subject, see our blog post on [Change Data Capture](https://materialize.com/change-data-capture-part-1/).

Currently, the Materialize CDC format is only supported for [Avro-formatted Kafka sources](/sql/create-source/avro-kafka). If you're interested in using it for another source type, let us know in our [Slack workspace](https://materialize.com/s/chat).

To use the Materialize CDC format, you must:

1. Transform the changefeed produced by your CDC tool into the Materialize CDC format.
2. Define the Materialize CDC format in an Avro schema when you [create a source](../../sql/create-source/avro-kafka/) in Materialize.

## Materialize CDC schema components

The Materialize CDC format has two components:

- Record updates, which include the record itself, a logical timestamp, and a description of type of update.
- Separate progress updates, which tell the downstream consumer (Materialize) how many updates have been transmitted at timestamps during a specified period.

Materialize compares the record updates it has received to the expected changes as described by the applicable progress update, and discards duplicates if there are too many identical record updates for a timestamp.

## Data requirements

Record updates take the form of `(data, time, diff)` triples, representing respectively the changed record, the logical timestamp of the update, and the type of update (`-1` for deletion and `1` for addition or upsert).

There should be at most one diff value for each `(data, time)` pair. That is, if a stream would have updates `(data1, time1, -1)` and `(data1, time1, 1)`, these should be combined and expressed as `(data1, time1, 0)` (and then suppressed). In effect, the data must be pre-consolidated before it is transmitted to Materialize. There can be multiple identical `(data, time, diff)` triples, but they will be treated as duplications, and only one update will be recorded.



## Example Materialize CDC Avro schema

```json
[
  {
    "type": "array",
    "items": {
      "type": "record",
      "name": "update",
      "namespace": "com.materialize.cdc",
      "fields": [
        {
          "name": "data",
          "type": {
            "type": "record",
            "name": "data",
            "fields": [
              {
                "name": "COLUMN_NAME1",
                "type": "DATA_TYPE"
              },
              {
                "name": "COLUMN_NAME2",
                "type": [
                  "DATA_TYPE1",
                  "DATA_TYPE2"
                ]
              }
            ]
          }
        },
        {
          "name": "time",
          "type": "long"
        },
        {
          "name": "diff",
          "type": "long"
        }
      ]
    }
  },
  {
    "type": "record",
    "name": "progress",
    "namespace": "com.materialize.cdc",
    "fields": [
      {
        "name": "lower",
        "type": {
          "type": "array",
          "items": "long"
        }
      },
      {
        "name": "upper",
        "type": {
          "type": "array",
          "items": "long"
        }
      },
      {
        "name": "counts",
        "type": {
          "type": "array",
          "items": {
            "type": "record",
            "name": "counts",
            "fields": [
              {
                "name": "time",
                "type": "long"
              },
              {
                "name": "count",
                "type": "long"
              }
            ]
          }
        }
      }
    ]
  }
  ]
  ```

Field | Description
-------- | -----------
`data`  | The array that represents the changed record. Each array is composed of objects that define the `name` and `type` (data type) of each field to be included in the changefeed. `type` supports multiple data types for a single object (for example, both `null` and `int` may be permissible for some fields).
`time`  | The logical timestamp of the update. Materialize uses this to determine the order of updates and, in conjunction with the update count, to determine whether there are duplicate updates to be discarded.
`diff`  | The type of update (`-1` for deletion, `1` for addition or upsert)
`lower`  | The earliest logical timestamp for a set of updates
`upper`  | The latest logical timestamp for a set of updates
`counts` | The number of updates transmitted between `lower` and `upper`.

## Example Materialize CDC workflow

You specify the use of the Materialize CDC format in the [Avro schema](../../sql/create-source/kafka/#format_spec) when a source is created.

```sql
  CREATE MATERIALIZED SOURCE name_of_source
  FROM KAFKA BROKER 'kafka_url:9092' TOPIC 'name_of_kafka_topic'
  FORMAT AVRO USING SCHEMA '<schema goes here>'
  ENVELOPE MATERIALIZE
```

The following example schema specifies that records will consist of `id` and `price` fields. Note that `price` object supports multiple datatypes: It can be `null` or `int`.

```json
[
  {
    "type": "array",
    "items": {
      "type": "record",
      "name": "update",
      "namespace": "com.materialize.cdc",
      "fields": [
        {
          "name": "data",
          "type": {
            "type": "record",
            "name": "data",
            "fields": [
              {
                "name": "id",
                "type": "long"
              },
              {
                "name": "price",
                "type": [
                  "null",
                  "int"
                ]
              }
            ]
          }
        },
        {
          "name": "time",
          "type": "long"
        },
        {
          "name": "diff",
          "type": "long"
        }
      ]
    }
  },
  {
    "type": "record",
    "name": "progress",
    "namespace": "com.materialize.cdc",
    "fields": [
      {
        "name": "lower",
        "type": {
          "type": "array",
          "items": "long"
        }
      },
      {
        "name": "upper",
        "type": {
          "type": "array",
          "items": "long"
        }
      },
      {
        "name": "counts",
        "type": {
          "type": "array",
          "items": {
            "type": "record",
            "name": "counts",
            "fields": [
              {
                "name": "time",
                "type": "long"
              },
              {
                "name": "count",
                "type": "long"
              }
            ]
          }
        }
      }
    ]
  }
  ]
  ```

As an example, the schema above might produce this changefeed:

```json
{{"array":[{"data":{"id":5,"price":{"int":10}},"time":5,"diff":1}]}
{"array":[{"data":{"id":5,"price":{"int":12}},"time":4,"diff":1}]}
{"array":[{"data":{"id":5,"price":{"int":12}},"time":5,"diff":-1}]}
{"array":[{"data":{"id":5,"price":{"int":10}},"time":6,"diff":-1}]}

{"com.materialize.cdc.progress":{"lower":[0],"upper":[3],"counts":[]}}
{"com.materialize.cdc.progress":{"lower":[3],"upper":[10],"counts":[{"time":4,"count":1},{"time":5,"count":2}, {"time": 6, "count": 1}]}}
```

Even if Materializes receives the updates in an order different from the order in which they were transmitted, it will be able to reorder the updates by  `time`. Additionally, the progress updates tell Materialize to expect one updated record for timestamp `4`, two updated records for timestamp `5`, and one updated record for timestamp `6`. If, for example, there are two identical updated records for timestamp 4, Materializes determines that this is a duplicate entry and discards one update.
