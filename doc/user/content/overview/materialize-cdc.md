---
title: "Materialize CDC"
description: "The Materialize CDC format is a format for change datafeeds that has been designed especially to prevent record duplication or mistaken missequencing."
menu:
  main:
    parent: "overview"
    weight: 4
---

Change data capture (CDC) tools provide feeds that record any changes to a database. Typically, the feeds are then saved to another platform, like Kafka, for storage or processing. However, sometimes the stream can have missing or duplicate records, or records can be received out of order. For example, if a CDC tool crashes while writing a record, it may retry and write the record again, resulting in a duplicate entry.

The **Materialize CDC format** has been designed to provide a downstream data consumer (like Materialize) with enough information to recognize when records are duplicated or out of order. For a technical deep dive on the subject, see our blog post on [Change Data Capture](https://materialize.com/change-data-capture-part-1/).

Currently, the Materialize CDC format is only supported for [Avro-formatted Kafka sources](/sql/create-source/avro-kafka). If you're interested in using it for another source type, let us know in our [Slack workspace](https://materialize.com/s/chat).

To use the Materialize CDC format, you must:

1. Set up your CDC tool to emit its changefeed in the Materialize CDC format
2. Define the Materialize CDC format in an Avro schema when you [create a source](../../sql/create-source/avro-kafka/) in Materialize

## Materialize CDC JSON

You use a JSON document to define the format of the record updates transmitted in the changefeed and the format of the separate progress messages which tell the downstream consumer (Materialize) how many updates to expect for a particular logical timestamp. The downstream consumer will then compare the updates it has received to the expected updates and discard duplicates if there are too many.

### Update JSON array

Field | description
-------- | -----------
data  | The array that represents the changed record. Each array is composed of objects that define the `name` and `type` of each field to be included in the changefeed.
time  | The logical timestamp of the update. Materialize uses this to determine the order of updates and, in combination with the progress message, to determine whether there are duplicate updates to be disgarded.
diff  | The type of update (`-1` for deletion, `1` for addition or upsert)

#### Example update JSON definition

```[
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
                "name": "COLUMNNAME1",
                "type": "DATATYPE"
              },
              {
                "name": "COLUMNAME2",
                "type": "DATATYPE"
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
```

### Progress JSON array

The progress messages describe the number of updates transmitted at a particular timestamp or between specified times.

Field | description
-------- | -----------
lower  | The earliest logical timestamp for a set of updates
upper  | The latest logical timestamp for a set of updates
counts | The number of updates transmitted between `lower` and `upper`.

#### Example progress message JSON

```
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
 ```

### Example Materialize CDC schema

You define the Materialize CDC format in the Avro schema when a source is created.

```sql
  CREATE MATERIALIZED SOURCE name_of_source
  FROM KAFKA BROKER 'kafka_url:9092' TOPIC 'name_of_kafka_topic'
  FORMAT AVRO USING SCHEMA '<schema goes here>'
  ENVELOPE MATERIALIZE
```

The following schema specifies that records will consist of `id` and `price` fields. Note that `price` object supports multiple datatypes: It can be null or `int`.

```[
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

The schema above produces this changefeed:

```
{{"array":[{"data":{"id":5,"price":{"int":10}},"time":5,"diff":1}]}
{"array":[{"data":{"id":5,"price":{"int":12}},"time":4,"diff":1}]}
{"array":[{"data":{"id":5,"price":{"int":12}},"time":5,"diff":-1}]}
{"array":[{"data":{"id":5,"price":{"int":10}},"time":6,"diff":-1}]}

{"com.materialize.cdc.progress":{"lower":[0],"upper":[3],"counts":[]}}
{"com.materialize.cdc.progress":{"lower":[3],"upper":[10],"counts":[{"time":4,"count":1},{"time":5,"count":2}, {"time": 6, "count": 1}]}}
```

Even if Materializes receives the updates in an order different from the order in which they were transmitted, it will be able to reorder the updates by  `time`. Additionally, the progress messages tell Materialize to expect one update for timestamp `4`, two updates for timestamp `5`, and one update for timestamp `6`. If, for example, there are two identical updates for timestamp 4, Materializes determines that this is a duplicate entry and discards one update.
