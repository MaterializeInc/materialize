---
title: "Materialize Open CDC"
description: "The Open CDC format allows Materialize to detect when records are duplicated or out of order."
menu:
  main:
    parent: "overview"
    weight: 4
---

Change data capture (CDC) tools provide feeds that record any changes to a database. Typically, the feeds are then saved to another platform, like Kafka, for storage or processing.

However, sometimes the stream can have missing or duplicate records, or send records out of order. For example, if a CDC tool crashes while writing a record, it may retry and write the record again, resulting in a duplicate entry.

To prevent this kind of error, Materialize has developed the **Open CDC format**, a format for the updates that CDC tools propagate to other systems.  The Open CDC format carries enough information for a downstream data consumer (like Materialize) to recognize when records are missing, duplicated, or out of order. Each record contains:

- the changes in a record (the updated fields or the deletion or addition of a new record)
- the timestamp for the change
- the number of messages used to transmit the record

The timestamp allows Materialize to re-order the events when they're received asynchronously, and the number of messages allows Materialize to determine when there are duplicate updates.

For a technical deep dive on the subject, see our blog post on [Change Data Capture](https://materialize.com/change-data-capture-part-1/).

**Note**: Currently, the Open CDC format is only supported for [Avro Kafka](/sql/create-source/avro-kafka) sources. If you're interested in using it for another source type, let us know in our [Slack workspace](https://materialize.com/s/chat).

## Example Open CDC Schema

You define the Open CDC format in the Avro schema when a source is created.


```sql
  CREATE MATERIALIZED SOURCE name_of_source
  FROM KAFKA BROKER 'kafka_url:9092' TOPIC 'name_of_kafka_topic'
  FORMAT AVRO USING SCHEMA '<schema goes here>'
  ENVELOPE MATERIALIZE
```

The following schema specifies that records will consist of `id` and `price` fields.

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

The updates for a single record might look like this:

```
{"array":[{"data":{"id":5,"price":{"int":10}},"time":1424849130111,"diff":1}]}
{"array":[{"data":{"id":5,"price":{"int":12}},"time":1424849130112,"diff":1}]}
{"array":[{"data":{"id":5,"price":{"int":12}},"time":1424849130111,"diff":-1}]}
{"array":[{"data":{"id":5,"price":{"int":10}},"time":1424849130113,"diff":-1}]}
```

This will be summarized as:

```
{"com.materialize.cdc.progress":{"lower":[1424849130110],"upper":[1424849130116],"counts":[{"time":1424849130112,"count":1},{"time":1424849130111,"count":2}, {"time": 1424849130113, "count": 1}]}}
```
`lower` and `upper` define the bounds of the timestamps.

Materialize should see 1 message for time 1424849130112, 2 messages for time 1424849130111, and 1 message for time 1424849130113. If there are 2 messages for time 1424849130113 with identical data updates, Materializes determines that this is a duplicate entry and discards one message.
