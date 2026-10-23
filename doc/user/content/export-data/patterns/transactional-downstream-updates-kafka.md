---
title: "Transactional downstream updates using Kafka"
description: "Expose only fully committed Materialize timestamps when loading a Kafka sink into another data store."
menu:
  main:
    parent: 'sink-patterns'
---

When you load a [Kafka sink](/sql/create-sink/kafka/) into another data store,
such as Snowflake or ClickHouse, the loader writes messages as they arrive. A
query against the downstream store can therefore see some, but not all, of the
changes Materialize made at a given timestamp. Use this pattern when downstream
readers must only see complete, transactionally consistent results.

## How it works

Materialize writes each batch of sink messages to Kafka in a transaction,
together with a progress record in the connection's [progress
topic](/sql/create-sink/kafka/#exactly-once-processing). The progress record
says which timestamps the sink has fully committed. A downstream view can join
the two topics and hide any row that is not yet covered by a progress record.

| Where | What to read | Value |
|-------|--------------|-------|
| Data topic | `materialize-timestamp` header | The Materialize timestamp of the change, for example `1792700000000`. |
| Data topic | [`materialize-sink-id`](/sql/create-sink/kafka/#the-materialize-sink-id-header) header | The sink's ID, as shown in [`mz_sinks`](/sql/system-catalog/mz_catalog/#mz_sinks), for example `u123`. |
| Progress topic | Message key | `mz-sink-` followed by the sink's ID, for example `mz-sink-u123`. |
| Progress topic | Message value | JSON of the form `{"frontier":[1792700001000],"version":1}`. |

The `frontier` is exclusive: the sink has committed every change with a
timestamp less than the frontier. To expose only committed data:

1. Match each data message to the progress records whose key equals
   `'mz-sink-' || <materialize-sink-id header>`.
1. Take the latest frontier for that sink.
1. Keep the data messages where `materialize-timestamp` is less than that
   frontier.

The progress topic name is shown in the `sink_progress_topic` column of
[`mz_kafka_connections`](/sql/system-catalog/mz_catalog/#mz_kafka_connections).

## Example: Snowflake

This example is illustrative. It assumes the [Snowflake connector for
Kafka](https://docs.snowflake.com/en/user-guide/kafka-connector-overview) loads
the data topic into `raw_orders` and the progress topic into `raw_progress`.
The connector stores each message in a `RECORD_CONTENT` column and its headers
under `RECORD_METADATA:headers`. It keeps the message key in
`RECORD_METADATA:key` only when `key.converter` is set to
`org.apache.kafka.connect.storage.StringConverter`, so configure the progress
topic that way, and parse its value as JSON.

```sql
CREATE VIEW committed_orders AS
WITH latest_progress AS (
  SELECT
    RECORD_METADATA:key::string AS progress_key,
    MAX(RECORD_CONTENT:frontier[0]::number) AS frontier
  FROM raw_progress
  GROUP BY 1
)
SELECT o.RECORD_CONTENT AS order_record
FROM raw_orders AS o
JOIN latest_progress AS p
  ON p.progress_key =
     'mz-sink-' || o.RECORD_METADATA:headers:"materialize-sink-id"::string
WHERE o.RECORD_METADATA:headers:"materialize-timestamp"::number < p.frontier;
```

Readers query `committed_orders` instead of `raw_orders`.

## Considerations

- The `materialize-sink-id` header is in private preview. See [The
  `materialize-sink-id` header](/sql/create-sink/kafka/#the-materialize-sink-id-header)
  for its current status.
- The progress topic is shared by every sink that uses the same Kafka
  connection. Always filter progress records by key.
- Materialize writes a progress record each time the sink's frontier advances,
  even when there is no new data. Expect the progress table to grow steadily.
- Downstream consumers must read only committed Kafka data
  (`isolation.level=read_committed`). Otherwise they can load messages from
  aborted transactions. See [End-to-end exactly-once
  processing](/sql/create-sink/kafka/#end-to-end-exactly-once-processing).
- Header keys starting with `materialize-` are reserved for Materialize.

## Related pages

- [`CREATE SINK`: Kafka and Redpanda](/sql/create-sink/kafka/)
- [Exactly-once processing](/sql/create-sink/kafka/#exactly-once-processing)
- [Headers](/sql/create-sink/kafka/#headers)
