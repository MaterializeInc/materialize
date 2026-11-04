---
title: "Transactional downstream updates using Kafka"
description: "Expose only fully committed Materialize timestamps when loading a Kafka sink into another data store."
menu:
  main:
    parent: 'sink-patterns'
---

When you load messages from Kafka into another data store, such as Snowflake or
ClickHouse, the loader writes messages as they arrive. A query against the
downstream store can therefore see some, but not all, of the changes
Materialize made at a given timestamp. Use this pattern when downstream readers
must only see complete, transactionally consistent results.

This guide walks you through the pattern end to end, using Snowflake as the
destination:

1. Create a Kafka sink in Materialize.
1. Load the sink's data topic and progress topic into Snowflake.
1. Create a Snowflake view that exposes only committed changes.
1. Confirm the loader has caught up before exposing a timestamp.

The same approach works for any destination that stores Kafka headers and
message keys alongside the message value.

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

All sinks on a Kafka connection share one progress topic, and each sink's
records are keyed by `mz-sink-<sink_id>`. Joining on the `materialize-sink-id`
header is what ties each data message to its own sink's frontier.

The `frontier` is exclusive: the sink has committed every change with a
timestamp less than the frontier. To expose only committed data:

1. Match each data message to the progress records whose key equals
   `'mz-sink-' || <materialize-sink-id header>`.
1. Take the latest frontier for that sink.
1. Keep the data messages where `materialize-timestamp` is less than that
   frontier.

## Before you begin

- A Kafka or Redpanda cluster that Materialize can write to.
- A Kafka Connect cluster with the [Snowflake connector for
  Kafka](https://docs.snowflake.com/en/user-guide/kafka-connector-overview)
  installed.
- A Snowflake database and schema, and a Snowflake user that the connector can
  authenticate as. See [Installing and configuring the Kafka
  connector](https://docs.snowflake.com/en/user-guide/kafka-connector-install).
- A table, view, or materialized view in Materialize to sink. This guide uses a
  materialized view named `orders` with a unique key column `id`.

## Step 1. Create a Kafka sink in Materialize

1. Create a [connection](/sql/create-connection/#kafka) to your Kafka cluster.
   You can optionally name the progress topic with the `PROGRESS TOPIC` option;
   otherwise, Materialize uses a default name.

   ```mzsql
   CREATE SECRET kafka_password AS '<BROKER_PASSWORD>';

   CREATE CONNECTION kafka_connection TO KAFKA (
       BROKER '<BROKER_URL>',
       SASL MECHANISMS = 'SCRAM-SHA-256',
       SASL USERNAME = '<BROKER_USER>',
       SASL PASSWORD = SECRET kafka_password
   );
   ```

1. Create the sink. This guide uses the JSON format and the upsert envelope.

   ```mzsql
   CREATE SINK orders_sink
     FROM orders
     INTO KAFKA CONNECTION kafka_connection (TOPIC 'orders')
     KEY (id)
     FORMAT JSON
     ENVELOPE UPSERT;
   ```

   Materialize adds the `materialize-timestamp` and `materialize-sink-id`
   headers to every message automatically. You don't need to set any options.

1. Look up the sink's ID and the name of its progress topic. You need the
   progress topic name to configure the Snowflake connector.

   ```mzsql
   SELECT s.id AS sink_id, c.sink_progress_topic
   FROM mz_sinks AS s
   JOIN mz_kafka_connections AS c ON s.connection_id = c.id
   WHERE s.name = 'orders_sink';
   ```

   ```
    sink_id |         sink_progress_topic
   ---------+--------------------------------------
    u123    | _materialize-progress-<env>-<conn>
   ```

## Step 2. Load both topics into Snowflake

Configure the Snowflake connector to load the data topic into a table named
`raw_orders` and the progress topic into a table named `raw_progress`. For
example:

```json
{
  "name": "materialize_orders",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "topics": "orders,<sink_progress_topic>",
    "snowflake.topic2table.map": "orders:raw_orders,<sink_progress_topic>:raw_progress",
    "snowflake.url.name": "<account>.snowflakecomputing.com:443",
    "snowflake.user.name": "<user>",
    "snowflake.private.key": "<private_key>",
    "snowflake.database.name": "<database>",
    "snowflake.schema.name": "<schema>",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "com.snowflake.kafka.connector.records.SnowflakeJsonConverter",
    "consumer.override.isolation.level": "read_committed"
  }
}
```

Each Snowflake table gets two columns:

- `RECORD_CONTENT`: the message value, as a `VARIANT`. For the progress topic,
  this is the JSON object containing `frontier`.
- `RECORD_METADATA`: the message metadata, as a `VARIANT`. The message key is
  in `RECORD_METADATA:key` and the headers are in `RECORD_METADATA:headers`.

Two settings in this configuration matter for the pattern:

- `key.converter` must be `org.apache.kafka.connect.storage.StringConverter`.
  Otherwise, the connector does not keep the progress topic's message key, and
  you can't tell which sink a progress record belongs to.
- `consumer.override.isolation.level` must be `read_committed`, so that the
  connector never loads messages from aborted Kafka transactions. Your Kafka
  Connect worker must allow client configuration overrides
  (`connector.client.config.override.policy`). Alternatively, set
  `consumer.isolation.level=read_committed` in the worker configuration.
  This setting only excludes aborted transactions. It does not order the
  connector's flushes across partitions; see [Step 4](#step-4-confirm-the-loader-has-caught-up).

## Step 3. Create a view of committed changes in Snowflake

In Snowflake, create a view that keeps only the data messages that Materialize
has fully committed:

```sql
CREATE VIEW committed_orders AS
WITH latest_progress AS (
  -- The latest frontier for each sink that uses the progress topic.
  SELECT
    RECORD_METADATA:key::string AS progress_key,
    MAX(RECORD_CONTENT:frontier[0]::number) AS frontier
  FROM raw_progress
  GROUP BY 1
)
SELECT
  o.RECORD_METADATA:headers:"materialize-timestamp"::number AS mz_timestamp,
  o.RECORD_CONTENT AS order_record
FROM raw_orders AS o
-- Match each data message to the progress records for its sink.
JOIN latest_progress AS p
  ON p.progress_key =
     'mz-sink-' || o.RECORD_METADATA:headers:"materialize-sink-id"::string
-- Keep only the changes below the frontier.
WHERE o.RECORD_METADATA:headers:"materialize-timestamp"::number < p.frontier;
```

The view works as follows:

1. `latest_progress` finds the most recent frontier for each sink. Because the
   progress topic is shared by every sink on the same Kafka connection, the
   query groups by message key.
1. The join matches each order message to its sink's frontier, using the
   `materialize-sink-id` header.
1. The `WHERE` clause drops any message whose timestamp is not yet below the
   frontier. Those messages appear in the view once Materialize commits the
   rest of their timestamp.

## Step 4. Confirm the loader has caught up

The view in Step 3 is not sufficient on its own. Kafka transactions are atomic
on write, not on read: the Snowflake connector loads each topic partition
through its own channel and flushes each one independently. The progress
record can land in Snowflake before every data message below its frontier has
landed, so the loader must also confirm it has consumed the data topic up to
the frontier before you expose it. Use one of the following checks.

### Option 1: Gate on loaded timestamps

This option needs no extra infrastructure. The sink writes data messages in
non-decreasing `materialize-timestamp` order, so once a message with timestamp
`T` has loaded from a partition, every message below `T` on that partition has
loaded too (assuming your loader writes each partition in offset order). Gate
the view on the lesser of the latest frontier and the lowest per-partition
maximum timestamp:

```sql
CREATE OR REPLACE VIEW committed_orders AS
WITH latest_progress AS (
  SELECT
    RECORD_METADATA:key::string AS progress_key,
    MAX(RECORD_CONTENT:frontier[0]::number) AS frontier
  FROM raw_progress
  GROUP BY 1
),
loaded AS (
  -- The highest timestamp loaded from each data topic partition.
  SELECT
    'mz-sink-' || RECORD_METADATA:headers:"materialize-sink-id"::string AS progress_key,
    RECORD_METADATA:partition::number AS kafka_partition,
    MAX(RECORD_METADATA:headers:"materialize-timestamp"::number) AS max_ts
  FROM raw_orders
  GROUP BY 1, 2
),
safe_frontier AS (
  SELECT p.progress_key, LEAST(p.frontier, MIN(l.max_ts)) AS frontier
  FROM latest_progress AS p
  JOIN loaded AS l ON l.progress_key = p.progress_key
  GROUP BY p.progress_key, p.frontier
  -- Require a row from every partition of the data topic.
  HAVING COUNT(*) = <data_topic_partition_count>
)
SELECT
  o.RECORD_METADATA:headers:"materialize-timestamp"::number AS mz_timestamp,
  o.RECORD_CONTENT AS order_record
FROM raw_orders AS o
JOIN safe_frontier AS f
  ON f.progress_key =
     'mz-sink-' || o.RECORD_METADATA:headers:"materialize-sink-id"::string
WHERE o.RECORD_METADATA:headers:"materialize-timestamp"::number < f.frontier;
```

This check is safe but conservative. A partition that has no rows yet, or that
receives no new messages, holds the gate back until it does.

### Option 2: Gate on committed offsets

If your loader exposes the offset it has committed for each partition, such as
Snowpipe Streaming offset tokens, run a small routine outside Snowflake that
publishes a frontier only after checking offsets. Point the view at the
published frontier instead of `latest_progress`. Before publishing a frontier,
the routine should:

- Read the candidate frontier for the sink from `raw_progress`.
- After that progress record is visible, read each data partition's high
  watermark, then wait until its last stable offset (LSO), read with
  `read_committed` isolation, reaches that high watermark.
- Find the last committed data message below the LSO. Transaction markers
  occupy offsets, so this is not simply `LSO - 1`, and aborted ranges must be
  skipped.
- Confirm that each partition's committed offset has reached that message, or
  that the partition is empty.

## Step 5. Query the view

Point downstream readers at `committed_orders` instead of `raw_orders`:

```sql
SELECT * FROM committed_orders ORDER BY mz_timestamp DESC LIMIT 10;
```

Once one of the checks in Step 4 is in place, every row in the result belongs
to a Materialize timestamp whose changes have all been loaded into Snowflake.
The frontier join from Step 3 alone does not guarantee this.

## Considerations

- `committed_orders` contains one row per change, not the current state of
  `orders`. With the upsert envelope, a deletion is a message with a `null`
  value (a tombstone). To build the current state, take the latest committed
  row for each key and handle tombstones as configured by the connector's
  `behavior.on.null.values` setting.
- Materialize writes a progress record each time the sink's frontier advances,
  even when there is no new data. Expect `raw_progress` to grow steadily.
- Header keys starting with `materialize-` are reserved for Materialize.
- For other guarantees you need end to end, see [End-to-end exactly-once
  processing](/sql/create-sink/kafka/#end-to-end-exactly-once-processing).

## Related pages

- [`CREATE SINK`: Kafka and Redpanda](/sql/create-sink/kafka/)
- [Exactly-once processing](/sql/create-sink/kafka/#exactly-once-processing)
- [Headers](/sql/create-sink/kafka/#headers)
