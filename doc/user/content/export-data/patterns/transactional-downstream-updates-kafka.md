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
1. Build a table of trusted frontiers that Snowflake has fully loaded.
1. Create Snowflake views that expose only changes below the trusted frontier.

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

{{< warning >}}
This pattern parses progress records. Their format is not yet a documented,
stable interface and may change in a future release. Ignore any fields you
don't recognize when you parse them.
{{< /warning >}}

The `frontier` is exclusive: the sink has committed every change with a
timestamp less than the frontier. The pattern has two parts:

1. **Build a trusted frontier.** For each sink, record a frontier only once
   the loader has confirmed that every data message below it has landed in
   the destination. The latest frontier in the progress topic is only a
   candidate; see [Step 3](#step-3-build-a-trusted-frontier-table).
1. **Gate views on the trusted frontier.** Keep the data messages whose
   `materialize-timestamp` is less than the trusted frontier for their sink.

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
`raw_orders` and the progress topic into a table named `raw_progress`. This
guide uses version 3 of the connector with Snowpipe Streaming, which loads
each partition through its own channel. For example:

```json
{
  "name": "materialize_orders",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
    "topics": "orders,<sink_progress_topic>",
    "snowflake.topic2table.map": "orders:raw_orders,<sink_progress_topic>:raw_progress",
    "snowflake.url.name": "<account>.snowflakecomputing.com:443",
    "snowflake.user.name": "<user>",
    "snowflake.private.key": "<private_key>",
    "snowflake.database.name": "<database>",
    "snowflake.schema.name": "<schema>",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "consumer.override.isolation.level": "read_committed"
  }
}
```

Each Snowflake table gets two columns:

- `RECORD_CONTENT`: the message value, as a `VARIANT`. For the progress topic,
  this is the JSON object containing `frontier`.
- `RECORD_METADATA`: the message metadata, as a `VARIANT`. The message key is
  in `RECORD_METADATA:key` and the headers are in `RECORD_METADATA:headers`.

These settings in this configuration matter for the pattern:

- `snowflake.ingestion.method` must be `SNOWPIPE_STREAMING`. Each channel
  then records an offset token, which [Step 3](#step-3-build-a-trusted-frontier-table)
  relies on. Snowpipe Streaming does not support the Snowflake converters, so
  this example uses `JsonConverter` for values.
- `key.converter` must be `org.apache.kafka.connect.storage.StringConverter`.
  Otherwise, the connector does not keep the progress topic's message key, and
  you can't tell which sink a progress record belongs to.
- `consumer.override.isolation.level` must be `read_committed`, so that the
  connector never loads messages from aborted Kafka transactions. Your Kafka
  Connect worker must allow client configuration overrides
  (`connector.client.config.override.policy`). Alternatively, set
  `consumer.isolation.level=read_committed` in the worker configuration.
  This setting only excludes aborted transactions. It does not order the
  connector's flushes across partitions; see [Step 3](#step-3-build-a-trusted-frontier-table).
- `errors.tolerance` and `behavior.on.null.values` must keep their defaults,
  `none` and `DEFAULT`. With other values, the connector can skip a message
  but still advance its offset token past it, and Step 3 would then trust a
  frontier that is missing that message. With `DEFAULT`, a deletion (a
  message with a `null` value) loads as a row with empty content.

## Step 3. Build a trusted frontier table

The latest frontier in `raw_progress` is not enough to gate on. Kafka
transactions are atomic on write, not on read: the Snowflake connector loads
each topic partition through its own channel and flushes each one
independently. A progress record can land in Snowflake before every data
message below its frontier has landed. Before you trust a frontier, you must
confirm that the loader has consumed the data topic up to it.

This requires a loader that reports the offset it has committed for each
partition. With Snowpipe Streaming, the Snowflake connector loads each
partition through its own channel, and each channel's offset token records the
last Kafka offset that channel has committed to Snowflake. To read the offset
tokens, run `SHOW CHANNELS IN TABLE raw_orders` and use the `offset_token`
column. The checks below compare those offset tokens to the data topic.
Create a table to hold the frontiers you have confirmed:

```sql
CREATE TABLE trusted_frontier (
  sink_id STRING NOT NULL,
  frontier NUMBER NOT NULL,
  confirmed_at TIMESTAMP_LTZ NOT NULL
);
```

Then run a small routine outside Snowflake that appends a row each time it
confirms a new frontier for a sink. Views read the highest frontier for each
sink, the same way they would read `raw_progress`. For each sink, the routine
should:

1. Read the candidate frontier for the sink from `raw_progress`.
1. Record each data partition's high watermark. The progress record is
   already in Snowflake, so every data message below the candidate frontier
   sits below these high watermarks.
1. For each data partition, start a consumer with `read_committed` isolation
   at the channel's offset token + 1, or at the start of the partition if the
   channel has no offset token yet. Read until the consumer's position reaches
   the recorded high watermark. `read_committed` skips transaction markers and
   aborted messages.
1. If no partition returns a message, Snowflake has loaded every committed
   data message below the candidate frontier. Write the candidate frontier
   to `trusted_frontier`.
1. Otherwise, the connector is still loading. Wait, then repeat from step 3
   with the same candidate frontier and high watermarks.

Keep the candidate fixed until you confirm it. If you pick a newer candidate
on each attempt, the routine can keep chasing new data and never confirm any
frontier. A production routine also needs to handle timeouts and retries, for
example when a transaction stays open and holds the consumer below the high
watermark.

## Step 4. Create a view of committed changes in Snowflake

Create a view that keeps only the data messages below the sink's trusted
frontier:

```sql
CREATE VIEW committed_orders AS
WITH latest_trusted AS (
  -- The latest trusted frontier for each sink.
  SELECT sink_id, MAX(frontier) AS frontier
  FROM trusted_frontier
  GROUP BY 1
)
SELECT
  o.RECORD_METADATA:headers:"materialize-timestamp"::number AS mz_timestamp,
  o.RECORD_CONTENT AS order_record
FROM raw_orders AS o
-- Match each data message to the trusted frontier for its sink.
JOIN latest_trusted AS f
  ON f.sink_id = o.RECORD_METADATA:headers:"materialize-sink-id"::string
-- Keep only the changes below the frontier.
WHERE o.RECORD_METADATA:headers:"materialize-timestamp"::number < f.frontier;
```

`latest_trusted` takes the highest trusted frontier for each sink. The join
uses the `materialize-sink-id` header, so each message is compared to its own
sink's frontier. Messages at or above the frontier appear in the view
once the routine confirms they have loaded.

## Step 5. Join changes from multiple sinks

If you sink several relations and create a view like `committed_orders` for
each, each view is gated on its own sink's frontier. Joining two such views can
combine changes from different Materialize timestamps, which loses
consistency. Gate joined views on a common frontier instead. There are two
ways to do this:

- **One shared frontier.** Gate every view on the lowest trusted frontier
  across all sinks. Any of the views can then be joined with any other. The
  tradeoff is freshness: every view is held back by the sink that is furthest
  behind.

  ```sql
  CREATE VIEW shared_frontier AS
  WITH current_frontiers AS (
    -- The latest trusted frontier for each live sink.
    SELECT sink_id, MAX(frontier) AS current_frontier
    FROM trusted_frontier
    WHERE sink_id IN ('u123', 'u456')
    GROUP BY sink_id
  )
  SELECT MIN(current_frontier) AS frontier
  FROM current_frontiers;
  ```

  List the live sinks explicitly. A dropped sink's frontier stops advancing,
  and a recreated sink gets a new ID, so update the list whenever you drop or
  recreate a sink. Otherwise, the shared frontier stalls.

- **One frontier per join.** For each join, gate on the lowest trusted
  frontier of only the sinks that join reads. This keeps each join as fresh as
  its inputs allow, but each join needs its own gate that lists its sinks.

## Step 6. Query the view

Point downstream readers at `committed_orders` instead of `raw_orders`:

```sql
SELECT * FROM committed_orders ORDER BY mz_timestamp DESC LIMIT 10;
```

Every row in the result belongs to a Materialize timestamp whose changes have
all been loaded into Snowflake, because the view gates on `trusted_frontier`
rather than on `raw_progress` directly.

## Considerations

- `committed_orders` contains one row per change, not the current state of
  `orders`. With the upsert envelope, a deletion is a message with a `null`
  value (a tombstone), which loads as a row with empty content. To build the
  current state, take the latest committed row for each key and drop the keys
  whose latest row is a tombstone.
- Materialize writes a progress record each time the sink's frontier advances,
  even when there is no new data. Expect `raw_progress` to grow steadily.
- Header keys starting with `materialize-` are reserved for Materialize.
- For other guarantees you need end to end, see [End-to-end exactly-once
  processing](/sql/create-sink/kafka/#end-to-end-exactly-once-processing).

## Related pages

- [`CREATE SINK`: Kafka and Redpanda](/sql/create-sink/kafka/)
- [Exactly-once processing](/sql/create-sink/kafka/#exactly-once-processing)
- [Headers](/sql/create-sink/kafka/#headers)
