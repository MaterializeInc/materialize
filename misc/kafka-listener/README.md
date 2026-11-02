# Kafka listener source demo

Kafka listener sources let Kafka producers write straight into Materialize, with
no Kafka cluster in between. The cluster replica running the source accepts
Kafka protocol connections, and every record produced to the source's topic is
ingested like a record read by a Kafka source. See
[the design doc](../../doc/developer/design/20261030_kafka_listener_source.md).

## Run it

1. Make sure CockroachDB is running on `localhost:26257` (see the `mz-run` skill).

2. Start environmentd. The script uses its own metadata database and blob
   directory, so it does not touch the state of other local instances. Pass
   `--fresh` to start over.

   ```
   misc/kafka-listener/run-environmentd.sh
   ```

3. In another terminal, run the demo:

   ```
   bin/pyactivate misc/kafka-listener/demo.py
   ```

   It enables the feature flags, creates a cluster and two sources, produces
   records with librdkafka, and queries them:

   - `page_views`: append-only JSON events, no authentication, with offset,
     timestamp and headers included as columns.
   - `users`: an `ENVELOPE UPSERT` table that requires SASL/PLAIN. The demo
     updates one user and deletes another with a tombstone.

   It ends by printing `kcat` commands for producing by hand.

4. Optionally, check exactly-once delivery, the schema registry, and measure
   throughput:

   ```
   bin/pyactivate misc/kafka-listener/exactly_once.py
   bin/pyactivate misc/kafka-listener/tables.py
   bin/pyactivate misc/kafka-listener/registry.py
   bin/pyactivate misc/kafka-listener/perf.py
   ```

## By hand

```sql
-- As mz_system (psql -h localhost -p 6877 -U mz_system):
ALTER SYSTEM SET enable_kafka_listener_source = true;
-- Only replicas provisioned after this accept Kafka connections.
ALTER SYSTEM SET enable_replica_kafka_listener = true;

-- As materialize (psql -h localhost -p 6875 -U materialize):
CREATE CLUSTER ingest SIZE 'scale=1,workers=1';
CREATE SOURCE events IN CLUSTER ingest
  FROM KAFKA LISTENER (TOPIC 'events')
  KEY FORMAT TEXT VALUE FORMAT JSON
  ENVELOPE UPSERT;
```

A producer that writes a topic per table, as CDC producers do, writes to one
source with a table per topic. Topics under the prefix without a table are
refused:

```sql
CREATE SOURCE cdc IN CLUSTER ingest
  FROM KAFKA LISTENER (TOPIC PREFIX 'cdc.public.');
CREATE TABLE orders FROM SOURCE cdc (REFERENCE "cdc.public.orders")
  KEY FORMAT JSON VALUE FORMAT JSON ENVELOPE UPSERT;
```

With `VALIDATE ON PRODUCE`, for example
`FROM KAFKA LISTENER (TOPIC PREFIX 'cdc.public.', VALIDATE ON PRODUCE)`, a batch
with a record that its table cannot decode, or that its envelope rejects, such
as a record without a key for `ENVELOPE UPSERT`, fails with `INVALID_RECORD`
instead of becoming an error in the table.

Each replica also serves a read-only schema registry, with the Confluent Schema
Registry API, for tables with an inline Avro schema
(`FORMAT AVRO USING SCHEMA '...'`): the subject `<topic>-value` holds the
table's schema. Confluent serializers can register a schema with the same
canonical form and produce with it, or use the latest version. Authenticate with
the source's SASL credentials as HTTP basic auth. Locally, its port is the
`"mz_orchestrator_port": "kafka-registry"` entry described below.
`registry.py` checks it with Confluent's Python serializer.

Locally, each replica's Kafka port is proxied through a TCP port on localhost.
Find it in `mzdata/prometheus/cluster.json`, under the entry with
`"mz_orchestrator_port": "kafka"` and the cluster's ID:

```
jq -r '.[] | select(.labels.mz_orchestrator_port == "kafka") | "\(.labels.mz_orchestrator_cluster_id) \(.targets[0])"' mzdata/prometheus/cluster.json
```

Then produce with any Kafka client, for example:

```
echo 'k1:{"hello": "world"}' | kcat -b 127.0.0.1:<port> -t events -P -K:
```

## Limitations

- No consumers, except of the compacted topics that Materialize Kafka sinks use
  for progress. One partition per topic.
- A topic can feed several tables, and a table created later is filled from the
  source's log. The topics of a source share one offset sequence, so a topic's
  offsets have gaps.
- Transactions may write to one topic each. Their records wait in the
  replica's memory until they commit.
- No TLS. SASL/PLAIN sends the password in the clear.
- Compression: none, gzip, snappy and lz4. zstd is rejected.
- Idempotent producers (`enable.idempotence=true`) get exactly-once delivery.
  Other producers get at-least-once delivery.
- An acknowledgement means the records are committed to the source's log, in
  about 0.1 s. They become readable in their tables at the source's next
  timestamp tick. Idempotent producers keep few requests in flight: the Java
  client at most 5, and librdkafka at most 5 messages, which is a single request
  once batches hold 5 or more records.
- The source's log keeps every record; there is no retention yet.
- On Kubernetes, clients connect through balancerd, which orchestratord does not
  configure yet. See the design doc.
