# Kafka listener sources

- Associated: (proof of concept, no issue yet)

## The Problem

Getting data into Materialize from an application today means standing up
something in between: a Kafka cluster and a Kafka source, a Postgres database
and a CDC source, or HTTP calls against a webhook source. Webhook sources are
the only "push" ingestion path, and they are a poor fit for high-volume event
streams. Every request is a single HTTP round trip that terminates in
`environmentd`, there is no batching or compression in the client, and
existing producers (Debezium, Kafka Connect, Vector, Fluent Bit, OpenTelemetry
collectors, every application already instrumented with a Kafka client) cannot
use them without rewriting.

The Kafka producer protocol is the de facto standard for shipping event streams.
Nearly every language has a mature, batching, compressing, retrying client for
it. If Materialize could accept Kafka producer traffic directly, any of those
producers could write into Materialize without a Kafka cluster in between,
which removes a whole system from many customers' architectures.

## Success Criteria

- Unmodified Kafka producers (librdkafka, the Java client, franz-go, kcat) can
  write to a Materialize source by pointing `bootstrap.servers` at Materialize.
- Ingestion runs on a cluster replica, not in `environmentd`, so it scales with
  cluster size and is isolated like every other source.
- A producer receives an acknowledgement only once its records are durable in
  persist.
- Idempotent producers get exactly-once delivery: a resent batch is ingested
  once, including across restarts. Other producers get at-least-once delivery.
- The source supports the formats, envelopes and `INCLUDE` metadata of Kafka
  sources (`ENVELOPE UPSERT`, `ENVELOPE NONE`, `FORMAT JSON/TEXT/BYTES/AVRO`,
  `INCLUDE KEY/OFFSET/TIMESTAMP/PARTITION/HEADERS`).
- Producers can be required to authenticate with a username and password.

## Out of Scope

These are needed for production but are not part of the proof of concept. The
design accounts for them, see [Future work](#future-work).

- TLS between producers and replicas. With balancerd, TLS ends at balancerd.
- Configuring the Kafka hostname and balancerd's Kafka port through
  orchestratord. See [Kubernetes](#kubernetes).
- Multiple partitions per topic.
- Consuming from a source's topic. `Fetch` serves only the compacted topics
  that [Materialize Kafka sinks](#materialize-kafka-sinks) use for progress.
- Metrics specific to the listener.
- Registering writer schemas, and choosing the envelope per source, see
  [Topic contracts](#topic-contracts).
- Retention of the source's log, which keeps every record.

## Solution Proposal

### SQL

```sql
CREATE SOURCE events
  IN CLUSTER ingest
  FROM KAFKA LISTENER (
    TOPIC 'events',                           -- optional, defaults to the source name
    SASL USERNAME 'producer',                 -- optional, together with SASL PASSWORD
    SASL PASSWORD SECRET producer_password,
    VALIDATE ON PRODUCE                       -- optional, see Validation on produce
  )
  KEY FORMAT TEXT VALUE FORMAT JSON
  INCLUDE OFFSET, TIMESTAMP, HEADERS
  ENVELOPE UPSERT;
```

`FROM KAFKA LISTENER` reads as "Materialize listens for Kafka producers", in
contrast to `FROM KAFKA CONNECTION`, where Materialize connects to a broker.
Purification records the defaulted `TOPIC` in the `create_sql`, so renaming the
source does not change the topic producers write to.

A producer that writes a topic per table, as CDC producers do, writes to one
source with a table per topic, see [Tables per topic](#tables-per-topic):

```sql
CREATE SOURCE cdc IN CLUSTER ingest
  FROM KAFKA LISTENER (TOPIC PREFIX 'cdc.public.');
CREATE TABLE orders FROM SOURCE cdc (REFERENCE "cdc.public.orders")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr
  ENVELOPE DEBEZIUM;
```

The source is gated by the `enable_kafka_listener_source` feature flag.

### Architecture

```
 producer ──Kafka protocol──▶ clusterd (replica of cluster `ingest`)
                               ├─ KafkaServer (one per process, port `kafka`)
                               │    topic registry: "events" ─┐
                               └─ ingestion dataflow          │ ProduceBatch
                                    KafkaListener source ◀────┘
                                      │ compare_and_append: records and batch rows
                                      ▼
                                    log shard ──committed──▶ ack to the producer
                                      │ emit to every table of the topic
                                      ▼ SourceMessage{key, value, metadata}
                                    decode ─▶ envelope (upsert) ─▶ persist (table)
```

**The broker is the replica.** Each `clusterd` process runs one
`mz_kafka_server::KafkaServer` on a new `kafka` service port. It presents itself
as a single-broker Kafka cluster whose topics are the Kafka listener sources
running on the replica. A source's ingestion dataflow registers its topic with
the server when it starts and deregisters it when it is dropped. This mirrors
how the replica already serves the storage and compute controller protocols, and
it means a cluster is the unit of placement, scaling and isolation for Kafka
ingestion like for every other source.

**The protocol implementation** lives in a new crate, `mz-kafka-server`, built on
the [`kafka-protocol`](https://crates.io/crates/kafka-protocol) crate. That crate
is generated from Kafka's own message schemas, covers every API and version,
and is pure Rust. Compression codecs are limited to pure-Rust ones: none, gzip,
snappy, and lz4 through `lz4_flex`, which Materialize Kafka sinks use by default.
zstd producers receive `UNSUPPORTED_COMPRESSION_TYPE`.
The server implements `ApiVersions`, `Metadata`, `Produce`, `InitProducerId`,
`SaslHandshake`/`SaslAuthenticate` with `PLAIN`, the transaction APIs
`FindCoordinator`, `AddPartitionsToTxn` and `EndTxn`, and `CreateTopics`,
`ListOffsets` and `Fetch` for compacted topics, see
[Materialize Kafka sinks](#materialize-kafka-sinks). It
knows nothing about timely or storage. Its interface is "register a topic, get a
stream of `ProduceBatch`es, acknowledge each one".

**The source reuses the Kafka source pipeline.** The source implements
`SourceRender` and emits exactly the `SourceMessage` a Kafka source emits for
the same record: raw key and value bytes, plus the requested metadata columns
in the same layout. Everything downstream (decoding, upsert, persist sink,
reclocking, statistics, status) is shared with Kafka sources. The planner reuses
`KafkaMetadataKind`, `kafka_metadata_columns_desc` and `SourceExportDetails::Kafka`,
and purification reuses `SourceFormatOptions::Kafka`, so
`FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY` resolves subjects from the topic
name just like a Kafka source.

**Time.** Each topic has one partition. The source assigns each record the next
offset of one sequence that all its topics share, and uses `MzOffset` as its
timestamp. A topic's offsets therefore have gaps when its source serves several
topics. Worker 0 runs the source, because
clients reach a replica's process 0 only and worker 0 is always in process 0.
The source takes the batches that are waiting as a group, and advances its
frontier only past whole groups.

Worker 0 only sequences: it assigns offsets, deduplicates, appends to the log
and acknowledges. It hands each committed group's records to an exchange that
spreads them over all workers in runs of 1000 offsets, and each worker turns its
share into `SourceMessage`s at their offsets. With worker 0 also building every
message, it was 92 percent busy at 16 workers while the others were 17 percent
busy.

**The log.** Every batch the source accepts is appended to the source's log, a
persist shard, with its records as the producer sent them: key, value, headers
and timestamp. A batch is acknowledged once its append commits, and its records
are then emitted to every table of its topic. The log is the durable copy of the
records, and the tables are derived from it, like consumers of a Kafka topic.
Producers with `acks=0` get no response, as in Kafka. An acknowledgement takes one
persist append, about 0.1 s, whatever the source's timestamp interval. The records
become readable in their tables at the next timestamp tick.

On restart, the source reads the log and sends each table the records at and
beyond its upper, so a restarted table catches up exactly, and a table created
later starts from the beginning of its topic. An append that did not commit was
never acknowledged, so its producers send its batches again. Offsets continue
after the greatest offset in the log or in any table.

The log's ID is derived from the source's remap shard, so it needs no catalog
entry, and the storage controller finalizes it when the source is dropped. Its
timestamps count appends. Record rows are never retracted, so the log grows with
every record, see [Log retention](#log-retention).

**Appending.** Appends commit in groups. An append starts at most every 50 ms,
and at most two are in flight, so batches that arrive meanwhile join the next
one. Each append is split into persist batches of at most 8 MiB that build on
their own tasks and commit together in one `compare_and_append_batch`, the way
`persist_sink` writes, within a 256 MiB budget of entries being built. Every
append costs a blob write, a consensus write and a batch for compaction to merge,
whatever its size: at 400k records/s, the 50 ms interval halved the appends and
cut the replica's CPU by 15 percent without changing ack latency. Building every
append on one task capped a source at 700k to 900k records/s whatever the
replica's size. The log's rows lead with the offset, because persist sorts and
merges rows column by column; with the key and value first, row comparisons took
1.07 of the 3.7 cores the replica used at 400k records/s, and 0.45 after.

**Exactly once for idempotent producers.** Each batch from an idempotent
producer carries its producer ID, epoch and first sequence number.
`mz_kafka_server::ProducerStates` implements the broker's rules: the last 5
batches of each producer are remembered, a resent batch is acknowledged with the
offset it was first appended at and not appended again, a gap returns
`OUT_OF_ORDER_SEQUENCE_NUMBER`, and an old epoch returns
`INVALID_PRODUCER_EPOCH`. Producers idle for 24 hours are forgotten, as with
Kafka's default `producer.id.expiration.ms`. Producer IDs are random 63-bit
numbers, because the producer state outlives the replica that handed out an ID.

The producer state lives in the log too, as a row per batch of an idempotent
producer, committed in the same append as the batch's records. A batch is
therefore in the log with its records or not at all, and recovery restores
exactly the batches whose records are durable. Batch rows are retracted once
their batch is forgotten, so the log holds a few batch rows per producer.

`compare_and_append` also fences writers. If another replica appends to the log,
for example during a failover, this one's next append fails with an upper
mismatch and the source restarts and reads the log again. Overlapping replicas
are untested.

Batches from other producers are ingested at least once. A batch whose
acknowledgement is lost is ingested again when the producer resends it. Under
`ENVELOPE UPSERT`, a resent batch can land after a newer write to the same key
and overwrite it. Idempotent producers rule this out, because a gap in sequence
numbers is rejected.

Deduplication cannot be left to the upsert operator. It keeps the record with
the highest offset per key, and a resent batch always gets a newer offset than
the write it repeats. It must happen before offsets are assigned.

**Backpressure.** The server bounds each topic's queue of batches in bytes, and
a connection stops reading after 128 requests without responses. The source has
a budget of an eighth of the replica's memory for records it has taken but not
every table of their topic has committed. It stops taking a topic's batches while
that topic holds more than half the budget, and all topics' while the whole
budget is used. Sizes count a record's in-memory size, not only its key and
value. Slow ingestion therefore pushes back on producers instead of buffering
without bound, and a table that falls behind delays its topic's producers, see
[Lagging tables](#lagging-tables). The budget also bounds throughput: a topic
carries at most its share divided by the time its records take to commit, about
1.5 s with a 1 s tick. A fixed 256 MiB per topic held a 32-worker replica to
1.4M records/s.

Idempotent producers keep little in flight per partition. The Java client keeps
at most 5 requests in flight. librdkafka keeps at most 5 messages in flight
(`RD_KAFKA_IDEMP_MAX_INFLIGHT`, compared against the partition's in-flight
message count), which with batches of 5 or more records is a single request.
Measured against the listener: peak 1 request in flight with idempotence and
batches of 10, and 5 without idempotence. Since an acknowledgement takes one log
append, an idempotent producer still sends several batches a second, and the
clients' CPU is the first limit: an idempotent librdkafka producer uses several
times the CPU per record of a plain one.

**Performance.** Measured with `misc/kafka-listener/perf.py`, 16 librdkafka
producers sending 100-byte JSON records as fast as they could to an
`ENVELOPE NONE` source, on an AWS m6id.16xlarge (64 vCPUs) with persist's blobs
on local NVMe and 32 GiB replicas. Every run ended with exactly the acknowledged
records in Materialize, except that plain producers on 1 worker retried long
waits for acks, which at-least-once delivery duplicates.

| Replica | Plain producers (at least once) | Idempotent producers (exactly once) |
| --- | --- | --- |
| 1 worker | 212k records/s | |
| 4 workers | 976k | |
| 16 workers | 1.71M, 21.0 cores | 1.51M |
| 32 workers | 1.97M, 26.7 cores | |

On a laptop with 8 workers and producers offering 25,000 records/s each, acks took
0.07 to 0.16 s at the median while the replica kept up, and plain producers
filled it near 950k records/s. At 400k records/s on 16 workers the replica used
8.0 cores and 6.1 GiB. Writing every record to persist twice costs about twice
the CPU and memory of [acknowledging at the table's
commit](#acknowledge-at-the-tables-commit). The extra CPU goes to encoding the
log's batches, compacting the log and blob writes. The extra memory is persist
compaction's decoded parts and merge buffers: a heap profile at 500k records/s
found the listener holding about 10 MB. Persist bounds each compaction by
`persist_compaction_memory_bound_bytes`, 1 GiB whatever the replica's size, so a
4 GiB replica at 500k records/s ran out of memory, and fit with the bound at
256 MiB. These are persist costs, compaction memory that follows the replica's
size and per-append overheads, to optimize in persist rather than in the
listener.

**Authentication.** A connection is anonymous unless it authenticates with
SASL/PLAIN. Anonymous connections can produce to sources without credentials.
Authenticated connections can also produce to sources whose `SASL USERNAME` and
`SASL PASSWORD` equal the presented credentials. The password is a Materialize
secret, read by the replica through its secrets reader, the same way Kafka
connections read theirs. Credentials compare in constant time.

**Replica placement.** The connection reports `prefers_single_replica`, so the
storage controller runs it on exactly one replica. A topic is served from one
place, which is what producers expect of a partition leader. The source does
not support read-only mode: a read-only ingestion cannot make records durable
and would never acknowledge them.

### Tables per topic

With a source per topic, a CDC producer that writes a topic per table needs one
`CREATE SOURCE` per table, and a produce to a topic without a source gets the
retriable `UNKNOWN_TOPIC_OR_PARTITION` until it exists. A source with a
`TOPIC PREFIX` has no relation of its own: its log holds the records of all its
topics. Each `CREATE TABLE .. FROM SOURCE` names a topic under the prefix as its
`REFERENCE`, and the source registers each table's topic with the server. A topic
can feed several tables, each with its own format, and a table created later is
filled from the log. A topic under the prefix without a table is refused like an
unknown topic. One endpoint, credential and TLS setup serves
every table, and adding a table is a catalog operation. The table's topic is its
schema registry subject's topic, so `FORMAT AVRO USING CONFLUENT SCHEMA
REGISTRY` resolves `<topic>-value`.

Auto-creating a source when a producer first writes a topic was rejected. The
replica would have to issue DDL, something would have to choose the name, owner,
format and envelope, and a mistyped topic name would create a catalog object.

**Each table behaves like its own Kafka source.** With a Kafka source per topic,
which is how CDC topics are ingested today, every table keeps its own offsets,
recovers on its own and lags on its own, and nothing is consistent across
tables. Tables of a listener source keep those semantics: acknowledgements wait
for the log, not for any table, each table is sent records from its own upper,
and the budget for uncommitted records is shared per topic. Two things remain
shared: the source's worker sequences every topic, and when the source restarts
with `ENVELOPE UPSERT` tables, it accepts records only once every table has
rehydrated, like other multi-table sources.

Sharing offsets is what lets the tables lag independently: a table's upper says
how far it committed, whatever the other tables did. A timestamp partitioned by
topic, like a Kafka source's is partitioned by partition, would need a numeric
partition per topic stored in the catalog, and gains nothing over shared offsets.

A transaction still writes one topic. Making transactions across topics atomic,
which Kafka Connect source tasks with exactly-once support need, is future work.

### Validation on produce

Without validation, records are acknowledged once durable and decoded
afterwards, so a record that does not match its table's format is accepted. It
then becomes a decode error: permanent with `ENVELOPE NONE`, and with
`ENVELOPE UPSERT` until its key is overwritten or deleted. Producers that use a
schema registry serializer rarely send such records, but producers that encode
by hand get no check.

`VALIDATE ON PRODUCE` rejects such records instead. The source registers each
topic with a validator built from its table's encoding and envelope. The server
runs it on the producer's connection task, before the batch reaches the topic or
joins a transaction, so validation runs concurrently per connection on the
replica's async runtime, not on the worker that sequences the topics. The
validator decodes with the dataflow's own decoders, pooled because they are
stateful, and applies the envelope checks that decoding does not: a record
without a key under `ENVELOPE UPSERT`, and a key without a value under
`ENVELOPE NONE`. A batch with any invalid record fails as a whole with
`INVALID_RECORD`, with an `error_message` and per-record `record_errors` (KIP-467)
for `Produce` version 8 and later. Records are decoded twice, once to validate
and once in the dataflow. With `VALUE DECODING ERRORS = INLINE`, value errors are
rejected too.

A registry that cannot be reached, and a schema ID it does not know, both end
the Avro decoder's retries with a transient error, as they do in the dataflow.
The batch then fails with the retriable `KAFKA_STORAGE_ERROR`, and the producer
retries until its delivery timeout.

Requests of a connection are dispatched one at a time, which keeps its batches
in order, while a separate writer returns responses in order. A batch that takes
long to validate therefore delays the connection's later requests, but not the
acknowledgements of its earlier ones.

**Rejection in the middle of an idempotent sequence.** A rejected batch never
reaches the topic's producer state, so batches the producer sent after it fail
with `OUT_OF_ORDER_SEQUENCE_NUMBER`, as with a Kafka broker. Both clients
recover by bumping the epoch of the same producer ID and resending from sequence
0, which the producer state accepts as a new epoch. Tested with librdkafka
(single-record batches, the only way it pipelines idempotent batches) in
`mz-kafka-server`'s tests, and end to end with librdkafka and the Java client
3.8: every acknowledged record was ingested once and nothing else. The Java
client uses the record errors to fail the invalid records with
`InvalidRecordException` and the rest of their batch with a generic
`KafkaException`, and does not resend them, as against a Kafka broker. Inside a
transaction, the rejection fails the produce, which clients treat as an
abortable error. That case is not tested.

The option is per source. A prefix source applies it to all its tables.

### Schema registry

A schema registry matters when producers use serializers that talk to one, which
Confluent's serializers do: by default they register their schema and encode
records with the returned ID. A table with an inline schema
(`FORMAT AVRO USING SCHEMA`) has no registry, so such producers cannot write to
it without an external registry that holds the same schema, and a schema
registry connection's URL may include a path for registries that serve the API
under one.

Each replica that accepts Kafka connections also serves a read-only registry for
its Kafka listener sources' topics, on a `kafka-registry` port, with the subset
of the Confluent Schema Registry API that serializers and registry clients use:
list subjects, versions of a subject, a version, a schema by ID, the subjects of
an ID, register, and look up. A table whose key or value is an inline Avro schema
with the Confluent wire format has the subjects `<topic>-key` and `<topic>-value`,
with that schema as their only version. Registering a schema succeeds only if it
has the table's schema's Parsing Canonical Form, which drops docs, defaults and
logical types but keeps everything that determines the encoding, and returns the
table's ID. Any other schema gets 409. IDs are derived from the canonical form,
so they are the same on every replica and across restarts.

Only inline schemas are served, because their decoder ignores the ID in each
record. A table whose schema comes from a schema registry connection resolves IDs
in that registry, where an ID from this one names another schema, or none.

A subject is readable with the access its topic requires to produce: the source's
SASL credentials, as HTTP basic auth. Wrong credentials get 401, and topics the
request may not produce to are not listed. Locally, the registry port is proxied
like the Kafka port. On Kubernetes, the controller keeps a second endpoint,
`<cluster ID>-kafka-registry`, next to the Kafka one, and balancerd proxies it
like Kafka connections (see [Kubernetes](#kubernetes)), at the same hostname.

The registry lives on the replica, not in `environmentd`. Serving a table's
schema could happen in either. Registering writer schemas, the
[next step](#topic-contracts), needs durable storage that validation
on produce and decoding read, and the source's log on the replica is that
storage. In `environmentd`, subjects would become a new catalog object type,
validation would need them shipped to the replica, and producers would need a
second identity, their Materialize role, where the replica authenticates them
with the source's credentials as the Kafka listener does. A registry in
`environmentd` could serve Materialize sinks, and subjects without a source,
which the replica cannot. Schema IDs are unique per cluster.

Tested with Confluent's Python `AvroSerializer`, registering an equivalent schema
and with `use.latest.version`, against a table with `VALIDATE ON PRODUCE`
(`misc/kafka-listener/registry.py`). The Java serializer is not tested.

### Networking and discovery

The controller adds a `kafka` service port (hint 9092) and a `kafka-registry`
port (hint 8081) to replicas and passes `--kafka-listen-addr`,
`--kafka-advertised-addr` and `--kafka-registry-listen-addr` to `clusterd`. Adding a
port changes a Kubernetes StatefulSet, which restarts every replica, so this is
gated by the `enable_replica_kafka_listener` dyncfg and applies only to replicas
provisioned after it is set.

Kafka clients use the bootstrap address only for the first request. Every
following request goes to the address the broker advertises in its `Metadata`
response, so the advertised address must be reachable by clients.
`ServiceAssignments` gains `external_addrs`, the address at which clients outside
the orchestrator reach a port, if the orchestrator provides one:

- The process orchestrator listens on Unix sockets, and with
  `--orchestrator-process-tcp-proxy-listen-addr` (set by `bin/environmentd`) it
  proxies each port through a TCP port on localhost. That proxy is the external
  address. Locally, the proxies are also listed in
  `mzdata/prometheus/cluster.json`, which is how the demo finds the endpoint.
- The Kubernetes orchestrator provides none. There, every replica of a cluster
  advertises the cluster's address through balancerd, which environmentd's
  `--kafka-listener-advertised-addr-template` gives, with `{cluster_id}`
  standing for the cluster's ID.

### Kubernetes

```
 producer ──TLS, SNI 7vifiksqeftxc6ld3r6zvc8n2-u7.lb.<region>:9093──▶ balancerd
   balancerd: terminate TLS, first SNI label = <environment>-<cluster ID>
     │  --kafka-sni-resolver-template
     ▼  e.g. cluster-{cluster}-kafka.<namespace>.svc.cluster.local:9092
 Service cluster-u7-kafka (ClusterIP, one per cluster, owned by environmentd)
     │  selector statefulset.kubernetes.io/pod-name=cluster-u7-replica-u12-gen-0-0
     ▼
 replica u12, process 0, port 9092 ── SASL/PLAIN, Produce ──▶ source
```

A cluster's Kafka listener sources run on one replica: the storage controller
runs single-replica ingestions on the replica with the lowest ID, and moves them
only when that replica is dropped. Within the replica, they run on worker 0,
which is in process 0. So clients must reach process 0 of the lowest-ID replica.

`NamespacedOrchestrator` gains endpoints: `ensure_endpoint(id, port, target,
process)` gives a port a stable address that routes to one process of a service,
and `drop_endpoint` removes it. The Kubernetes orchestrator implements an
endpoint as a ClusterIP Service selecting the target pod by its StatefulSet
`pod-name` label. A cluster IP stays the same when the target changes, so
balancerd needs no DNS refresh. The process orchestrator does nothing, because
local clients connect to each replica's own TCP proxy. Endpoints are not
StatefulSets, so `list_services` does not report them.

The controller keeps each cluster's endpoints, named `<cluster ID>-kafka` and
`<cluster ID>-kafka-registry`, pointed at process 0 of the cluster's lowest-ID replica, whenever it creates or
drops a replica. It drops the endpoints when that replica does not accept Kafka
connections, and when the cluster is dropped. A read-only controller leaves
endpoints alone. During a zero-downtime upgrade, the new generation repoints
them once it is promoted and restarts writable.

balancerd gains an optional Kafka listener, `--kafka-listen-addr` with
`--kafka-sni-resolver-template`. It terminates TLS with the certificate it
already serves, reads the first label of the SNI hostname as
`<environment>-<cluster ID>`, substitutes them for `{}` and `{cluster}` in the
template, and proxies the connection unencrypted to the address that resolves
to. Only labels ending in a cluster ID (`u` or `s` and digits) are routed, so a
client cannot steer the lookup to another name. Producers authenticate to the
replica with SASL inside the TLS session, so balancerd needs no Kafka protocol
support. `--kafka-registry-listen-addr` with
`--kafka-registry-sni-resolver-template` proxies schema registry connections the
same way, for example to
`cluster-{cluster}-kafka-registry.<namespace>.svc.cluster.local:8081`.

Not done yet:

- orchestratord does not yet pass the Kafka flags. It needs a Materialize
  resource field for the external Kafka hostname, which it passes to
  environmentd, and a balancerd Kafka port in the balancerd Deployment, Service
  and ingress NetworkPolicy. The existing within-environment NetworkPolicy
  already lets balancerd reach clusterd. Self-managed deployments then route a
  wildcard DNS name and load balancer port to balancerd's Kafka port.
- In Materialize Cloud, the SNI label names the environment by its resource ID,
  while namespaces are named after the tenant. The resolver template needs a
  per-environment DNS name to bridge the two, like the `blncr-<label>` CNAMEs
  that balancerd's HTTPS routing resolves today.
- None of this has run on a Kubernetes cluster yet. balancerd's routing is
  covered by a unit test of the SNI parsing and an integration test that proxies
  a TLS connection to a backend.

### Materialize Kafka sinks

A Materialize Kafka sink can write to a Kafka listener source, which moves a
collection from one cluster or environment into another, exactly once. The sink
needs three things from a broker:

1. **Transactions.** The sink writes each timestamp's changes and a progress
   record in one transaction, with a `transactional.id` per sink.
2. **A progress topic.** The sink creates it with `CreateTopics` if it is
   missing. On every start, after `InitProducerId` fences its predecessor, the
   sink reads the topic from `ListOffsets`'s earliest offset to its high
   watermark, with `read_committed` isolation, and resumes from the last
   progress record with its key.
3. **The data topic**, found through `Metadata`. A source's topic exists while
   the source runs.

**The server is its own transaction coordinator.** `FindCoordinator` points at
the server. `InitProducerId` with a `transactional.id` returns the ID's producer
ID with a new epoch, which aborts its open transaction and fences older
producers. Produced records of a transaction are buffered in memory until
`EndTxn`. A commit hands them to the source as one `ProduceBatch`, and `EndTxn`
answers once the source acknowledges the batch, that is once the records are
committed. The source puts a batch into one group, so a transaction is committed
whole or not at all. A transaction may write at most one claimed topic, which
`AddPartitionsToTxn` enforces. Transaction state lives in memory: after a
replica restart, every producer must call `InitProducerId` again.

**Progress topics are compacted topics stored by the source.** A topic created
with `CreateTopics` is a compacted topic: the server serves the latest record of
each key, and stores nothing itself. A transaction's records for compacted topics
travel with its batch to the source, which writes them to its
[log](#architecture) and acknowledges the batch only after they commit there.
When the source registers its topic again after a restart, it hands the server
the latest record of each key. So a sink's progress record commits atomically
with its data, in the same source. A transaction that writes only progress, which
a sink sends when time passes without changes, goes to the source that stores the
record's key. If no source stores it yet, the server keeps it in memory only. That
loses nothing, because a sink without a stored progress record has written no
data.

Three rules keep the progress record from running ahead of the data or behind
it:

- **A compacted row is durable exactly when its data is.** The row commits in
  the same log append as its transaction's records, and records the offset after
  them.
- **Retract the old row only after the new one is acknowledged.** Otherwise
  losing the new row in a restart would also lose the old one, and the sink would
  resend everything. Rows carry a version so recovery can pick the latest
  durable one.
- **Withhold records of a stopped source.** When a source's topic is released,
  for example when the source restarts, its records may be older than what the
  source stored, since the source may have committed a transaction it never
  acknowledged. Until the topic is registered again, `Fetch` on a compacted
  topic that holds such records fails with a retriable error, and so do commits
  that would go to that source. After 5 minutes without a new registration, which
  is what happens when the source was dropped, the records are forgotten.

Limitations: a transaction's records are held in the replica's memory until it
commits, and a sink's first transaction holds its whole snapshot. Compacted
topics have one partition. Compacted records are readable by anyone who may
produce to the topic of the source that stores them.

### Code map

| Piece | Location |
| --- | --- |
| Protocol server and topic registry | `src/kafka-server` |
| AST, parser | `src/sql-parser/src/ast/defs/ddl.rs` (`CreateSourceConnection::KafkaListener`) |
| Purification, planning | `src/sql/src/pure.rs`, `src/sql/src/plan/statement/ddl.rs` |
| Connection type | `src/storage-types/src/sources/kafka_listener.rs` |
| Source operator | `src/storage/src/source/kafka_listener.rs` |
| Log shard and appender | `src/storage/src/source/kafka_listener/log.rs` |
| Deduplication rules | `src/kafka-server/src/idempotence.rs` |
| Validation on produce | `src/storage/src/source/kafka_listener/validate.rs` |
| Schema registry | `src/kafka-server/src/registry.rs` |
| Transactions | `src/kafka-server/src/transaction.rs` |
| Log finalization on drop | `src/storage-client/src/storage_collections.rs` |
| Replica wiring, endpoints | `src/clusterd/src/lib.rs`, `src/controller/src/clusters.rs` |
| Kubernetes endpoints | `src/orchestrator-kubernetes/src/lib.rs` (`ensure_endpoint`) |
| balancerd routing | `src/balancerd/src/lib.rs` (`KafkaBalancer`) |
| Demo | `misc/kafka-listener` |

## Minimal Viable Prototype

`misc/kafka-listener/README.md` walks through a local demo. `mz-kafka-server`
has integration tests that drive it with librdkafka producers: plain, gzip,
snappy and lz4 compression, idempotent producers, headers, tombstones, SASL/PLAIN
success and failure, unknown topics, and failed acknowledgements. Others drive
transactions and compacted topics with a librdkafka transactional producer,
admin client and `read_committed` consumer: commits, aborts, progress-only
transactions, fencing, and withholding records while their topic is released.
Unit tests cover the deduplication rules and the log's row encoding.

`misc/kafka-listener/exactly_once.py` checks exactly-once delivery end to end.
It speaks the Kafka protocol directly to resend identical batches, before and
after a replica restart, and checks the offsets returned and the rows ingested.
It also restarts the replica while a batch's response is outstanding, and
checks that resending the batch ingests it exactly once.
It then runs librdkafka idempotent producers before and after a restart and
checks that Materialize holds exactly the acknowledged records.
`misc/kafka-listener/tables.py` checks a source with a topic prefix: idempotent
producers write to two tables and a single-topic source, a third table and a
second table on a topic that already has records are added while they run, and
the replica is killed, after which every acknowledged record must be in every
table of its topic exactly once.
`misc/kafka-listener/perf.py` measures throughput, ack latency, and the replica's
CPU and peak memory per replica size and number of producers.

One finding from prototyping: librdkafka only produces record batch format v2
(which carries headers and producer IDs) to brokers that advertise both
`Produce` v3 and `Fetch` v4. The server advertised `Fetch` for that reason
before it served compacted topics.

## Alternatives

### Terminate in `environmentd`, like webhook sources

The webhook appender path is simple, but it puts ingestion load on the
coordinator's process, does not scale with cluster size, and would need its own
decoding and upsert. Running on the replica reuses the whole source pipeline.

### A separate ingest service

A standalone "Kafka gateway" that writes to persist would duplicate the source
pipeline and reclocking, and add a new service to operate. Hosting the listener inside `clusterd` gets placement,
restarts, status reporting and statistics from existing machinery.

### Reuse `KafkaSourceConnection` with a flag

It requires a Kafka connection object, and most of its fields (start offsets, group ID prefix, metadata refresh)
mean nothing here. A separate connection type is clearer, and the shared parts
(metadata columns, export details, formats) are reused at the type level.

### Acknowledge at the table's commit

A log of batch metadata only, with a batch acknowledged once its table commits
it, writes every record to persist once. At 400k records/s on 16 workers it used
4.0 cores and 2.5 GiB against the log's 8.0 cores and 6.1 GiB, and 32 workers
reached 2.48M records/s flat out against 1.97M. It was rejected for what it
costs everything else:

- Every ack waits for the next timestamp tick: 0.79 s at the median at 400k
  records/s, against 0.11 s.
- An idempotent producer, with one request in flight, commits about one batch
  per tick. 16 idempotent producers leveled off near 800,000 records/s whatever
  the replica's size.
- Clients that wait for acks slow down with them. Debezium Server sent about 500
  events a second with its default batching and fell minutes behind, and a
  Materialize Kafka sink, which commits a transaction per timestamp, fell 18 s
  behind at two statements a second.
- A topic can feed only one table, and a table created later never sees the
  topic's earlier records. A batch one table committed and another did not would
  be ingested into the first again after a restart.

### Log every record, retract it once committed

Writing every record to a log, acknowledging there, and retracting it once the
table committed it, writes every record to persist three times. Log compaction
starved the table's writes, and a local 8-worker replica peaked at 687,000
records/s.

### A Rust wrapper around a C or JVM broker

We need only a small slice of the broker, and `kafka-protocol` gives us exact
wire compatibility without a foreign runtime.

## Open questions

- Production defaults of `enable_kafka_listener_source` and
  `enable_replica_kafka_listener`. Both default to off. This is a product
  decision.
- Should a cluster's Kafka address be visible in SQL, for example in
  `mz_internal.mz_kafka_listener_endpoints`, or only in the Console?
- The name of the Materialize resource field for the external Kafka hostname.
- Should a topic be able to span several sources, for example to feed a source
  and a table from the same producer?
- How long the log keeps records, see [Log retention](#log-retention).

## Future work

### Topic contracts

The log already holds every topic's records as the producer sent them, and
tables decode them. The next step lets tables of a topic evolve independently:
several tables per topic with their own reader schemas, and producers that
register writer schemas without breaking tables.

**The envelope belongs to the source.** `ENVELOPE NONE` or `ENVELOPE UPSERT` is
chosen on `CREATE SOURCE` and inherited by every table, which only chooses a
format. An upsert log keeps the latest record of each key per topic, compared as
raw key bytes, so a table reading it applies the log's changes, decoding both
the retracted and the inserted value, and needs no upsert state of its own.

**Writer and reader schemas.** A writer schema describes how a record's bytes are
laid out, and belongs to the producer; each record names its own by ID. A reader
schema describes a table's columns, and belongs to the table. Decoding reads the
bytes with the writer schema and resolves them against the reader schema: reader
fields missing from the writer take their defaults, and writer fields the reader
lacks are dropped. Producers register writer schemas with the cluster's schema
registry, which stores them in the source's log, and tables resolve record IDs
through it.

**Three checks keep every accepted record readable by every table:**

- On produce, a record's ID must name a schema registered under its subject, and the
  record must decode with that schema. Otherwise the batch fails with
  `INVALID_RECORD`. A registered key schema also makes a key required.
- On registering a writer schema, every table on the topic must be able to read it.
  Otherwise the registry answers 409 and names the tables that cannot.
- On `CREATE TABLE`, the table's reader schema must be able to read every writer
  schema registered for its topic. Otherwise the statement fails.

Validation on produce therefore never consults a table, and its cost does not grow
with the number of tables. Registration and table creation each compare schemas
once. A topic with no registered schema is validated by nothing, like a Kafka topic,
and its tables can hold decode errors, as can rules that no schema states, such as
a `REGEX` table's pattern. JSON topics are covered once producers register a JSON
Schema.

**Open questions:**

- Under `ENVELOPE UPSERT`, the log compacts on raw key bytes, which start with
  the key schema's ID. A new key schema would make the same logical key a second
  key. Allowing a single key schema per topic under upsert avoids this. Schema IDs
  are derived from the canonical form, so an identical schema keeps the same bytes.
- A Debezium delete event is a key's latest value in an upsert log. The row
  disappears with the tombstone that Debezium sends after it by default. Either
  require tombstones, or have the log recognize Debezium deletes.
- Whether the log is the source's own relation, queryable as raw records, or
  stays an internal shard.

### Lagging tables

The budget for uncommitted records counts records emitted to a topic's tables
until the slowest one commits them, so a table that falls behind delays its
topic's producers. Since every record is in the log, a lagging table could
instead read its records back from the log, and stop counting against the budget.

### Streaming replay

On restart, the source reads the records to replay into memory before sending
them to tables. A table created after many records makes that large. Reading the
log in bounded chunks would bound it.

### Log retention

The log keeps every record, so it grows without bound. Retention for an
append-only log is out of scope for now; an upsert log, see
[Topic contracts](#topic-contracts), keeps only the latest record of each key.

### Partitions across replicas and processes

Every Kafka listener source of a cluster runs on worker 0 of one replica. A
cluster with heavy Kafka ingestion could spread topics across workers and
processes, with each process advertising itself as a separate broker, and an
endpoint per process.

### Durable transaction state

Transaction state lives in the replica's memory, so a replica restart fences
every transactional producer, and an open transaction cannot outlive it. For
Materialize sinks that is enough, because they restart on any error. Storing
epochs per `transactional.id` in the log would let other transactional
applications ride through restarts.

### Consuming

Serving `Fetch` for a source's topic from its persist shard would make
Materialize readable by Kafka consumers, a Kafka-compatible view of any
collection.

### Partitions

More partitions would let producers spread load across workers. The source
timestamp would become `Partitioned<RangeBound<i32>, MzOffset>` like Kafka
sources, with partitions assigned to workers, and leaders spread across the
processes of a multi-process replica.

### Metrics

Per-topic request rate, batch size, acknowledgement latency, authentication
failures, and connection counts.
