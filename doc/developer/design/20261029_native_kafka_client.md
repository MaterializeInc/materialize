# Native Rust Kafka client

- Associated: replaces the `rdkafka`/`rdkafka-sys` crates (librdkafka) with
  `mz-kafka-client` (`src/kafka-client`).

## The Problem

Every Kafka interaction in Materialize (sources, exactly-once sinks, connection
validation, purification, real-time recency, testdrive, kgen) goes through
rust-rdkafka, a wrapper around librdkafka, a large C library built from source
with CMake. This has costs that compound:

* **Resources.** librdkafka runs at least two threads plus one thread per broker
  for every client. A clusterd with many sources and sinks runs hundreds of
  threads that mostly sleep. Each idle client holds about 1 MB. With the
  `fetch.message.max.bytes` of 128 MiB that Materialize's sources set, a single
  consumer of an 8-partition lz4 topic peaks at 350 to 480 MB.
* **Control.** Behavior Materialize depends on is spread across C code, a
  patched fork of rust-rdkafka, and the text of librdkafka log lines that
  `MzKafkaError::from_str` parses. Callbacks that need the Tokio runtime
  (`resolve_broker_addr` for SSH tunnels and PrivateLink, OAuth token generation
  for AWS IAM) run on librdkafka threads and call `Handle::block_on`.
* **Safety and build.** Memory safety of the whole Kafka path rests on C code
  and FFI. The C build adds CMake, a vendored zlib and OpenSSL configuration,
  and a fork that must track upstream.

## Success Criteria

1. No `rdkafka`, `rdkafka-sys` or librdkafka anywhere in the workspace.
   The new crate is `#![forbid(unsafe_code)]`.
2. Every Materialize test that involves Kafka or Redpanda passes, including the
   broker-version matrix and upgrades from librdkafka-based releases.
3. Measurably lower CPU per message and lower memory than librdkafka for
   Materialize's access patterns, with throughput at least equal.

## Out of Scope

* GSSAPI/Kerberos. Our librdkafka build had no libsasl2 either. The exact
  failure text `No provider for SASL mechanism GSSAPI` is preserved.
* Brokers older than Kafka 0.11 (Produce v3, Fetch v4). Exactly-once sinks
  already required 0.11. Legacy MessageSet v0/v1 data stored in topics is still
  decoded.
* Arbitrary librdkafka configuration pass-through. The client accepts every key
  Materialize, testdrive and CI set, accepts and ignores a documented list of
  tuning keys, and rejects other keys with librdkafka's error text.
* Consumer group protocols other than the classic eager one (range,
  roundrobin). Only testdrive subscribes to groups. Production clients are
  assign-only.

## Solution Proposal

### API

`mz-kafka-client` mirrors the subset of rust-rdkafka's public API that
Materialize uses, under the same module paths (`config`, `client`, `consumer`,
`producer`, `admin`, `message`, `topic_partition_list`, `metadata`, `error`,
`types`, `statistics`, `util`). Switching the workspace was mostly an import
rename. The deliberate deviations are listed in the crate docs. The notable
ones:

* `ClientContext::stats_raw`, `ConsumerContext::rebalance` and
  `main_queue_min_poll_interval` are gone (Materialize only forwarded them).
* `ProducerContext::DELIVERY_REPORT_CONTENTS` (default false): successful
  delivery reports carry topic, partition and offset only, so the producer does
  not retain every payload until it is acknowledged.
* Admin `describe_configs`/`alter_configs` report per-resource errors, which
  rust-rdkafka dropped.
* Re-assigning a partition at its unchanged start offset keeps its fetch
  position. librdkafka restarted it, which made Materialize's source re-read
  every partition each time a new partition appeared.

Display strings of `KafkaError` and `RDKafkaErrorCode` are byte-identical to
rust-rdkafka, and log lines Materialize parses or tests assert carry
librdkafka's exact text. The tracing target stays `librdkafka`, because log
filters and the Sentry filter key on it.

### Architecture

```
facade (rdkafka-shaped API)
  engines: consumer (fetcher, partition queues, offsets, group membership)
           producer (accumulator, sender, idempotence, transactions)
           admin
  cluster: bootstrap, metadata cache, broker registry, leader/coordinator/
           controller lookup, statistics, callback event queue
  net:     one connection per broker per client: resolve hook, TCP, TLS
           (OpenSSL), ApiVersions, SASL (PLAIN, SCRAM-SHA-256/512,
           OAUTHBEARER, KIP-368 renewal), pipelined requests
  protocol: kafka-protocol message structs (generated from Apache Kafka's
           JSON schemas), own RecordBatch codec, compression, partitioners
```

All clients of a process share one Tokio runtime (`kafka-io`, at most four
worker threads). Blocking API calls wait on a standard library channel, so they
work from timely threads, `spawn_blocking` threads and Tokio workers. The
blocking hooks (`resolve_broker_addr`, `generate_oauth_token`) run on the
runtime's blocking pool, where `Handle::block_on` on another runtime is
allowed. `error` and `stats` callbacks follow librdkafka's main-queue
semantics: consumers deliver them from `poll`, producers immediately, admin
clients never. Connection validation depends on this.

TLS uses OpenSSL (already vendored), like librdkafka: PEM and PKCS#12 parsing,
certificate and hostname verification, and the OpenSSL error texts that tests
assert behave as before.

### Memory and CPU design

* Consumer queues hold fetched record sets still compressed. Records are
  decoded lazily in `poll`, one batch at a time, on the caller's thread.
  Messages are views into the batch buffer, with no per-message allocation.
* Per-partition prefetch adapts to the observed drain rate instead of filling
  queues to `queued.min.messages`. librdkafka's limits stay as hard caps.
* Record sets are copied out of multi-partition responses so a slow partition
  never pins a whole response. Small record sets are copied to their exact
  size, and queue limits count the buffer capacity actually held.
* Fetch requests rotate partitions, so one backlogged partition cannot starve
  the others on its broker.
* The producer encodes each record straight into the open batch and writes
  batches into requests without copying them.

## Testing

* Crate tests (unit, integration against Kafka 4.2 and Redpanda, TLS/SASL
  against a dedicated broker): codec fixtures written by librdkafka and the Java
  client, partitioner choices checked against librdkafka and Java, SCRAM RFC
  vectors, malformed-input tests.
* Differential harnesses that run the same scenario through librdkafka 2.12.1
  and the new client and compare outcomes: connection failure log texts, the
  sink's exact transactional call sequence (commit, abort, fencing, timeouts,
  deleted topics), consumer positions after every poll (read_committed, control
  records, EOF, offset reset, re-assign), admin results, and mixed consumer
  groups with one member of each client.
* Materialize suites, run locally with images built from this branch: testdrive
  on Kafka, Redpanda and with five default partitions; kafka-auth;
  ssh-connection; source-sink-errors; kafka-exactly-once; kafka-rtr;
  kafka-low-watermark; kafka-multi-broker; kafka-resumption on both brokers;
  kafka-matrix (26 broker versions, Redpanda v22.3 to v26.2 and Confluent 7.0 to
  8.4); persistence; cluster; upsert; storage-usage; data-ingest; bounded-memory;
  debezium; 0dt; testdrive-old-kafka-src-syntax including the migration
  workflow; aws-glue-schema-registry; platform-checks Kafka checks for
  NoRestartNoUpgrade, ZeroDowntimeUpgradeEntireMzOnce and UpgradeEntireMz (a
  librdkafka-based release hands transactional ids, progress topics and
  committed offsets to the new client); zippy KafkaSources; cargo tests that
  need a broker.
* Two independent robustness reviews of the crate. All high and medium findings
  are fixed, except the kafka-protocol item below.

## Performance

Measured with a harness that compiles the same benchmark against both clients
(`rdkafka` vs `mz-kafka-client`), jemalloc as global allocator as in
production, interleaved runs. Consumer: 5M records, 8 partitions, split
partition queues polled like Materialize's source.

| Workload | librdkafka | mz-kafka-client |
|---|---|---|
| Consume, 128 MiB fetch, lz4: peak RSS / throughput | 393-483 MB / 1.7-1.9M msg/s | 59-63 MB / 4.1-4.6M msg/s |
| Consume, 128 MiB fetch, uncompressed | 273-276 MB / 1.5-1.7M msg/s | 76-96 MB / 3.9-4.5M msg/s |
| Consume, 1 MiB fetch (4 configs) | 47-84 MB / 2.0-3.2M msg/s | 36-48 MB / 4.1-4.8M msg/s |
| Consume CPU per message | 0.49-0.57 µs | 0.22-0.28 µs |
| Transactional produce, lz4: CPU per message | 1.0-2.2 µs | 0.7-1.0 µs |
| Transactional produce: throughput | 0.2-0.9M msg/s | 0.4-1.0M msg/s |
| Transactional produce, 10k-record transactions: peak RSS | 24.6-25.6 MB | 24.3-26.0 MB |
| Transactional produce, 100k-record transactions: peak RSS | ~97 MB | 67-72 MB |
| 50 idle consumers: RSS per client / threads | ~950 KB / 206 | ~110 KB / 10 |
| 50 idle transactional producers: RSS per client / threads | ~1.3 MB / 306 | ~325 KB / 60 |
| Backpressured consumer: Fetch requests per second | 2.0-3.6 | 2.1-2.6 |

End to end, Materialize's feature benchmark (this branch vs its upstream base,
same optimized profile):

* `KafkaEnvelopeNoneBytes` ingests about 2x faster (median 5.2 s vs 10.9 s).
* Peak source-cluster RSS during heavy ingestion is at or below librdkafka
  (`KafkaEnvelopeNoneBytesScalability` at scale 7: median 4.4 GB vs 6.2 GB;
  `KafkaEnvelopeNoneBytes` -5%; `KafkaUpsertUnique` -6%; scale 6 +6%).
* No reproducible wallclock regression in `KafkaUpsert*`, `ExactlyOnce`,
  `ManyKafkaSourcesOnSameCluster`, `ParallelIngestion`, `KafkaRestartBig`,
  `SubscribeParallelKafka` or `StartupLoaded`.

## Alternatives

* **Keep librdkafka and fix the issues upstream.** Does not address threads per
  client, the FFI boundary or the log-text coupling.
* **An existing Rust client.** `rskafka` lacks transactions, consumer groups and
  most SASL mechanisms. Other crates wrap librdkafka.
* **Generate our own protocol codec.** `kafka-protocol` already generates the
  message structs from Kafka's JSON schemas. Record batches, where the volume
  is, are our own code.
* **A runtime switch between both clients.** Keeps librdkafka in the build and
  doubles the surface to test. Not done; the decision is open (see below).

## Open questions

* **kafka-protocol preallocation.** kafka-protocol 0.18 sizes array allocations
  from the count the broker declares. A malicious or corrupted response of a few
  bytes, including the ApiVersions reply before authentication, can request a
  huge allocation and abort the process. A patch (cap reservations by the bytes
  present) is prepared for upstream. Applying it before an upstream release needs
  a fork.
* **Rollout.** There is no runtime fallback to librdkafka. Options: ship as is
  behind the existing test coverage, or keep both clients for one release behind
  a flag.
* **Faster ingestion raises some downstream peaks.** The client itself holds at
  most about 50 MiB per process while data flows (librdkafka: up to 150 MiB in
  `bounded-memory`, up to 440 MiB in the feature benchmark). Because data arrives
  in larger batches, the upsert operator's staging buffers grow larger during
  hydration: `bounded-memory` `upsert-index-hydration` peaked about 5% above
  librdkafka (median 6981 vs 6605 MiB with a raised limit), while settling below
  it afterwards. Those buffers keep their largest capacity after draining.
  Shrinking them in `upsert_continual_feedback` would remove the residual for
  any client speed. The nightly aarch64 `bounded-memory` job is the place to
  watch.
* **Not covered by local tests:** AWS MSK with IAM authentication (OAUTHBEARER
  with SigV4 tokens) and real PrivateLink endpoints. The code paths are tested
  against mocks and local brokers only.
