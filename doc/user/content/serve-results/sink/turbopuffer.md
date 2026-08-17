---
title: "turbopuffer"
description: "How to export results from Materialize to a turbopuffer namespace using the Kafka sink and mz-tpuf-sink."
menu:
  main:
    parent: sink
    name: "turbopuffer"
    weight: 35
---

This guide shows how to keep a [turbopuffer](https://turbopuffer.com)
namespace in sync with a Materialize view. A [Kafka
sink](/sql/create-sink/kafka/) writes the results to a Kafka topic.
[`mz-tpuf-sink`](https://github.com/MaterializeInc/mz-turbopuffer-sink), a
Python library that Materialize develops, reads that topic and applies each
change to a turbopuffer namespace.

Use this pipeline to keep a vector search index up to date to within hundreds
of milliseconds, just using SQL. Materialize maintains the search document as
an incrementally updated view over your operational data, and the sink applies
precise deltas to turbopuffer, so only the affected documents are rewritten.

`mz-tpuf-sink` applies each Materialize transaction as a single atomic
turbopuffer write, and it recomputes an embedding only when the columns that
embedding reads actually change. Editing an article's `title` re-embeds it;
incrementing its `views` a thousand times does not embed it once.

## Before you begin

- A turbopuffer API key, and the region your namespaces live in.

- Python 3.12 or later, and somewhere to run a long-lived process. The library
  has no built-in daemon or CLI: you call `run_sink()` from your own program,
  which blocks until stopped.

- Network access from that process to Kafka, to the schema registry, to
  Materialize (port 6875), and to turbopuffer.

- A Materialize role for the sink process. The process runs a `SUBSCRIBE`
  against the system catalog to watch the sink's write frontier, so the role
  needs `USAGE` on a cluster to run that `SUBSCRIBE` on.

{{% include-headless "/headless/kafka-sink-search/prerequisites" %}}

## Step 1. Set up the sink in Materialize

The examples in this guide build a search document for an article catalog
tracking its content and page views.

### Create the connections

```mzsql
CREATE SECRET kafka_password AS '<BROKER_PASSWORD>';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER '<BROKER_HOST>:9092',
    SASL MECHANISMS = 'SCRAM-SHA-256',
    SASL USERNAME = '<BROKER_USERNAME>',
    SASL PASSWORD = SECRET kafka_password
);

CREATE SECRET csr_password AS '<CSR_PASSWORD>';

CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL '<CSR_URL>',
    USERNAME = '<CSR_USERNAME>',
    PASSWORD = SECRET csr_password
);
```

The sink reads column types out of the registered Avro schema, so this
pipeline needs Avro with a schema registry. For other authentication methods,
see [`CREATE CONNECTION`](/sql/create-connection/#kafka).

### Create the search document

Create a [materialized view](/sql/create-materialized-view/) that builds the
document you want to search.

```mzsql
CREATE MATERIALIZED VIEW articles AS
    SELECT a.id, a.title, a.body, count(p.article_id) AS views
    FROM article_content a
    LEFT JOIN page_views p ON a.id = p.article_id
    GROUP BY 1, 2, 3;
```

### Create the sink

```mzsql
CREATE SINK articles_sink_v1
  IN CLUSTER sinks_cluster
  FROM articles
  INTO KAFKA CONNECTION kafka_connection (
    TOPIC 'articles_v1',
    TOPIC PARTITION COUNT 6
  )
  KEY (id) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE DEBEZIUM;
```

`ENVELOPE DEBEZIUM` wraps each change in a `{"before": ..., "after": ...}`
value. The sink compares these two fields to find the columns that changed,
and writes an update as a patch of only those columns. For the full list of
options, see [`CREATE SINK ... INTO KAFKA`](/sql/create-sink/kafka/).

`KEY` must name **exactly one** column. Its value becomes the turbopuffer
document ID verbatim, so the column must be an integer (non-negative), a
string of at most 64 bytes, or a `uuid`. Nothing is hashed, truncated, or
concatenated: every such transformation could map two distinct keys onto one
document. If your view has no single-column key, add a derived key column to
the view rather than listing several columns here.

{{< warning >}}
Document identity in turbopuffer is only as good as the key. `NOT ENFORCED`
disables Materialize's check that the key is unique. If two rows share a key,
they collapse into one document and each overwrites the other. Drop `NOT
ENFORCED` when Materialize can prove the key unique, and only keep it when you
have outside knowledge that it is.
{{< /warning >}}

A value column named `id` is dropped, because `id` is turbopuffer's reserved
document-ID field and the document ID comes from the Kafka key. The sink logs
a warning when it drops one.

{{% include-headless "/headless/kafka-sink-search/empty-destination" %}}

## Step 2. Run the sink

Install the library:

```sh
pip install "mz-tpuf-sink @ git+https://github.com/MaterializeInc/mz-turbopuffer-sink"
```

Write a program that builds a `SinkConfig` and calls `run_sink()`. `run_sink()`
blocks the calling thread. Pass a `threading.Event` and set it from a signal
handler to shut down after the current poll:

```python
import os
import signal
import threading

from mz_tpuf_sink import SinkConfig, run_sink

config = SinkConfig(
    kafka_bootstrap_servers="<BROKER_HOST>:9092",
    kafka_topic="articles_v1",
    kafka_group_id="mz-tpuf-sink-articles",
    schema_registry_url="<CSR_URL>",
    schema_registry_auth=f"<CSR_USERNAME>:{os.environ['CSR_PASSWORD']}",
    materialize_dsn=os.environ["MATERIALIZE_DSN"],
    materialize_sink="materialize.public.articles_sink_v1",
    turbopuffer_api_key=os.environ["TURBOPUFFER_API_KEY"],
    turbopuffer_region="aws-us-east-1",
    namespace="articles_v1",
)

stop = threading.Event()
for sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(sig, lambda *_: stop.set())

run_sink(config, stop=stop)
```

Two settings are worth reading closely:

- `materialize_sink` must be fully qualified as `database.schema.sink`. Sink
  names are unique only within a schema, so a bare name could match sinks in
  several schemas and mix their write frontiers together. The sink validates
  the qualified name against the configured topic at startup, so a name
  pointing at a different topic fails immediately rather than tracking the
  wrong frontier.

- `materialize_dsn` is not optional. The sink connects to Materialize to watch
  the sink's write frontier, which is what lets it complete a transaction while
  some partitions are idle. See [Transaction
  atomicity](#transaction-atomicity).

Startup failures raise rather than retrying forever. An unreachable broker, a
sink name that matches nothing, and a transform reading a column the topic
lacks all fail immediately. Run the process under a supervisor that restarts
it. A restart resumes from the last committed Kafka offsets.

{{< warning >}}
Run **exactly one** process per topic and namespace.

The sink completes a Materialize transaction by reasoning over the partitions
that its own consumer is assigned. Two processes sharing a consumer group each
hold a subset of partitions, so each writes its own slice of a transaction
independently, and a search can then observe half of an update. Scale
throughput by adding partitions and letting the single process consume them
all, not by adding processes.
{{< /warning >}}

## Step 3. Add an embedding

The reason to put a view in turbopuffer is usually vector search, and the
expensive part of vector search is embedding. A **transform** derives extra
turbopuffer attributes from a record's columns, and runs only when the columns
it reads actually change:

```python
from mz_tpuf_sink import FunctionTransform, SinkConfig, run_sink
from openai import OpenAI

client = OpenAI()

def embed(rows):
    """Called once per batch of records, never once per row."""
    text = [f"{row['title']}\n\n{row['body']}" for row in rows]
    response = client.embeddings.create(model="text-embedding-3-small", input=text)
    return [{"embedding": item.embedding} for item in response.data]

article_embedding = FunctionTransform(
    name="article_embedding",
    sources=("title", "body"),                              # columns it reads
    schema={"embedding": {"type": "[1536]f32", "ann": True}},
    distance_metric="cosine_distance",
    batch_size=256,
    compute=embed,
)

run_sink(config, stop=stop, transforms=[article_embedding])
```

Each row handed to `compute` holds exactly the columns named in `sources`,
plus `id`. Return one mapping per row, in the same order, containing the
attributes named in `schema`.

A transform is ordinary Python, so it can call any model, local or hosted, and
it receives records in batches so one API call covers many documents. It can
produce anything, not just vectors: a slug, a sentiment score, a translated
title.

A few rules the sink enforces at startup, before it connects to Kafka:

- A transform cannot read or produce `id`, cannot produce an attribute that is
  already a table column or another transform's output, and cannot read
  another transform's output. Transforms run on the translated record and do
  not chain.
- A vector attribute needs both `ann: True` and a `distance_metric`.
  turbopuffer requires a metric on every write to a namespace that holds a
  vector, and it takes one metric per request, so two vector transforms cannot
  disagree.
- A namespace holds at most two vector attributes.

An update that changes a column feeding a vector is written as a whole-document
upsert rather than a patch, because turbopuffer cannot patch a vector
attribute. Updates that miss those columns stay patches and leave the stored
vector untouched, which is what keeps an unrelated update from paying for a
re-embed.

## Step 4. Validate the pipeline

1.  Query the namespace and confirm the documents arrived with their vectors.
    Read at strong consistency so the query reflects the most recent write:

    ```python
    from turbopuffer import Turbopuffer

    ns = Turbopuffer(api_key="tpuf_...", region="aws-us-east-1").namespace("articles_v1")

    response = ns.query(
        rank_by=("id", "asc"),
        top_k=1,
        include_attributes=True,
        consistency={"level": "strong"},
    )
    print(response.rows)
    ```

    The output should resemble the following:

    ```nofmt
    [Row(id=1, title='Storage engines', body='...', views=42,
         embedding=[0.021, -0.118, ...])]
    ```

1.  Delete the row with `id = 1` from `article_content` in Materialize:

    ```mzsql
    DELETE FROM article_content WHERE id = 1;
    ```

    Confirm that the document is gone:

    ```python
    response = ns.query(
        rank_by=("id", "asc"),
        top_k=100,
        consistency={"level": "strong"},
    )
    print([row.id for row in response.rows or []])
    ```

    The ID `1` is no longer in the output.

## Consistency and delivery

### Transaction atomicity

Materialize commits a batch of changes at a single logical timestamp, and the
sink applies that timestamp to turbopuffer as one write request. A statement
that changes fifty rows shows up as fifty changed documents at once, and a row
that leaves the view leaves the namespace. A search never observes half of an
update.

Holding that guarantee means the sink cannot write a timestamp until it knows
no more messages will arrive for it. Every message carries the
`materialize-timestamp` header, and within a partition Materialize emits
messages in non-decreasing timestamp order, so a partition is known complete
through timestamp `F` when either:

- it has yielded a message with a timestamp at or past `F`, or
- the consumer has caught up to the partition's high watermark *after* the
  Materialize sink's write frontier reached `F`. The write frontier guarantees
  that every future message has a timestamp at or past `F`, so an idle
  partition has nothing more to say about anything below it.

The second rule is why the process needs a Materialize connection. Without it,
one quiet partition would stall every transaction behind it.

Every buffered timestamp below the minimum of those points, across all
assigned partitions, is complete and is written atomically.

{{< note >}}
A transaction larger than the configured request limits (`max_rows_per_request`,
10,000 rows by default, and `max_bytes_per_request`, 200 MiB) is split into
sequential chunks, and turbopuffer's atomicity is per request. The sink logs a
warning the first time it splits a transaction. Raise the limits, or narrow the
sinked view, if your transactions routinely exceed them.
{{< /note >}}

### At-least-once delivery, idempotent writes

Kafka offsets are committed only after a transaction has landed in turbopuffer,
so delivery is at-least-once. A crash between the write and the commit replays
that transaction. Every operation the sink issues, whether an upsert, a column
patch, or a delete, is keyed by document ID, so a replay reproduces the same
state rather than compounding. The end state converges, though individual
writes may repeat.

The same applies to a consumer group rebalance. Buffered-but-unwritten data may
be redelivered to another consumer, so on partition revocation the sink drops
all buffered state, which guarantees a stale flush can never overwrite a newer
write. The work replays from the last committed offsets.

Materialize writes its sink topic using Kafka transactions, and the consumer
pins `isolation.level=read_committed`, so records from an aborted Materialize
transaction never reach turbopuffer.

### Column-level updates

An update is written as a patch containing only the columns whose values
changed. Attributes that turbopuffer holds but Materialize does not know about
survive such an update, and an update whose values are all unchanged produces
no write at all.

Inserts and deletes are whole-document operations, as is the one update case
that cannot be a patch: a change to a column feeding a vector. A whole-document
upsert replaces the document, so an attribute written to turbopuffer from
outside this pipeline does not survive one.

When a transform reads several columns, changing one of them retains the rest
from the record's `after` value, so `compute` always sees complete input rather
than a partial patch.

### Attribute types are declared, not inferred

The sink reads the row's column types out of the Avro schema in the registry
and declares them to turbopuffer on every write. Left to inference, turbopuffer
types an attribute from the first value it sees, which breaks a stream later.
An integral float such as `5.00` is inferred as `int`, and the next fractional
value for that column is then rejected. A column whose first row is `NULL` has
nothing to infer from at all. Declaring the schema up front removes that class
of failure, and adding a column to your view needs no configuration change
here.

## Operational guidelines

- **Size the process for the snapshot.** A transaction is held in memory until
  it is complete, and a new sink's initial snapshot is one transaction. Size
  the process for the snapshot, not for the steady-state rate of change. The
  sink logs a warning once buffered data passes `buffer_warn_bytes` (1 GiB by
  default).

- **Read-ahead is bounded.** A partition may read one timestamp past the oldest
  unflushed transaction. Beyond that, the sink pauses it until flushing catches
  up. Memory therefore tracks transaction size, not consumer lag.

- **Watch the poll budget when transforms are slow.** A flush that outlasts
  Kafka's `max.poll.interval.ms` gets the consumer evicted, which drops
  buffered state and replays the work. Set `kafka_max_poll_interval_ms` above
  the time your slowest transaction of embedding calls takes, or lower a
  transform's `batch_size`. When that setting is present, the sink warns once a
  flush passes half the budget, while there is still time to react.

- **Transient turbopuffer failures are retried.** Connection errors and HTTP
  408, 409, 429, and 5xx responses are retried up to five times with
  exponential backoff. Anything else fails the process, rather than skipping
  a transaction and leaving the namespace permanently wrong.

- **Version the namespace alongside the sink.** As with the Kafka topic, point
  a new sink at a new namespace and swap the reader over once it has caught up.

## Type mapping

Column types come from the Avro schema the sink publishes to the schema
registry, so numbers stay numbers and timestamps stay timestamps, filterable
and sortable in turbopuffer.

| Materialize | turbopuffer |
| --- | --- |
| `text`, `char`, `varchar` | `string` |
| `smallint`, `integer`, `bigint` | `int` |
| `numeric`, `real`, `double precision` | `float` |
| `boolean` | `bool` |
| `date`, `timestamp`, `timestamptz` | `datetime` |
| `time` | `int` (microseconds since midnight) |
| `uuid` | `string`, or `uuid` when it is the key column |
| `jsonb` | `string` (JSON text) |
| `bytea`, `interval`, `uint2`, `uint4`, `uint8` | base64-encoded `string` |
| lists and arrays of scalars | `[]string`, `[]int`, `[]float`, … |
| records, maps, nested lists | `string` (JSON text) |

{{< note >}}
`numeric` becomes a 64-bit float, because turbopuffer has no exact decimal
type. Values carrying more than 17 significant digits lose precision, and the
sink logs a warning the first time it sees one.

Materialize encodes unsigned integers as opaque Avro `fixed` values, so they
arrive base64-encoded rather than as numbers, and cannot be filtered or sorted
on. Cast them to `bigint` in the view if you need them as numbers.
{{< /note >}}

## Related pages

- [`CREATE SINK ... INTO KAFKA`](/sql/create-sink/kafka/)
- [`CREATE CONNECTION`](/sql/create-connection/#kafka)
- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/)
- [Sinks](/concepts/sinks/)
- [Kafka and Redpanda](/serve-results/sink/kafka/)
- [Troubleshooting sinks](/serve-results/sink/sink-troubleshooting/)
- [`mz-tpuf-sink`](https://github.com/MaterializeInc/mz-turbopuffer-sink)
