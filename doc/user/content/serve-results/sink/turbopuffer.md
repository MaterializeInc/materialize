---
title: "turbopuffer"
description: "How to export results from Materialize to turbopuffer using the Kafka sink and mz-tpuf-sink."
menu:
  main:
    parent: sink
    name: "turbopuffer"
    weight: 35
---

This guide shows how to send results from Materialize to turbopuffer. A
[Kafka sink](/sql/create-sink/kafka/) writes the results to a Kafka topic.
[`mz-tpuf-sink`](https://github.com/MaterializeInc/mz-turbopuffer-sink)
reads that topic and writes the documents to a turbopuffer namespace.

Use this pipeline to keep a turbopuffer namespace up to date, just using SQL. 
Materialize maintains the search document as an incrementally updated view 
over your operational data, and pushes precise deltas to turbopuffer as 
upstream data changes, so only the affected documents are rewritten.

Vectors come from a **transform**, a Python function that declares the columns
it reads and the attributes it produces. The sink calls a transform only for
the documents whose source columns actually changed, so embedding costs scale
with what changed.

## Before you begin

- A turbopuffer API key, and the region that holds your namespace.

- Python 3.12 or later on the host that runs the sink, and a way to install
  packages. The examples below use [uv](https://docs.astral.sh/uv/).

- Materialize SQL credentials to read catalog metadata.

{{% include-headless "/headless/kafka-sink-search/prerequisites" %}}

## Step 1. Set up the sink in Materialize

{{% include-headless "/headless/kafka-sink-search/debezium-setup" %}}

`KEY (id)` must name exactly one column. That column's value becomes the
turbopuffer document id, unchanged. Integer, string, and `uuid` columns work,
and a string id holds at most 64 bytes. To key on something wider, add a
hashed key column to the view and sink that column instead.

## Step 2. Install the sink library

Install `mz-tpuf-sink` and the client for your embedding provider. This
example uses OpenAI:

```sh
uv add "mz-tpuf-sink @ git+https://github.com/MaterializeInc/mz-turbopuffer-sink"
uv add openai
```

## Step 3. Configure and run the sink

The sink writes documents to a turbopuffer namespace. Do not create the
namespace first. turbopuffer creates it on the first write, and the sink
declares the attribute schema on every write. Column types come from the Avro
schema in the schema registry, so numbers stay numbers and timestamps stay
timestamps, filterable and sortable. Adding a column to the view needs no
change to the program below.

{{% include-headless "/headless/kafka-sink-search/empty-destination" %}}

Write a program that declares the transforms and runs the sink:

```python
import os

from mz_tpuf_sink import FunctionTransform, SinkConfig, run_sink
from openai import OpenAI

client = OpenAI()


def embed_column(column, rows):
    """Embed one column for a batch of documents, in one API call."""
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=[row[column] for row in rows],
    )
    return [{f"{column}_embedding": item.embedding} for item in response.data]


def embedding_transform(column):
    return FunctionTransform(
        name=f"{column}_embedding",
        sources=(column,),
        schema={f"{column}_embedding": {"type": "[1536]f32", "ann": True}},
        distance_metric="cosine_distance",
        batch_size=256,
        compute=lambda rows: embed_column(column, rows),
    )


run_sink(
    SinkConfig(
        kafka_bootstrap_servers="<BROKER_HOST>:9092",
        kafka_topic="articles_v1",
        schema_registry_url="<CSR_URL>",
        schema_registry_auth=f"<CSR_USERNAME>:{os.environ['CSR_PASSWORD']}",
        materialize_dsn=os.environ["MATERIALIZE_DSN"],
        materialize_sink="materialize.public.articles_sink_v1",
        turbopuffer_api_key=os.environ["TURBOPUFFER_API_KEY"],
        turbopuffer_region="aws-us-east-1",
        namespace="articles_v1",
    ),
    transforms=[embedding_transform("title"), embedding_transform("body")],
)
```

The `OpenAI()` client reads its key from the `OPENAI_API_KEY` environment
variable.

Each transform keeps one vector in sync with one text column:

- `sources` names the columns the transform reads. An update that leaves
  every source column unchanged never reaches `compute`. Editing an article's
  `title` re-embeds the title. Changing its `views` a thousand times embeds
  nothing.
- `schema` declares the turbopuffer attributes the transform produces. A
  vector needs `ann: True` and a `distance_metric`, and the sink refuses to
  start without them. A namespace holds at most two vector attributes, so the
  two transforms above are at the limit.
- `batch_size` bounds how many documents reach one `compute` call. The sink
  batches the calls, so one API request covers many documents.
- `distance_metric` applies to the whole namespace. Two vector transforms
  cannot declare different metrics.

A transform is ordinary Python, so it can call any model, local or hosted, and
it can produce anything, not just vectors. A slug, a sentiment score, or a
translated title all work the same way.

`SinkConfig` names the two ends of the pipeline:

- `kafka_topic` is the topic the Materialize sink writes to.
- `materialize_sink` is the sink's fully qualified name, as
  `database.schema.sink`. A bare name could match sinks in several schemas, so
  the sink rejects one.
- `namespace` is the turbopuffer namespace to write.

Run the program:

```sh
uv run python sink.py
```

Run one process per topic, writing to one namespace. `run_sink` blocks until
stopped. To shut down cleanly, pass a `threading.Event` as its second argument
and set that event from a signal handler.

Embedding calls make a flush long-running, and a Kafka consumer that does not
poll within `max.poll.interval.ms` is evicted, which drops buffered state and
replays the work. Raise `kafka_max_poll_interval_ms` in `SinkConfig` if the
sink logs a slow flush warning.

## Step 4. Validate the pipeline

1.  Confirm that the documents arrived with their vectors:

    ```python
    from turbopuffer import Turbopuffer

    namespace = Turbopuffer(
        api_key="<TURBOPUFFER_API_KEY>",
        region="aws-us-east-1",
    ).namespace("articles_v1")

    response = namespace.query(
        rank_by=("id", "asc"),
        top_k=1,
        include_attributes=["id", "title", "views", "title_embedding"],
        consistency={"level": "strong"},
    )
    for row in response.rows:
        print(row.id, row.title, row.views, row.title_embedding[:4])
    ```

    The output should resemble the following:

    ```nofmt
    1 Storage engines 42 [0.021, -0.118, 0.043, 0.009]
    ```

1.  Delete the row with `id = 1` from `article_content` in Materialize:

    ```mzsql
    DELETE FROM article_content WHERE id = 1;
    ```

    Confirm that the document is gone:

    ```python
    response = namespace.query(
        rank_by=("id", "asc"),
        filters=("id", "Eq", 1),
        top_k=1,
        consistency={"level": "strong"},
    )
    print(response.rows)
    ```

    The output is an empty list.

## Related pages

- [`CREATE SINK ... INTO KAFKA`](/sql/create-sink/kafka/)
- [`CREATE CONNECTION`](/sql/create-connection/#kafka)
- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/)
- [Sinks](/concepts/sinks/)
- [Kafka and Redpanda](/serve-results/sink/kafka/)
- [Troubleshooting sinks](/serve-results/sink/sink-troubleshooting/)
