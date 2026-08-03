---
title: "Elasticsearch"
description: "How to export results from Materialize to Elasticsearch using the Kafka sink and Kafka Connect."
menu:
  main:
    parent: sink
    name: "Elasticsearch"
    weight: 20
---

This guide shows how to send results from Materialize to Elasticsearch. A
[Kafka sink](/sql/create-sink/kafka/) writes the results to a Kafka topic.
Kafka Connect reads that topic and writes the documents to Elasticsearch.

The [`perfect-embedding`](https://github.com/MaterializeInc/perfect-embedding)
transform runs inside the connector. It updates each vector embedding only
when the text for that embedding changes.

## Before you begin

- An Elasticsearch 7.x or 8.x cluster. The self-managed connector does not
  work with Elasticsearch 9.x. For Elasticsearch 9.x, use the Confluent
  Cloud managed [Elasticsearch Sink
  V2](https://docs.confluent.io/cloud/current/connectors/cc-elasticsearch-sink-v2/cc-elasticsearch-sink-v2.html)
  connector instead.

- Kafka Connect workers that run in distributed mode. Each worker needs a
  writable `plugin.path` and Java 11 or later.

- An Elasticsearch role for the connector. Grant this role `read`, `write`,
  `view_index_metadata`, and `create_index` on the target index.

{{< include-md file="shared-content/kafka-sink-search-prerequisites.md" >}}

## Step 1. Set up the sink in Materialize

{{< include-md file="shared-content/kafka-sink-search-debezium-setup.md" >}}

## Step 2. Create the Elasticsearch index

The connector writes documents to an index. The index name is the Kafka
topic name in lowercase letters. The sink topic is `articles_v1`, so the
index name is also `articles_v1`.

Create an empty index named `articles_v1`. The sink fills this index later.

Declare the index mapping yourself. The connector **does not infer** a
`dense_vector` field. An index that the connector creates cannot serve
vector queries.

```nofmt
PUT /articles_v1
{
  "mappings": {
    "properties": {
      "id":    { "type": "integer" },
      "title": { "type": "text" },
      "body":  { "type": "text" },
      "views": { "type": "long" },
      "title_embedding": {
        "type": "dense_vector",
        "dims": 1536,
        "similarity": "cosine"
      },
      "body_embedding": {
        "type": "dense_vector",
        "dims": 1536,
        "similarity": "cosine"
      }
    }
  }
}
```

The output should resemble the following:

```nofmt
{ "acknowledged": true, "shards_acknowledged": true, "index": "articles_v1" }
```

For the list of vector options, see Elastic's [`dense_vector` field
reference](https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html).

Create a read alias named `articles`. Applications send queries to this
alias, not to the index `articles_v1` directly:

```nofmt
POST /_aliases
{
  "actions": [
    { "add": { "index": "articles_v1", "alias": "articles" } }
  ]
}
```

The output should resemble the following:

```nofmt
{ "acknowledged": true }
```

{{< include-md file="shared-content/kafka-sink-empty-destination.md" >}}

## Step 3. Deploy the connector

1. Install the [Confluent Elasticsearch Sink
   Connector](https://docs.confluent.io/kafka-connectors/elasticsearch/current/overview.html)
   from Confluent Hub.
1. Extract the
   [`perfect-embedding`](https://github.com/MaterializeInc/perfect-embedding/releases)
   release zip file into a directory on the worker's `plugin.path`.
1. Restart the workers. Kafka Connect then finds both plugins.

Create the connector. Send this configuration to the Kafka Connect REST API
with `POST /connectors`:

```json
{
  "name": "elasticsearch-articles",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "topics": "articles_v1",
    "connection.url": "https://<ELASTICSEARCH_HOST>:9200",
    "connection.username": "<ELASTICSEARCH_USERNAME>",
    "connection.password": "<ELASTICSEARCH_PASSWORD>",
    "tasks.max": "4",
    "key.ignore": "false",
    "schema.ignore": "false",
    "write.method": "UPSERT",
    "behavior.on.null.values": "delete",
    "max.in.flight.requests": "1",
    "read.timeout.ms": "30000",
    "batch.size": "100",
    "consumer.override.isolation.level": "read_committed",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "<CSR_URL>",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "<CSR_URL>",
    "transforms": "extractKey,embed",
    "transforms.extractKey.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.extractKey.field": "id",
    "transforms.embed.type": "com.materialize.connect.smt.embedding.EmbeddingDiffTransform",
    "transforms.embed.embedded.columns": "title,body",
    "transforms.embed.provider": "openai",
    "transforms.embed.openai.api.key": "${file:/opt/connect/secrets.properties:openai_api_key}",
    "transforms.embed.openai.model": "text-embedding-3-small",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq.elasticsearch.articles_v1",
    "errors.deadletterqueue.context.headers.enable": "true"
  }
}
```

The `${file:...}` reference needs the file config provider. Enable this
provider in the worker properties. Set `config.providers=file` and
`config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider`.

The `embed` transform updates the vectors. For each record, it reads the
Debezium `before` and `after` values. It recomputes an embedding only for a
column in `embedded.columns` whose value changed. It leaves the rest of the
document unchanged:

- `transforms.embed.embedded.columns` names the text columns to embed. Each
  column must have the string type.
- `transforms.embed.provider` selects the embedding provider. This example
  uses `openai`.
- `transforms.embed.openai.api.key` and `transforms.embed.openai.model`
  configure the OpenAI client. The connector reads these settings only when
  `provider` is `openai`.

For the other transform options, see the
[`perfect-embedding`](https://github.com/MaterializeInc/perfect-embedding)
documentation.

## Step 4. Validate the pipeline

1.  Check that the connector is running:

    ```nofmt
    GET /connectors/elasticsearch-articles/status
    ```

    The output should resemble the following:

    ```nofmt
    {
      "name": "elasticsearch-articles",
      "connector": { "state": "RUNNING" },
      "tasks": [ { "id": 0, "state": "RUNNING" } ]
    }
    ```

1.  Confirm that the documents have their vectors. Use the `fields` parameter
    to request the vector field. This parameter returns the vector field even
    when the index excludes vectors from `_source`:

    ```nofmt
    GET /articles/_search
    {
      "size": 1,
      "_source": [ "id", "title", "views" ],
      "fields": [ "title_embedding" ]
    }
    ```

    The output should resemble the following:

    ```nofmt
    "hits": [
      {
        "_id": "1",
        "_source": { "id": 1, "title": "Storage engines", "views": 42 },
        "fields": { "title_embedding": [ [ 0.021, -0.118, ... ] ] }
      }
    ]
    ```

1.  Delete the row with `id = 1` from `article_content` in Materialize:

    ```mzsql
    DELETE FROM article_content WHERE id = 1;
    ```

    Confirm that the document is gone:

    ```nofmt
    GET /articles/_doc/1
    ```

    The response reports `"found": false`.

## Related pages

- [`CREATE SINK ... INTO KAFKA`](/sql/create-sink/kafka/)
- [`CREATE CONNECTION`](/sql/create-connection/#kafka)
- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/)
- [Sinks](/concepts/sinks/)
- [Kafka and Redpanda](/serve-results/sink/kafka/)
- [Troubleshooting sinks](/serve-results/sink/sink-troubleshooting/)
