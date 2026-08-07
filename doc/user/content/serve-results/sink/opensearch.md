---
title: "OpenSearch"
description: "How to export results from Materialize to OpenSearch using the Kafka sink and Kafka Connect."
menu:
  main:
    parent: sink
    name: "OpenSearch"
    weight: 30
---

This guide shows how to send results from Materialize to OpenSearch. A
[Kafka sink](/sql/create-sink/kafka/) writes the results to a Kafka topic.
Kafka Connect reads that topic and writes the documents to OpenSearch.

The [`perfect-embedding`](https://github.com/MaterializeInc/perfect-embedding)
transform runs inside the connector. It updates each vector embedding only
when the text for that embedding changes.

## Before you begin

- An OpenSearch 2.x or later cluster. You install version 4 of the [Aiven
  OpenSearch Sink
  Connector](https://github.com/Aiven-Open/opensearch-connector-for-apache-kafka)
  in Step 3. This connector version does not work with OpenSearch 1.x.

- Kafka Connect workers that run in distributed mode. Each worker needs a
  writable `plugin.path`. The connector requires Java 21 or later on each
  worker.

- Credentials for the connector. Choose one of these methods: basic
  authentication with an internal user under fine-grained access control,
  SigV4, or mTLS.

{{< include-md file="shared-content/kafka-sink-search-prerequisites.md" >}}

## Step 1. Set up the sink in Materialize

{{< include-md file="shared-content/kafka-sink-search-debezium-setup.md" >}}

## Step 2. Create the OpenSearch index

The connector writes documents to an index. The index name is the Kafka
topic name in lowercase letters. The sink topic is `articles_v1`, so the
index name is also `articles_v1`.

Create an empty index named `articles_v1`. The sink fills this index later.

An index that holds vectors needs the `index.knn` setting and an explicit
mapping. The connector sets **neither** of these. An index that the
connector creates cannot serve vector queries.

```nofmt
PUT /articles_v1
{
  "settings": { "index.knn": true },
  "mappings": {
    "properties": {
      "id":    { "type": "integer" },
      "title": { "type": "text" },
      "body":  { "type": "text" },
      "views": { "type": "long" },
      "title_embedding": {
        "type": "knn_vector",
        "dimension": 1536,
        "space_type": "cosinesimil",
        "method": { "name": "hnsw" }
      },
      "body_embedding": {
        "type": "knn_vector",
        "dimension": 1536,
        "space_type": "cosinesimil",
        "method": { "name": "hnsw" }
      }
    }
  }
}
```

The output should resemble the following:

```nofmt
{ "acknowledged": true, "shards_acknowledged": true, "index": "articles_v1" }
```

For the list of vector options, see OpenSearch's [`knn_vector` field
reference](https://docs.opensearch.org/latest/mappings/supported-field-types/knn-vector/).

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

1. Extract the [Aiven OpenSearch Sink
   Connector](https://github.com/Aiven-Open/opensearch-connector-for-apache-kafka)
   release zip file onto the worker's `plugin.path`. This connector is not
   available on Confluent Hub.
1. Extract the
   [`perfect-embedding`](https://github.com/MaterializeInc/perfect-embedding/releases)
   release zip file into a separate directory on the same `plugin.path`.
1. Restart the workers. Kafka Connect then finds both plugins.

Create the connector. Send this configuration to the Kafka Connect REST API
with `POST /connectors`:

```json
{
  "name": "opensearch-articles",
  "config": {
    "connector.class": "io.aiven.kafka.connect.opensearch.OpenSearchSinkConnector",
    "topics": "articles_v1",
    "connection.url": "https://<OPENSEARCH_HOST>:9200",
    "connection.username": "<OPENSEARCH_USERNAME>",
    "connection.password": "<OPENSEARCH_PASSWORD>",
    "tasks.max": "4",
    "key.ignore": "false",
    "schema.ignore": "false",
    "index.write.method": "upsert",
    "behavior.on.null.values": "delete",
    "behavior.on.version.conflict": "ignore",
    "max.in.flight.requests": "1",
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
    "errors.deadletterqueue.topic.name": "dlq.opensearch.articles_v1",
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
    GET /connectors/opensearch-articles/status
    ```

    The output should resemble the following:

    ```nofmt
    {
      "name": "opensearch-articles",
      "connector": { "state": "RUNNING" },
      "tasks": [ { "id": 0, "state": "RUNNING" } ]
    }
    ```

1.  Confirm that the documents have their vectors:

    ```nofmt
    GET /articles/_search
    {
      "size": 1,
      "_source": [ "id", "title", "views", "title_embedding" ]
    }
    ```

    The output should resemble the following:

    ```nofmt
    "hits": [
      {
        "_id": "1",
        "_source": {
          "id": 1,
          "title": "Storage engines",
          "views": 42,
          "title_embedding": [ 0.021, -0.118, ... ]
        }
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
