The examples in this guide build a search document for an article catalog tracking its content and page views.

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

The embedding transform compares structured records to find changes, so this
pipeline needs Avro with a schema registry. For other authentication
methods, see [`CREATE CONNECTION`](/sql/create-connection/#kafka).

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
value. The transform compares these two fields to find the columns that
changed. The transform also converts each delete into a tombstone. The
connector applies this tombstone as a document delete. For the full list of
options, see [`CREATE SINK ... INTO KAFKA`](/sql/create-sink/kafka/).
