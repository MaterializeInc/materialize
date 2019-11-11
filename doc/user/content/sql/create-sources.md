---
title: "CREATE SOURCES"
description: "`CREATE SOURCES` connects Materialize to Kafka, and defines the data coming from Kafka using a schema registry."
menu:
  main:
    parent: 'sql'
---

`CREATE SOURCES` connects Materialize to Kafka streams and lets you interact
with the stream's data as if it were in a SQL table.

## Conceptual framework

To provide data to Materialze, you must create "sources", which are Kafka topics
that you have set up to publish a change feed from an underlying relational
database. In Materialize's current iteration, this only works with databases set
up to publish a change feed to Kafka through [Debezium](https://debezium.io), a
change data capture (CDC) tool for relational databases.

Materialize also needs to understand the structure of the source, so it relies
on receiving the topic's schema from either a Confluent Schema Registry or a
user-supplied Avro schema. Debezium handles this automatically by using
Confluent Schema Registry, so most users do not need to do anything for this step.

After you create the source, Materialize automatically collects all of the data
that streams in, which is then used to supply your queries and views with data.

## Syntax

### Create multiple sources

{{< diagram "create-sources.html" >}}

Field | Use
------|-----
**LIKE** _schema&lowbar;expr_ | If using a Confluent schema registry, create sources from all Kafka sources that match _schema&lowbar;expr_. If not used, Materialize creates sources for all topics published at _kafka&lowbar;src_.<br/><br/>The name for all sources is the name of the table that originated from within your database.
**FROM** _kafka&lowbar;src_ | The Kafka source you want to use (begins with `kafka://`)
**REGISTRY** _registry&lowbar;src_ | Use the Confluent schema registry at _registry&lowbar;src_ to define the structure of the Kafka source.
_avro&lowbar;schema_ | The [Avro schema](https://avro.apache.org/docs/current/spec.html) for the topic.

### Create single source

{{< diagram "create-source.html" >}}

Field | Use
------|-----
_src&lowbar;name_ | The name for the source, which is used as its table name within SQL.
**FROM** _kafka&lowbar;src_ | The Kafka source you want to use (begins with `kafka://`)
**REGISTRY** _registry&lowbar;src_ | Use the Confluent schema registry at _registry&lowbar;src_ to define the structure of the Kafka source
_avro&lowbar;schema_ | The [Avro schema](https://avro.apache.org/docs/current/spec.html) for the topic.

## Details

### Overview

Materialize receives data from Kafka sources, which are set up
to publish a change feed from a relational database (also known as "change data
capture" or CDC).

For Materialize to meaningfully process arbitrary changes from the upstream
database, it requires a "diff envelope", which describes records' old and new
values. In the current landscape, the only CDC tool that provides a diff
envelope from relational databases is [Debezium](https://debezium.io/).

This means to create sources within Materialize, you must:

- Configure Debezium with your database
  ([MySQL](https://debezium.io/documentation/reference/0.10/connectors/mysql.html)/[PostgreSQL](https://debezium.io/documentation/reference/0.10/connectors/postgresql.html))
  and Kafka.
- Define the Kafka sources; these are 1:1 with Kafka topics.
- Provide a schema for Materialize to structure the source's data, preferably
  through Confluent Schema Registry. The source's schema will be mirrored in the
  table-like API offered to you when reading from the source.

   Note that this is handled automatically through Debezium.

After creating the source and connecting to Kafka, Materialize receives all of the data published to the Kafka topic.

### Source requirements

Materialize requires that the data it receives from Kafka have a structure that
contains both the old and new values for any fields within a record (a "diff
envelope"), which lets Differential dataflow process arbitrary changes to the
underlying data, e.g. expressing a record's deletion. 

The only tool that we're aware of that provide data to Kafka in this form is
Debezium through its envelope structure that contains a `before` and `after`
field (although CockroachDB's change data capture feature is close to
operational, as well). 

Ultimately, this means that the only sources that you can create must originate
from Debezium, which must be published through Kafka.

### Source schema

When creating a source, Materialize looks up stream's structure using a
Confluent Schema Registry or with the Avro schema you define. The stream's
schema is then used to create the source within Materialize, which is
essentially equivalent to a table in the language of RDBMSes. 

Once Materialize has determined the structure of the stream, it connects the
underlying Kafka stream directly to its Differential dataflow engine.

### Data storage

As data streams in from Kafka, Materialize's internal Differential instance builds an arrangement (which is
roughly equivalent to an index in the language of RDBMSes). When the source is
initially created this will receive all of the data that the Kafka stream
contains for the topic, e.g. the last 24 hours of data, and construct its
initial arrangement from that. As new data streams in, Materialize will collect that, as well.

However, Materialize has a separate garbage collection period for arrangements,
which works independently from the Kafka stream's retention period.

When [creating views](../create-views), they are populated with all of the data
from their sources that are available from the arrangements within Differential.

## Examples

### Using a Confluent schema registry

```sql
CREATE SOURCES 
LIKE 'mysql.simple.%' 
FROM 'kafka://kafka:9092' 
USING SCHEMA REGISTRY 'http://schema-registry:8081';
```

### Manually defining Avro schema

```sql
CREATE SOURCE 'mysql.simple.user' 
FROM 'kafka://kafka:9092'
USING SCHEMA '{
  "type": "record",
  "name": "envelope",
  "fields": [
    {
      "name": "before",
      "type": [
        {
          "name": "row",
          "type": "record",
          "fields": [
            {"name": "a", "type": "long"},
            {"name": "b", "type": "long"}
          ]
        },
        "null"
      ]
    },
    { "name": "after", "type": ["row", "null"] }
  ]
}';
```

## Related pages

- [`CREATE VIEW`](../create-view)
- [`SELECT`](../select)
