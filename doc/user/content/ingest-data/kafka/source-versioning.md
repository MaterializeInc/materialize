---
title: "Guide: Handle upstream schema changes with zero downtime"
description: "How to handle Kafka topic schema changes on Avro-formatted topics, without any downtime in Materialize"

menu:
    main:
        parent: "kafka"
        identifier: "kafka-source-versioning"
        weight: 85
---

{{< public-preview />}}

Materialize resolves the Avro schema of a Kafka topic when you create a table
from the source, and pins that reader schema to the table. Compatible upstream
schema changes continue to decode, but a table does not expose fields that were
added after it was created.

This guide walks you through how to pick up an upstream schema change without any
downtime in Materialize, using a blue/green cutover:

- Create a new table that reads the evolved schema.
- Recreate the downstream objects against the new table.
- Swap the new objects into place with [`ALTER SCHEMA ... SWAP WITH`](/sql/alter-schema/#swap-with).

Consumers keep referencing the same object names, and the cutover is atomic.

## Prerequisites

Some familiarity with Materialize. If you've never used Materialize before,
start with our [guide to getting started](/get-started/quickstart/).

### Set up a Kafka topic

For this guide, set up an Avro-formatted Kafka topic `orders` whose value schema
is registered in a Confluent Schema Registry. The initial schema has two fields:

```json
{
    "type": "record",
    "name": "order",
    "fields": [
        { "name": "id", "type": "long" },
        { "name": "item", "type": "string" }
    ]
}
```

Produce a few records using this schema.

### Connect your Kafka broker and schema registry to Materialize

In Materialize, create a [connection](/sql/create-connection/) to your Kafka
broker and to your Confluent Schema Registry:

```mzsql
CREATE CONNECTION kafka_conn TO KAFKA (
    BROKER '<broker>',
    SECURITY PROTOCOL PLAINTEXT
);

CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '<schema-registry-url>'
);
```

## Create a source

Create a Kafka [source](/sql/create-source/kafka/). The source connects to the
topic but does not decode it. Each table you create from the source carries its
own format.

```mzsql
CREATE SOURCE orders_src
    FROM KAFKA CONNECTION kafka_conn (TOPIC 'orders');
```

## Create a table from the source

To start ingesting the topic, create a table from the source. We'll add it into
a `prod` schema in Materialize:

```mzsql
CREATE SCHEMA prod;

CREATE TABLE prod.orders
    FROM SOURCE orders_src (REFERENCE "orders")
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
    ENVELOPE NONE;
```

Materialize resolves the latest registered schema when the `CREATE TABLE`
statement runs, and pins it as the reader schema for `prod.orders`. The table
exposes the `id` and `item` fields.

Once you've created a table from the source, the [initial
snapshot](/ingest-data/#snapshotting) of table `prod.orders` will begin.

{{< note >}}

The `TEXT COLUMNS` and `EXCLUDE COLUMNS` options are not supported for Kafka
`CREATE TABLE ... FROM SOURCE`. Unlike PostgreSQL, MySQL, and SQL Server
sources, a Kafka table's columns are determined entirely by its Avro reader
schema. To exclude or recast a field, project or cast it in a view on top of the
table.

{{< /note >}}

## Create a view on top of the table

For this guide, add a materialized view `orders_by_item` (also in schema `prod`)
that counts orders per item:

```mzsql
CREATE MATERIALIZED VIEW prod.orders_by_item AS
    SELECT item, count(*) AS orders
    FROM prod.orders
    GROUP BY item;
```

## Handle an upstream schema change

### A. Evolve the schema in your upstream system

In your upstream system, evolve the topic's value schema in a
[backward-compatible](https://avro.apache.org/docs/++version++/specification/#schema-resolution)
way. Here, add a `quantity` field with a default so that existing records still
decode:

```json
{
    "type": "record",
    "name": "order",
    "fields": [
        { "name": "id", "type": "long" },
        { "name": "item", "type": "string" },
        { "name": "quantity", "type": "int", "default": 0 }
    ]
}
```

Produce records using the new schema.

This operation has no immediate effect in Materialize. `prod.orders` keeps its
pinned reader schema, so it continues to decode new records but resolves away the
`quantity` field. The materialized view `prod.orders_by_item` is unaffected.

### B. Incorporate the new field in Materialize

To pick up the new field, create a new table from the same source in a `staging`
schema. Because you create the table after the schema evolved, it resolves the
latest schema and exposes `quantity`:

```mzsql
CREATE SCHEMA staging;

CREATE TABLE staging.orders
    FROM SOURCE orders_src (REFERENCE "orders")
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
    ENVELOPE NONE;
```

Records produced with the earlier schema take the default value for `quantity`.

Recreate the downstream objects in the `staging` schema, referencing the new
table. Since `staging.orders` exposes `quantity`, the view can use it:

```mzsql
CREATE MATERIALIZED VIEW staging.orders_by_item AS
    SELECT item, sum(quantity) AS total_quantity
    FROM staging.orders
    GROUP BY item;
```

### C. Cut over with a schema swap

Once `staging.orders` has finished snapshotting and its views are hydrated,
atomically swap the `staging` schema into `prod`:

```mzsql
ALTER SCHEMA prod SWAP WITH staging;
```

After the swap, `prod.orders` and `prod.orders_by_item` serve the evolved schema.
Consumers that reference these names pick up the change without any downtime and
without changing their queries. The previous objects now live in the `staging`
schema, where you can drop them once you've validated the cutover:

```mzsql
DROP SCHEMA staging CASCADE;
```

{{< note >}}

This guide covers backward-compatible changes, such as adding a field with a
default. An incompatible change, such as dropping a field that the pinned reader
schema still requires, causes decode errors on the affected table. To recover,
create a new table that reads the current schema, following the steps above,
rather than reading from the existing table.

{{< /note >}}
