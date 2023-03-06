---
title: "Get started with Materialize"
description: "Get started with Materialize"
menu:
  main:
    parent: "quickstarts"
    weight: 10
    name: "Get started with Materialize"
aliases:
  - /katacoda/
  - /quickstarts/
---

[//]: # "TODO(morsapaes) Once we're GA, add details about signing up and logging
into a Materialize account"

This guide walks you through getting started with Materialize, covering:

* Connecting to a streaming data source

* Getting familiar with views, indexes and materialized views

* Exploring common patterns like joins and time-windowing

* Simulating a failure to see active replication in action

{{< note >}}
We are rolling out Early Access to the new, cloud-native version of Materialize. [Sign up](https://materialize.com/register/) to get on the list! 🚀
{{</ note >}}

## Set up

### Connect to Materialize

Open a terminal window and connect to Materialize using [a compatible CLI](/integrations/sql-clients/), like `psql`. If you already have a compatible CLI installed, use the connection string provided in the Materialize UI to connect:

```bash
psql "postgres://user%40domain.com@host:6875/materialize"
```

Otherwise, install `psql` using the following instructions:

{{< tabs >}}
{{< tab "macOS">}}

Install `libpq` using [Homebrew](https://brew.sh/):

```bash
brew install libpq
```

Then symlink the `psql` binary to your `/usr/local/bin` directory:
```bash
brew link --force libpq
```

{{< /tab >}}

{{< tab "Linux">}}

Install the `postgresql-client` package using [apt](https://linuxize.com/post/how-to-use-apt-command/):

```bash
sudo apt-get update
sudo apt-get install postgresql-client
```

{{< /tab >}}

{{< tab "Windows">}}
Download and install the [PostgreSQL installer](https://www.postgresql.org/download/windows/) certified by EDB.
{{< /tab >}}
{{< /tabs >}}

### Prepare your environment

Before getting started, let's prepare an isolated environment for experimenting that guarantees we won't interfere with other workloads running in your database. As a first step, create a new [cluster](/overview/key-concepts/#clusters) with dedicated physical resources:

```sql
CREATE CLUSTER quickstart REPLICAS (small_replica (SIZE = '2xsmall'));
```

And make it our active cluster:

```sql
SET cluster = quickstart;
```

Now that **physical isolation** is out of the way, let's also take care of **logical isolation** and create a new schema to make sure we don't run into [namespacing](/sql/namespaces/) annoyances (like naming collisions):

```sql
CREATE SCHEMA qck;

SET search_path = qck;
```

## Explore a streaming source

Materialize allows you to work with streaming data from multiple external data sources, like [Kafka](/sql/create-source/kafka/) and [PostgreSQL](/sql/create-source/postgres/). You write arbitrarily complex **SQL queries**; Materialize takes care of keeping the results up to date as new data arrives.

[//]: # "TODO(morsapaes) Add a diagram + description of the data to the
Quickstarts landing page, then link below"

To get you started, we provide a Kafka cluster with sample data that you can use to explore and learn the basics.

### Create a source

The first step is to declare where to find and how to connect to the sample Kafka cluster. For this, we need three Materialize concepts: [secrets](/sql/create-secret/), [connections](/sql/create-connection/) and [sources](/overview/key-concepts/#sources).

1. **Create secrets.** Secrets allow you to store sensitive credentials securely in Materialize. To retrieve the access credentials, navigate to the [Materialize UI](https://cloud.materialize.com/showSourceCredentials) and replace the placeholders below with the provided values.

    ```sql
    CREATE SECRET qck.kafka_user AS '<KAFKA-USER>';
    CREATE SECRET qck.kafka_password AS '<KAFKA-PASSWORD>';
    CREATE SECRET qck.csr_user AS '<CSR-USER>';
    CREATE SECRET qck.csr_password AS '<CSR-PASSWORD>';
    ```

1. **Create connections.** Connections describe how to connect and authenticate to an external system you want Materialize to read data from. In this case, we want to [create a connection](/sql/create-connection/#kafka) to a Kafka cluster.

    ```sql
    CREATE CONNECTION qck.kafka_connection TO KAFKA (
      BROKER 'pkc-n00kk.us-east-1.aws.confluent.cloud:9092',
      SASL MECHANISMS = 'PLAIN',
      SASL USERNAME = SECRET kafka_user,
      SASL PASSWORD = SECRET kafka_password
    );
    ```

    In addition to Kafka, we need a connection to a Schema Registry that Materialize will use to fetch the schema of our sample data.

    ```sql
    CREATE CONNECTION qck.csr_connection TO CONFLUENT SCHEMA REGISTRY (
      URL 'https://psrc-ko92v.us-east-2.aws.confluent.cloud:443',
      USERNAME = SECRET csr_user,
      PASSWORD = SECRET csr_password
    );
    ```

    Once created, these connections can be reused across multiple `CREATE SOURCE` statements.

1. **Create sources.** Now that we have the details to connect, we can create a source for each Kafka topic we want to use.

    ```sql
    CREATE SOURCE qck.purchases
      FROM KAFKA CONNECTION qck.kafka_connection (TOPIC 'mysql.shop.purchases')
      FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION qck.csr_connection
      ENVELOPE DEBEZIUM
      WITH (SIZE = '3xsmall');
    ```

    ```sql
    CREATE SOURCE qck.items
      FROM KAFKA CONNECTION qck.kafka_connection (TOPIC 'mysql.shop.items')
      FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION qck.csr_connection
      ENVELOPE DEBEZIUM
      WITH (SIZE = '3xsmall');
    ```

    To list the sources we just created:

    ```sql
    SHOW SOURCES FROM qck;
    ```

    ```nofmt
       name    | type  |  size
    -----------+-------+---------
     items     | kafka | 3xsmall
     purchases | kafka | 3xsmall
    ```

    Creating a source will prompt Materialize to **start ingesting data** from the specified topics into durable storage.

### Transform data

With data being ingested into Materialize, we can start building SQL statements to transform it into something actionable.

1. Create a [view](/overview/key-concepts/#non-materialized-views) `item_purchases` that calculates rolling aggregations for each item in the inventory:

    ```sql
    CREATE VIEW qck.item_purchases AS
      SELECT
        item_id,
        SUM(quantity) AS items_sold,
        SUM(purchase_price) AS revenue,
        COUNT(id) AS orders,
        MAX(created_at::timestamp) AS latest_order
      FROM purchases
      GROUP BY item_id;
    ```

    As in other databases, a view doesn't store the results of the query, but simply provides an alias for the embedded `SELECT` statement.

    To keep the results of this view incrementally maintained **in memory** within our cluster, we'll create an [index](/sql/create-index).

1. Create an index `item_purchases_idx`:

   ```sql
   CREATE INDEX item_purchases_idx ON qck.item_purchases (item_id);
   ```

1. To see the results:

    ```sql
    SELECT * FROM item_purchases LIMIT 10;

    --Lookups will be fast because of the index on item_id!
    SELECT * FROM item_purchases WHERE item_id = 768;
    ```

    Regardless of how complex the underlying view definition is, querying an **indexed** view is computationally free because the results are pre-computed and available in memory. Using indexes can help decrease query time, as well as optimize other operations that depend on the view (like joins).

### Enrich data with joins

Materialize efficiently supports [all types of SQL joins](/sql/join/#examples) under all the conditions you would expect from a relational database, including maintaining strong [consistency guarantees](/overview/isolation-level/).

Now that we are keeping track of purchases in the `item_purchases` indexed view, let's enrich this view with the reference item information streaming in from the `items` source.

1. Create a [materialized view](/overview/key-concepts/#materialized-views) `item_summary` that joins `item_purchases` and `items` based on the item identifier:

    ```sql
      CREATE MATERIALIZED VIEW qck.item_summary AS
      SELECT
          ip.item_id AS item_id,
          i.name AS item_name,
          i.category AS item_category,
          SUM(ip.items_sold) AS items_sold,
          SUM(ip.revenue) AS revenue,
          SUM(ip.orders) AS orders,
          ip.latest_order AS latest_order
      FROM item_purchases ip
      JOIN items i ON ip.item_id = i.id
      GROUP BY item_id, item_name, item_category, latest_order;
    ```

    Unlike a view, the results of a materialized view are **persisted and incrementally updated** in durable storage as new data arrives. Because the storage layer is shared across all clusters, materialized views allow you to **share data** between different environments.

1. To see the results:

    ```sql
    SELECT * FROM item_summary ORDER BY revenue DESC LIMIT 5;
    ```

### Work with time windows

In streaming, time keeps ticking along — unlike in batch, where data is frozen at query time. [Temporal filters](/sql/patterns/temporal-filters/) allow you to define time-windows over otherwise unbounded streams of data.

1. Create a view `item_summary_5min` that keeps track of any items that had orders in the past 5 minutes:

    ```sql
    CREATE VIEW qck.item_summary_5min AS
    SELECT *
    FROM item_summary
    WHERE mz_now() >= latest_order
    AND mz_now() < latest_order + INTERVAL '5' MINUTE;
    ```

    “In the past 5 minutes” is a moving target that changes as time ticks along. The `mz_now()` function is used to keep track of the logical time for your query (similarly to `now()` in other databases, as explained more in-depth in ["now and mz_now functions"](/sql/functions/now_and_mz_now/)). As time advances, records progressively falling outside this 5-minute window will be expired, and new records that satisfy the time constraint will be used in the view.


1. To see the results, let's use `SUBSCRIBE` instead of a vanilla `SELECT`:

    ```sql
    COPY (
        SUBSCRIBE (
            SELECT item_name, item_category, revenue
            FROM item_summary_5min
    )) TO STDOUT;
    ```

    To cancel out of the subscription, press **CTRL+C**.

## Break something!

1. From a different terminal window, open a new connection to Materialize.

1. Add an additional replica to the `quickstart` cluster:

    ```sql
    CREATE CLUSTER REPLICA quickstart.bigger SIZE = 'xsmall';
    ```

1. To simulate a failure, drop the `small_replica`:

    ```sql
    DROP CLUSTER REPLICA quickstart.small_replica;
    ```

    If you switch to the terminal window running the `SUBSCRIBE` command, you'll see that results are still being pushed out to the client!

## What's next?

That's it! You just created your first transformations on streaming data using common SQL patterns, and tried to break Materialize! We encourage you to continue exploring the sample Kafka source, or try one of the use-case specific quickstarts. If you’re done for the time being, remember to clean up the environment:

```sql
DROP CLUSTER quickstart CASCADE;

DROP SCHEMA qck CASCADE;
```

For a more comprehensive overview of the basic concepts behind Materialize, take a break and read through ["What is Materialize?"](/overview/what-is-materialize).
