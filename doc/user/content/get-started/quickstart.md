---
title: "Quickstart with Materialize"
description: "Quickstart with Materialize"
menu:
  main:
    parent: "get-started"
    weight: 15
    name: "Quickstart"
aliases:
  - /katacoda/
  - /quickstarts/
  - /install/
  - /get-started/
---

This guide walks you through getting started with Materialize, including:

- Connecting a streaming data source

- Computing real-time results with indexes and materialized views

- Using replication to increase fault tolerance

- Scaling up or down based on computational needs

## Before you begin

Materialize is wire-compatible with PostgreSQL, which means it integrates with most SQL clients and [other third-party tools](/integrations/) that support PostgreSQL.

In this guide, you'll use [`psql`](https://www.postgresql.org/docs/current/app-psql.html) to interact with Materialize, so make sure you have it installed locally.

{{< tabs >}}
{{< tab "macOS">}}

Using [Homebrew](https://brew.sh/), install `libpq` and symlink the `psql` binary to `/usr/local/bin`:

```bash
brew install libpq
```

```bash
brew link --force libpq
```

{{< /tab >}}

{{< tab "Linux">}}

Using [`apt`](https://linuxize.com/post/how-to-use-apt-command/), install the `postgresql-client` package:

```bash
sudo apt-get update
```

```bash
sudo apt-get install postgresql-client
```

{{< /tab >}}

{{< tab "Windows">}}
Download and install the [PostgreSQL installer](https://www.postgresql.org/download/windows/) certified by EDB.
{{< /tab >}}
{{< /tabs >}}

## Step 1. Start a free trial

With a free trial, you get 14 days of free access to Materialize resources worth up to 4 credits per hour. This limit should accommodate most trial scenarios. For more details, see [Free trial FAQs](/free-trial-faqs/).

{{< note >}}
If you already have a Materialize account, skip to the [next step](#step-2-create-clusters).
{{</ note >}}

1. [Sign up for a Materialize trial](https://materialize.com/register/?utm_campaign=General&utm_source=documentation) using your business email address.

1. Activate your Materialize account.

    Once your account is ready, you'll receive an email from Materialize asking you to activate your account. In the process, you'll create credentials for logging into the Materialize UI.

## Step 2. Create clusters

In Materialize, a [cluster](/get-started/key-concepts/#clusters) is an isolated environment, similar to a virtual warehouse in Snowflake. When you create a cluster, you choose the size of its physical compute resource (i.e., [replica](/get-started/key-concepts/#cluster-replicas)) based on the work you need the cluster to do, whether ingesting data from a source, computing always-up-to-date query results, serving results to clients, or a combination.

For this guide, you'll create 2 clusters, one for ingesting source data and the other for computing and serving query results. For now, each cluster will get a single `2xsmall` physical compute resource. Later, you'll explore how to use replication to increase tolerance to failure.

1. In the [Materialize UI](https://console.materialize.com/), enable the region where you want to run Materialize.

    Region setup will take a few minutes.

1. On the **Connect** screen, create a new app password and then copy the `psql` command.

    The app password will be displayed only once, so be sure to copy the password somewhere safe. If you forget your password, you can create a new one.

1. Open a new terminal window, run the `psql` command, and enter your app password.

    In the SQL shell, you'll be connected to a [pre-installed `default` cluster](/sql/show-clusters/#pre-installed-clusters) from which you can get started.

1. Use the [`CREATE CLUSTER`](/sql/create-cluster/) command to create two new clusters:

    ```sql
    CREATE CLUSTER ingest_qck SIZE = '2xsmall';
    ```

    ```sql
    CREATE CLUSTER compute_qck SIZE = '2xsmall';
    ```

    The `2xsmall` size is sufficient for the data ingestion and computation in this getting started scenario.

1. The physical compute resources in your clusters are called [`replicas`](/get-started/key-concepts/#cluster-replicas). Use the [`SHOW CLUSTER REPLICAS`](https://materialize.com/docs/sql/show-cluster-replicas/) command to check the status of the replicas:

    ```sql
    SHOW CLUSTER REPLICAS WHERE cluster IN ('compute_qck', 'ingest_qck');
    ```
    <p></p>

    ```noftm
       cluster   | replica |  size   | ready
    -------------+---------+---------+-------
     compute_qck | r1      | 2xsmall | t
     ingest_qck  | r1      | 2xsmall | t
    (2 rows)
    ```

    Once both replicas are ready (`ready=t`), move on to the next step.

## Step 3. Ingest streaming data

Materialize supports streaming data from multiple external sources, including [Kafka](/sql/create-source/kafka/) and [PostgreSQL](/sql/create-source/postgres/). The process for integrating a source typically involves configuring the source's network and creating connection objects in Materialize.

For this guide, you'll use a [built-in load generator](/sql/create-source/load-generator/#auction) that simulates an auction house, where users bid on an ongoing series of auctions.

1. Most objects in Materialize are [namespaced](/sql/namespaces/) by database and schema, including sources, so start by creating a unique schema within the default `materialize` database:

    ```sql
    CREATE SCHEMA qck;
    ```

    ```sql
    SET schema = qck;
    ```

1. Use the [`CREATE SOURCE`](/sql/create-source/load-generator/) command to create the auction house source:

    ```sql
    CREATE SOURCE auction_house
      IN CLUSTER ingest_qck
      FROM LOAD GENERATOR AUCTION
      (TICK INTERVAL '500ms')
      FOR ALL TABLES;
    ```

    Note that the `IN CLUSTER` clause attaches this source to the existing `ingest_qck` cluster, but it's also possible to create a cluster and replica at the time of source creation using the [`WITH SIZE`](/sql/create-source/load-generator/#with-options) option.

1. Now that you've created a source, Materialize starts ingesting data into durable storage, automatically splitting the stream into multiple _subsources_ that represent different tables. Use the [`SHOW SOURCES`](/sql/show-sources/) command to get an idea of the data being generated:

    ```sql
    SHOW SOURCES;
    ```
    <p></p>

    ```nofmt
            name            |      type      | size
    ------------------------+----------------+------
     accounts               | subsource      |
     auction_house          | load-generator |
     auction_house_progress | progress       |
     auctions               | subsource      |
     bids                   | subsource      |
     organizations          | subsource      |
     users                  | subsource      |
    (7 rows)
    ```

    In addition to the `auction_house` load generator source and its subsources, you'll see `auction_house_progress`, which Materialize creates so you can [monitor source ingestion](/sql/create-source/load-generator/#monitoring-source-progress).

1. Before moving on, get a sense of the data you'll be working with:

    ```sql
    SELECT * FROM auctions LIMIT 1;
    ```
    <p></p>

    ```nofmt
     id | seller |        item        |          end_time
    ----+--------+--------------------+----------------------------
      1 |   1824 | Best Pizza in Town | 2023-06-21 21:25:04.838+00
    (1 row)
    ```

    ```sql
    SELECT * FROM bids LIMIT 1;
    ```
    <p></p>

    ```nofmt
     id | buyer | auction_id | amount |          bid_time
    ----+-------+------------+--------+----------------------------
     10 |  3844 |          1 |     59 | 2023-06-21 21:24:54.838+00
    (1 row)
    ```

## Step 4. Compute real-time results

With auction data streaming in, you can now explore the unique value of Materialize: computing real-time results over fast-changing data.

1. Switch to your `compute_qck` cluster:

    ```sql
    SET CLUSTER = compute_qck;
    ```

1. Enable `psql`'s timing feature so you can see how quickly results are returned:

    ```sql
    \timing
    ```

1. First, create a [**view**](/get-started/key-concepts/#non-materialized-views):

    ```sql
    CREATE VIEW avg_bids AS
      SELECT auctions.item, avg(bids.amount) AS average_bid
      FROM bids
      JOIN auctions ON bids.auction_id = auctions.id
      WHERE bids.bid_time < auctions.end_time
      GROUP BY auctions.item;
    ```

    This view joins data from `auctions` and `bids` to get the average price of bids that arrived before their auctions closed.

    Note that, as in other SQL databases, a view in Materialize is simply an alias for the embedded `SELECT` statement. Materialize computes the results of the query only when the view is called.

1. Query the view a few times:

    ```sql
    SELECT * FROM avg_bids;
    ```
    <p></p>


    ```nofmt
            item        |    average_bid
    --------------------+--------------------
     Custom Art         |                 46
     Gift Basket        |              57.25
     City Bar Crawl     | 48.333333333333336
     Best Pizza in Town |   50.4390243902439
     Signed Memorabilia | 54.916666666666664
    (5 rows)

    Time: 919.607 ms
    ```

    ```sql
    SELECT * FROM avg_bids;
    ```
    <p></p>


    ```nofmt
            item        |    average_bid
    --------------------+-------------------
     Custom Art         | 51.06666666666667
     Gift Basket        |              57.5
     City Bar Crawl     | 46.61818181818182
     Best Pizza in Town | 50.67307692307692
     Signed Memorabilia | 49.06896551724138
    (5 rows)

    Time: 458.388 ms
    ```

    You'll see the average bid change as new auction data streams into Materialize. However, the view retrieves data from durable storage and computes results at query-time, so latency is high and would be much higher with a production dataset.

1. Next, create an [**index**](/get-started/key-concepts/#indexes) on the view:

    ```sql
    CREATE INDEX avg_bids_idx ON avg_bids (item);
    ```

    🚀🚀🚀 This is where Materialize becomes a true [streaming database](https://materialize.com/guides/streaming-database/). When you use an index, **Materialize incrementally computes the results of the indexed query in memory as new data arrives**.

1. Query the view again:

    ```sql
    SELECT * FROM avg_bids;
    ```
    <p></p>

    ```nofmt
            item        |    average_bid
    --------------------+-------------------
     Custom Art         | 51.78431372549019
     Gift Basket        |  53.8448275862069
     City Bar Crawl     | 44.68181818181818
     Best Pizza in Town | 50.63636363636363
     Signed Memorabilia |              43.3
    (5 rows)

    Time: 37.004 ms
    ```

    You'll see the average bids continue to change, but now that the view is indexed and results are pre-computed and stored in memory, latency is down to 37 milliseconds!

1. One thing to note about indexes is that they exist only in the cluster where they are created. To experience this, switch to the `default` cluster and query the view again:

    ```sql
    SET CLUSTER = default;
    ```

     ```sql
    SELECT * FROM avg_bids;
    ```
    <p></p>

    ```nofmt
            item        |    average_bid
    --------------------+--------------------
     Custom Art         |  49.76620397600282
     Gift Basket        | 49.850028105677346
     City Bar Crawl     |  50.08233974737339
     Best Pizza in Town |  50.46824567514223
     Signed Memorabilia |  50.12977674688315
    (5 rows)

    Time: 846.322 ms
    ```

    Latency is high again because the index you created on the view exists only inside the `compute_qck` cluster. In the `default` cluster, where you are currently, you don't have access to the index's pre-computed results. Instead, the view once again retrieves data from durable storage and computes the results at query-time.

1. In many cases, you'll want results to be accessible from multiple clusters, however. To achieve this, you use [materialized views](/get-started/key-concepts/#materialized-views).

    Like an index, a materialized view incrementally computes the results of a query as new data arrives. But unlike an index, **a materialized view persists its results to durable storage** that is accessible to all clusters.

    To see this in action, confirm that you are in the `default` cluster and then create a materialized view:

    ```sql
    SHOW CLUSTER;
    ```
    <p></p>

    ```noftm
     cluster
    ---------
     default
    (1 row)
    ```

    ```sql
    CREATE MATERIALIZED VIEW num_bids AS
      SELECT auctions.item, count(bids.id) AS number_of_bids
      FROM bids
      JOIN auctions ON bids.auction_id = auctions.id
      WHERE bids.bid_time < auctions.end_time
      GROUP BY auctions.item;
    ```

    The `SELECT` in this materialized view joins data from `auctions` and `bids`, but this time to get the number of eligible bids per item.

1. Switch to the `compute_qck` cluster and query the materialized view:

    ```sql
    SET CLUSTER = compute_qck;
    ```

    ```sql
    SELECT * FROM num_bids;
    ```
    <p></p>

    ```nofmt
            item        | number_of_bids
    --------------------+----------------
     Custom Art         |            111
     Gift Basket        |            101
     City Bar Crawl     |            133
     Best Pizza in Town |            126
     Signed Memorabilia |             83
    (5 rows)

    Time: 146.928 ms
    ```

    As you can see, although the materialized view was created in the `default` cluster, its results are available from other clusters as well because they are in shared, durable storage.

1.  If retrieving a materialized view's results from storage is too slow, you can create an index on the materialized view as well:

    ```sql
    CREATE INDEX num_bids_idx ON num_bids (item);
    ```

    ```sql
    SELECT * FROM num_bids;
    ```
    <p></p>

    ```nofmt
            item        | number_of_bids
    --------------------+----------------
     Custom Art         |            116
     Gift Basket        |            113
     City Bar Crawl     |            142
     Best Pizza in Town |            131
     Signed Memorabilia |             87
    (5 rows)

    Time: 34.888 ms
    ```

    Now that the materialzed view serves results from memory, latency is low again.

## Step 5. Survive failures

The clusters you created earlier each got a single replica (i.e., physical compute resource) by default. For clusters used to ingest source data (e.g., `ingest_qck`), that's the max replicas allowed. However, for clusters used to do computation or serving (e.g., `compute_qck`), you can increase the replication factor to increase the cluster's tolerance to failure.

Each replica in a cluster is a logical clone, doing the same computation and holding the same results in memory. When a cluster has a replication factor higher than 1, and one of the replicas fails, the cluster can therefore continue making progress with the remaining replicas and service continues uninterrupted.

1. Use the [`ALTER CLUSTER`](/sql/alter-cluster/) command to increase the replication factor of the `compute_qck` cluster:

    ```sql
    ALTER CLUSTER compute_qck SET (REPLICATION FACTOR 2);
    ```

1. Check the status of the new replica:

    ```sql
    SHOW CLUSTER REPLICAS WHERE CLUSTER = 'compute_qck';
    ```
    <p></p>

    ```noftm
       cluster   | replica |  size   | ready
    -------------+---------+---------+-------
     compute_qck | r1      | 2xsmall | t
     compute_qck | r2      | 2xsmall | t
    (2 rows)
    ```

    When the new replica shows `ready=t`, Materialize has acquired physical compute resources for the new replica and is catching it up to the initial replica. Once the new replica is caught up, the two replicas are logical clones that do the same computation and hold the same results.

## Step 6. Scale up or down

You can scale the physical compute resources of a cluster up or down based on need. For example, let's say `2xsmall` is not providing enough memory for the work of the `compute_qck` cluster.

1. Use the [`ALTER CLUSTER`](/sql/alter-cluster/) command to bump the cluster up to the next largest size, `xsmall`:

    ```sql
    ALTER CLUSTER compute_qck SET (SIZE 'xsmall');
    ```

    Behind the scenes, this command adds a new `xsmall` replica and removes the `2xsmall` replica. There may be an interruption in the cluster's ability to serve queries while the new replica catches up to where the old replica left off.

1. Once the new replica is caught up, which should happen very quickly in this case, the new `xsmall` replica returns results just like the old `2xsmall` replica:

    ```sql
    SELECT * FROM avg_bids;
    ```
    <p></p>

    ```nofmt
            item        |    average_bid
    --------------------+-------------------
     Custom Art         | 49.82171985815603
     Gift Basket        | 49.81225672519678
     City Bar Crawl     | 50.16030259365994
     Best Pizza in Town | 50.32564214192425
     Signed Memorabilia | 49.96557507282196
    (5 rows)

    Time: 31.655 ms
    ```

## Step 7. Clean up

Once you're done exploring the auction house source, remember to clean up your environment:

```sql
DROP SCHEMA qck CASCADE;
```

```sql
DROP CLUSTER ingest_qck;
```

```sql
DROP CLUSTER compute_qck;
```

```sql
RESET schema;
```

```sql
RESET cluster;
```

## What's next?

- Learn more about the [key concepts of Materialize](/get-started/key-concepts/)
- Integrate a [streaming data source](https://materialize.com/docs/sql/create-source/)
- Explore [when to use indexes and materialized views](https://materialize.com/blog/views-indexes/)
