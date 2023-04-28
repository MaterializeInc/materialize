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
  - /install/
---

This guide walks you through getting started with Materialize, including:

- Connecting a streaming data source

- Computing real-time results with indexes and materialized views

- Simulating failure to see active replication in action

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
If you already have a Materialize account, skip to the [next step](#step-2-prepare-your-environment).
{{</ note >}}

1. [Sign up for a Materialize trial](https://materialize.com/register/) using your business email address.

1. Activate your Materialize account.

    Once your account is ready, you'll receive an email from Materialize asking you to activate your account. In the process, you'll create credentials for logging into the Materialize UI.

## Step 2. Prepare your environment

In Materialize, a [cluster](/overview/key-concepts/#clusters) is an **isolated environment**, similar to a virtual warehouse in Snowflake. Within a cluster, you have [replicas](/overview/key-concepts/#cluster-replicas), which are the **physical resources** for doing computational work. Clusters are completely isolated from each other, so replicas can be sized based on the specific task of the cluster, whether that is ingesting data from a source, computing always-up-to-date query results, serving results to clients, or a combination.

For this guide, you'll create 2 clusters, one for ingesting source data and the other for computing and serving query results. Each cluster will contain a single replica at first (you'll explore the value of adding replicas later).

1. In the [Materialize UI](https://console.materialize.com/), enable the region where you want to run Materialize.

    Region setup will take a few minutes.

1. On the **Connect** screen, create a new app password and then copy the `psql` command.

    The app password will be displayed only once, so be sure to copy the password somewhere safe. If you forget your password, you can create a new one.

1. Open a new terminal window, run the `psql` command, and enter your app password.

    In the SQL shell, you'll be connected to a [pre-installed `default` cluster](/sql/show-clusters/#pre-installed-clusters) from which you can get started.

1. Use the [`CREATE CLUSTER`](/sql/create-cluster/) command to create two new clusters, each with a single replica:

    ```sql
    CREATE CLUSTER ingest_qck REPLICAS (r1 (SIZE = '2xsmall'));
    ```

    ```sql
    CREATE CLUSTER compute_qck REPLICAS (r1 (SIZE = '2xsmall'));
    ```

    The `2xsmall` replica size is sufficient for the data ingestion and computation in this getting started scenario.

1. Use the [`SHOW CLUSTER REPLICAS`](https://materialize.com/docs/sql/show-cluster-replicas/) command to check the status of the replicas:

    ```sql
    SHOW CLUSTER REPLICAS WHERE cluster = 'compute_qck' OR cluster = 'ingest_qck';
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

1. Aside from clusters and replicas, sources and most other objects in Materialize are [namespaced](/sql/namespaces/) by database and schema, so start by creating a unique schema within the default `materialize` database:

    ```sql
    CREATE SCHEMA qck;
    ```

    ```sql
    SET search_path = qck;
    ```

1. Use the [`CREATE SOURCE`](/sql/create-source/load-generator/) command to create the auction house source:

    ```sql
    CREATE SOURCE auction_house
      IN CLUSTER ingest_qck
      FROM LOAD GENERATOR AUCTION (TICK INTERVAL '50ms')
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
    ----------------------------+----------------+------
     accounts                   | subsource      |
     auction_house_qck          | load-generator |
     auction_house_qck_progress | subsource      |
     auctions                   | subsource      |
     bids                       | subsource      |
     organizations              | subsource      |
     users                      | subsource      |
    (7 rows)
    ```

    In addition to the `auction_house` load generator source and its subsources, you'll see `auction_house_progress`, which Materialize creates so you can [monitor source ingestion](/sql/create-source/load-generator/#monitoring-source-progress).

    <!-- NOT SURE IF THIS IS CORRECT: In production scenarios, it's important to note that Materialize takes an [initial snapshot](/ops/troubleshooting/#has-my-source-ingested-its-initial-snapshot) of a source that must complete before Materialize can guarantee correct results.   -->

1. Before moving on, look at the schema of the source data you'll be working with:

    ```sql
    SHOW COLUMNS FROM auctions;
    ```
    <p></p>

    ```nofmt
       name   | nullable |           type
    ----------+----------+--------------------------
     id       | f        | bigint
     seller   | f        | bigint
     item     | f        | text
     end_time | f        | timestamp with time zone
    (4 rows)
    ```

    ```sql
    SHOW COLUMNS FROM bids;
    ```
    <p></p>

    ```nofmt
        name    | nullable |           type
    ------------+----------+--------------------------
     id         | f        | bigint
     buyer      | f        | bigint
     auction_id | f        | bigint
     amount     | f        | integer
     bid_time   | f        | timestamp with time zone
    (5 rows)
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

1. First, create a [**view**](/overview/key-concepts/#non-materialized-views):

    ```sql
    CREATE VIEW avg_bids AS
      SELECT auctions.item, avg(bids.amount) AS average_bid
      FROM bids
      JOIN auctions ON bids.auction_id = auctions.id
      WHERE bids.bid_time < auctions.end_time
      GROUP BY auctions.item;
    ```

    This view joins data from `auctions` and `bids` to get the average price of bids that arrived befored their auctions closed.

    Note that, as in other SQL databases, a view in Materialize is simply an alias for the embedded `SELECT` statement. Materialize computes the results of the query only when the view is called.

1. Query the view a few times:

    ```sql
    SELECT * FROM avg_bids;
    ```
    <p></p>


    ```nofmt
            item        |    average_bid
    --------------------+-------------------
     Custom Art         | 50.10550599815441
     Gift Basket        | 50.51195882531032
     City Bar Crawl     | 50.02785145888594
     Best Pizza in Town | 50.20555741546703
     Signed Memorabilia | 49.34376098418278
    (5 rows)

    Time: 738.987 ms
    ```

    ```sql
    SELECT * FROM avg_bids;
    ```
    <p></p>


    ```nofmt
            item        |    average_bid
    --------------------+--------------------
     Custom Art         | 50.135432589422194
     Gift Basket        | 50.485373134328356
     City Bar Crawl     |  50.03637566137566
     Best Pizza in Town |  50.16190159574468
     Signed Memorabilia | 49.354624781849914
    (5 rows)

    Time: 707.403 ms
    ```

    You'll see the average bid change as new auction data streams into Materialize. However, the view retrieves data from durable storage and computes results at query-time, so latency is high and would be much higher with a production dataset.

1. Next, create an [**index**](/overview/key-concepts/#indexes) on the view:

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
    --------------------+--------------------
     Custom Art         | 49.783986655546286
     Gift Basket        |  49.93436483689761
     City Bar Crawl     |  49.93733653007847
     Best Pizza in Town |  50.43617136074242
     Signed Memorabilia |  50.09202958093673
    (5 rows)

    Time: 26.403 ms
    ```

    You'll see the average bids continue to change, but now that the view is indexed and results are pre-computed and stored in memory, latency is down to 26 milliseconds!

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

1. In many cases, you'll want results to be accessible from multiple clusters, however. To achieve this, you use [materialized views](/overview/key-concepts/#materialized-views).

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
     Custom Art         |          10634
     Gift Basket        |          11266
     City Bar Crawl     |          10292
     Best Pizza in Town |          10498
     Signed Memorabilia |          10801
    (5 rows)

    Time: 790.384 ms
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
     Custom Art         |          14373
     Gift Basket        |          15271
     City Bar Crawl     |          14294
     Best Pizza in Town |          14606
     Signed Memorabilia |          14843
    (5 rows)

    Time: 32.064 ms
    ```

    Now that the materialzed view serves results from memory, latency is low again.

## Step 5. Survive failures

Earlier, when you created your clusters, you gave each cluster one [replica](/overview/key-concepts/#cluster-replicas), that is, one physical resource. For the `ingest_qck` cluster, that's the max number of replicas allowed, as clusters for sources can have only one replica. For the `compute_qck` cluster, however, you can increase the number of replicas for greater tolerance to replica failure.

Each replica in a non-source cluster is a logical clone, doing the same computation and holding the same results in memory. This design provides Materialize with active replication, and so long as one replica is still reachable, the cluster continues making progress.

Let's see this in action.

1. Add a second replica to the `compute_qck` cluster:

    ```sql
    CREATE CLUSTER REPLICA compute_qck.r2 SIZE = '2xsmall';
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

1. Once the `r2` replica is ready (`ready=t`), drop the `r1` replica to simulate a failure:

    ```sql
    DROP CLUSTER REPLICA compute_qck.r1;
    ```

1. Query the indexed view that you created in `compute_qck` earlier:

    ```sql
    SELECT * FROM avg_bids;
    ```
    <p></p>

    ```nofmt
            item        |    average_bid
    --------------------+--------------------
     Custom Art         | 49.770776201263864
     Gift Basket        |   49.8909070204407
     City Bar Crawl     | 50.056635368698046
     Best Pizza in Town |  50.50023551577956
     Signed Memorabilia |  50.11854192264935
    (5 rows)

    Time: 23.537 ms
    ```

    As you can see, the results are available despite the failure of one of the cluster's replicas.

## Step 6. Scale up or down

In addition to using replicas to increase fault tolerance, you can add and remove replicas to scale resources up or down according to the needs of a cluster. For example, let's say the `2xsmall` replica in the `compute_qck` cluster is running low on memory.

1. Add the next largest replica, `xsmall`:

    ```sql
    CREATE CLUSTER REPLICA compute_qck.r3 SIZE = 'xsmall';
    ```

1. Use the [`SHOW CLUSTER REPLICAS`](https://materialize.com/docs/sql/show-cluster-replicas/) command to check the status of the new replica:

    ```sql
    SHOW CLUSTER REPLICAS WHERE CLUSTER = 'compute_qck';
    ```
    <p></p>

    ```noftm
       cluster   | replica |  size   | ready
    -------------+---------+---------+-------
     compute_qck | r2      | 2xsmall | t
     compute_qck | r3      | xsmall  | t
    (2 rows)
    ```

1. Once the `r3` replica is ready (`ready=t`), it's safe to remove the `r2` replica:

    ```sql
    DROP CLUSTER REPLICA compute_qck.r2;
    ```

1. Again, because replicas in a cluster are logical clones, the new replica returns results just like the old replica:

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
RESET search_path;
```

```sql
RESET cluster;
```

## What's next?

- Learn more about the [key concepts of Materialize](/overview/key-concepts/)
- Integrate a [streaming data source](https://materialize.com/docs/sql/create-source/)
- Explore [when to use indexes and materialized views](https://materialize.com/blog/views-indexes/)
