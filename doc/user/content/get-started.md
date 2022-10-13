---
title: "Get started with Materialize"
description: "Get started with Materialize"
menu:
  main:
    parent: 'quickstarts'
    weight: 20
    name: 'Get started with Materialize'
aliases:
  - /katacoda/
---

[//]: # "TODO(morsapaes) Once we're GA, add details about signing up and logging into a Materialize account"

This guide walks you through getting started with Materialize, covering:

* Connecting to a streaming data source

* Getting familiar with views, indexes and materialized views

* Exploring common patterns like joins and time-windowing

* Simulating a failure to see active replication in action

{{< note >}}
We are rolling out Early Access to the new, cloud-native version of Materialize. [Sign up](https://materialize.com/register/) to get on the list! 🚀
{{</ note >}}

## Connect

1. Open a terminal window and connect to Materialize using any Materialize-compatible CLI, like `psql`. If you already have `psql` installed on your machine, use the provided connection string to connect:

    Example:

    ```bash
    psql "postgres://user%40domain.com@host:6875/materialize"
    ```

    Otherwise, you can find the steps to install and use your CLI of choice under [Supported tools](/integrations/sql-clients/#supported-tools).

## Explore a streaming source

Materialize allows you to work with streaming data from multiple external sources using nothing but standard SQL. You write arbitrarily complex queries; Materialize takes care of maintaining the results automatically up to date with very low latency.

We'll start with some real-time data produced by Materialize's built-in [load generator source](/sql/create-source/load-generator/), which gives you a quick way to get up and running with no dependencies.

1. Let's use the `AUCTION` load generator source to simulate an auction house where different users are bidding on an ongoing series of auctions:

    ```sql
    CREATE SOURCE auction_house
        FROM LOAD GENERATOR AUCTION
        FOR ALL TABLES
        WITH (SIZE = '3xsmall');
    ```

    The `CREATE SOURCE` statement is a definition of where to find and how to connect to a data source. Submitting the statement will prompt Materialize to start ingesting data into durable storage.

1. The `auction_house` source will be automatically demuxed into multiple subsources, each representing a different underlying table populated by the load generator:

    ```sql
    SHOW SOURCES;
    ```

    ```nofmt
         name      |      type      |  size
    ---------------+----------------+---------
     accounts      | subsource      | 3xsmall
     auction_house | load-generator | 3xsmall
     auctions      | subsource      | 3xsmall
     bids          | subsource      | 3xsmall
     organizations | subsource      | 3xsmall
     users         | subsource      | 3xsmall
     ```

1. Now that we have some data to play around with, let's set up a [cluster](/sql/create-cluster) (logical compute) with one `xsmall` [replica](/sql/create-cluster-replica) (physical compute) so we can start running some queries:

    ```sql
    CREATE CLUSTER auction_house REPLICAS (xsmall_replica (SIZE = 'xsmall'));

    SET CLUSTER = auction_house;
    ```

### Joins

Materialize efficiently supports [all types of SQL joins](/sql/join/#examples) under all the conditions you would expect from a traditional relational database.

The first thing we might want to do with our data is find all the _on-time bids_: bids that arrived before their corresponding auction closed, and so are eligible to be winners. For that, we'll enrich the `bids` stream with the reference `auctions` data.

1. Create a view `on_time_bids` that joins `bids` and `auctions` based on the auction identifier:

    ```sql
    CREATE VIEW on_time_bids AS
        SELECT bids.id AS bid_id,
               auctions.id   AS auction_id,
               auctions.seller,
               bids.buyer,
               auctions.item,
               bids.bid_time,
               auctions.end_time,
               bids.amount
        FROM bids
        JOIN auctions ON bids.auction_id = auctions.id
        WHERE bids.bid_time < auctions.end_time;
    ```

1. To see the results:

    ```sql
    SELECT * FROM on_time_bids LIMIT 5;
    ```

    ```nofmt
     bid_id | auction_id | seller | buyer |    item    |          bid_time          |          end_time          | amount
    --------+------------+--------+-------+------------+----------------------------+----------------------------+--------
        512 |         51 |   1389 |  3546 | Custom Art | 2022-09-16 23:29:38.694+00 | 2022-09-16 23:29:46.694+00 |     98
      66560 |       6656 |    715 |   874 | Custom Art | 2022-09-17 11:24:38.198+00 | 2022-09-17 11:24:48.198+00 |     27
     132352 |      13235 |    202 |  1442 | Custom Art | 2022-09-17 23:17:32.66+00  | 2022-09-17 23:17:40.66+00  |     18
     198144 |      19814 |   2593 |  1510 | Custom Art | 2022-09-18 11:07:06.605+00 | 2022-09-18 11:07:12.605+00 |     52
       2560 |        256 |   1074 |  1982 | Custom Art | 2022-09-16 23:51:53.236+00 | 2022-09-16 23:52:03.236+00 |     36
    ```

    If you re-run the `SELECT` statement at different points in time, you can see the updated results based on the latest data.

### Indexes

1. Create a view `avg_bids` that keeps track of the average bid price for _on-time bids_:

    ```sql
    CREATE VIEW avg_bids AS
        SELECT auction_id,
               avg(amount) AS amount
        FROM on_time_bids
        GROUP BY auction_id;
    ```

    One thing to note here is that we created a [non-materialized view](/overview/key-concepts/#non-materialized-views), which doesn't store the results of the query but simply provides an alias for the embedded `SELECT` statement. The results of a view can be incrementally maintained **in memory** within a
[cluster](/overview/key-concepts/#clusters) by creating an [index](/sql/create-index).

1. Create an index `avg_bids_idx`;

    ```sql
    CREATE INDEX avg_bids_idx ON avg_bids (auction_id);
    ```

    Indexes assemble and incrementally maintain a query’s results updated **in memory** within a cluster, which speeds up query time.

1. To see the results:

    ```sql
    SELECT * FROM avg_bids LIMIT 10;
    ```

    Regardless of how complex the underlying view definition is, querying an indexed view is computationally free because the results are pre-computed and available in memory.

### Materialized views

So far, we've built our auction house strictly using [views](/sql/create-view). Depending on your setup, in some cases you might want to use a [materialized view](/sql/create-materialized-view): a view that is persisted in durable storage and incrementally updated as new data arrives.

1. Create a view that takes the on-time bids and finds the highest bid for each auction:

    ```sql
    CREATE VIEW highest_bid_per_auction AS
        SELECT grp.auction_id,
               bid_id,
               buyer,
               seller,
               item,
               amount,
               bid_time,
               end_time
        FROM
        (SELECT DISTINCT auction_id FROM on_time_bids) grp,
        LATERAL (
            SELECT * FROM on_time_bids
            WHERE auction_id = grp.auction_id
            ORDER BY amount DESC LIMIT 1
    );
    ```

    In other databases, you might have used a window function (like `ROW_NUMBER()`) to implement this query pattern. In Materialize, it can be implemented in a more performant way using a [`LATERAL` subquery](/sql/patterns/top-k/).

### Temporal filters

In Materialize, [temporal filters](/sql/patterns/temporal-filters/) allow you to define time-windows over otherwise unbounded streams of data. This is useful to model business processes or simply to limit resource usage.

1. In this case, we need to filter out rows for auctions that are still ongoing:

    ```sql
    CREATE MATERIALIZED VIEW winning_bids AS
        SELECT * FROM highest_bid_per_auction
        WHERE end_time < mz_now();
    ```

    The `mz_now()` function is used to keep track of the logical time that your query executes (similar to `now()` in other systems, as explained more in-depth in ["now and mz_now functions"](/sql/functions/now_and_mz_now/)). As time advances, only the records that satisfy the time constraint are used in the materialized view.

1. To see the results, let's use `SUBSCRIBE` instead of a vanilla `SELECT`:

    ```sql
    COPY (
        SUBSCRIBE (
            SELECT auction_id, bid_id, item, amount
            FROM winning_bids
    )) TO STDOUT;
    ```

    To cancel out of the stream, press **CTRL+C**.

[//]: # "TODO(morsapaes) Work in consistency query."

## Break something!

1. From a different terminal window, open a new connection to Materialize.

1. Add an additional replica to the `auction_house` cluster:

    ```sql
    CREATE CLUSTER REPLICA auction_house.bigger SIZE = 'small';
    ```

1. To simulate a failure, drop the `xsmall_replica`:

    ```sql
    DROP CLUSTER REPLICA auction_house.xsmall_replica;
    ```

    If you switch to the terminal window running the `SUBSCRIBE` command, you'll see that results are still being pushed out to the client.

## Learn more

That's it! You just created your first materialized view, tried out some common patterns enabled by SQL on streams, and tried to break Materialize! We encourage you to continue exploring the `AUCTION` load generator source using the supported [SQL commands](/sql/), and read through ["What is Materialize?"](/overview/what-is-materialize) for a more comprehensive overview.
