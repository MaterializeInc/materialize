---
title: "Quickstart"
description: "Learn the basics of Materialize."
menu:
  main:
    parent: "get-started"
    weight: 10
    name: "Quickstart"
aliases:
  - /katacoda/
  - /quickstarts/
  - /install/
---

{{% text-style %}}

Materialize provides always-fresh results while also providing [strong
consistency guarantees](/get-started/isolation-level/). In Materialize, both
[indexes](/concepts/indexes/ "Indexes represents query results stored in memory
within a cluster") and [materialized views](/concepts/views/#materialized-views)
**incrementally update** results when Materialize ingests new data; i.e., work
is performed on writes. Because work is performed on writes, reads from these
objects return up-to-date results while being computationally **free**.

In this quickstart, you will continuously ingest a sample auction data set to
build an operational use case around finding auction winners and auction
flippers. Specifically, you will:

- Create and query various [views](/concepts/views/) on sample auction data. The
  data is continually generated at 1 second intervals to mimic a data-intensive
  workload.

- Create an [index](/concepts/indexes "Indexes represents query results stored
  in memory within a cluster") to compute and store view results in memory. As
  new auction data arrives, the index **incrementally updates** view
  results instead of recalculating the results from scratch, making fresh
  results immediately available for reads.

- Create and query views to verify that Materialize always serves
  **consistent results**.

![Image of Quickstart in the Materialize Console](/images/quickstart-console.png "Quickstart in the Materialize Console")

## Prerequisite

To get started with Materialize Cloud, you will need a Materialize account. If
you do not have an account, you can [sign up for a free
trial](https://materialize.com/register/?utm_campaign=General&utm_source=documentation).

Alternatively:

- You can [download the Materialize
Emulator](/get-started/install-materialize-emulator/). However, the Materialize
Emulator does not provide the full experience of using Materialize.

- You can run against your [Self-managed Materialize].

[Self-managed Materialize]: https://materialize.com/docs/installation/

## Step 0. Open the SQL Shell

- If you have a Materialize account, navigate to the [Materialize
  Console](/console/) and sign in. By default, you should
  be in the SQL Shell. If you're already signed in, you can access the SQL Shell in the left-hand menu.

- If you are using the Materialize Emulator, open the Materialize Console in
  your browser at [http://localhost:6874](http://localhost:6874).

- If you are running against your own [Self-managed
  Materialize], open your deployment's Materialize Console.

## Step 1. Create a schema

By default, you are using the `quickstart` cluster, working in the
`materialize.public` [namespace](/sql/namespaces/), where:

- A [cluster](/concepts/clusters/) is an isolated pool of compute resources
  (CPU, memory, and scratch disk space) for running your workloads),

- `materialize` is the database name, and

- `public` is the schema name.

Create a separate schema for this quickstart. For a schema name to be valid:

- The first character must be either: an ASCII letter (`a-z` and `A-Z`), an
  underscore (`_`), or a non-ASCII character.

- The remaining characters can be: an ASCII letter (`a-z` and `A-Z`), ASCII
  numbers (`0-9`), an underscore (`_`), dollar signs (`$`), or a non-ASCII character.

Alternatively, by double-quoting the name, you can bypass the aforementioned
constraints with the following exception: schema names, whether double-quoted or
not, cannot contain the dot (`.`).

See also [Naming restrictions](/sql/identifiers/#naming-restrictions).

1. Enter a schema name in the text field and click the `Create` button.

1. Switch to the new schema. From the top of the SQL Shell, select your schema
   from the namespace dropdown.

## Step 2. Create the source

[Sources](/concepts/sources/) are external systems from which Materialize reads
in data. This tutorial uses Materialize's [sample `Auction` load
generator](/sql/create-source/load-generator/#auction) to create the source.

1. Create the [source](/concepts/sources "External systems from which
   Materialize reads data.") using the [`CREATE SOURCE`](/sql/create-source/)
   command.

   For the [sample `Auction` load
   generator](/sql/create-source/load-generator/#auction), the quickstart uses
   [`CREATE SOURCE`](/sql/create-source/) with the `FROM LOAD GENERATOR` clause
   that works specifically with Materialize's sample data generators. The
   tutorial specifies that the generator should emit new data every 1s.

    ```mzsql
    CREATE SOURCE auction_house
    FROM LOAD GENERATOR AUCTION
    (TICK INTERVAL '1s', AS OF 100000)
    FOR ALL TABLES;
    ```

    `CREATE SOURCE` can create **multiple** tables (referred to as `subsources`
    in Materialize) when ingesting data from multiple upstream tables. For each
    upstream table that is selected for ingestion, Materialize creates a
    subsource.

1. Use the [`SHOW SOURCES`](/sql/show-sources/) command to see the results of
   the previous step.

    ```mzsql
    SHOW SOURCES;
    ```

    The output should resemble the following:

    ```nofmt
    | name                   | type           | cluster    | comment |
    | ---------------------- | -------------- | ---------- | ------- |
    | accounts               | subsource      | quickstart |         |
    | auction_house          | load-generator | quickstart |         |
    | auction_house_progress | progress       | null       |         |
    | auctions               | subsource      | quickstart |         |
    | bids                   | subsource      | quickstart |         |
    | organizations          | subsource      | quickstart |         |
    | users                  | subsource      | quickstart |         |
    ```

    A [`subsource`](/sql/show-subsources) is how Materialize refers to a table
    that has the following properties:

    - A subsource can only be written by the source; in this case, the
      load-generator.

    - Users can read from subsources.

1. Use the [`SELECT`](/sql/select) statement to query `auctions` and `bids`.

    * View a sample row in `auctions`:

      ```mzsql
      SELECT * FROM auctions LIMIT 1;
      ```

      The output should return a single row (your results may differ):

      ```nofmt
       id    | seller | item               | end_time
      -------+--------+--------------------+---------------------------
       29550 | 2468   | Best Pizza in Town | 2024-07-25 18:24:25.805+00
      ```

    * View a sample row in `bids`:

      ```mzsql
      SELECT * FROM bids LIMIT 1;
      ```

      The output should return a single row (your results may differ):

      ```nofmt
       id     | buyer | auction_id | amount | bid_time
      --------+-------+------------+--------+---------------------------
       295641 | 737   | 29564      | 72     | 2024-07-25 18:25:42.911+00
      ```


    * To view the relationship between `auctions` and `bids`, you can join by
      the auction id:

      ```mzsql
      SELECT a.*, b.*
      FROM auctions AS a
      JOIN bids AS b
        ON a.id = b.auction_id
      LIMIT 3;
      ```

      The output should return (at most) 3 rows (your results may
      differ):

      ```nofmt
      | id    | seller | item               | end_time                   | id     | buyer | auction_id | amount | bid_time                   |
      | ----- | ------ | ------------------ | -------------------------- | ------ | ----- | ---------- | ------ | -------------------------- |
      | 15575 | 158    | Signed Memorabilia | 2024-07-25 20:30:25.085+00 | 155751 | 215   | 15575      | 27     | 2024-07-25 20:30:16.085+00 |
      | 15575 | 158    | Signed Memorabilia | 2024-07-25 20:30:25.085+00 | 155750 | 871   | 15575      | 63     | 2024-07-25 20:30:15.085+00 |
      | 15575 | 158    | Signed Memorabilia | 2024-07-25 20:30:25.085+00 | 155752 | 2608  | 15575      | 16     | 2024-07-25 20:30:17.085+00 |
      ```

      Subsequent steps in this quickstart uses a query to find winning bids for
      auctions to show how Materialize uses views and indexes to provide
      immediately available up-to-date results for various queries.

## Step 3. Create a view to find winning bids

A [view](/concepts/views/) is a saved name for the underlying `SELECT`
statement, providing an alias/shorthand when referencing the query. The
underlying query is not executed during the view creation; instead, the
underlying query is executed when the view is referenced.

Assume you want to find the winning bids for auctions that have ended. The
winning bid for an auction is the highest bid entered for an auction before the
auction ended. As new auction and bid data appears, the query must be rerun to
get up-to-date results.

1. Using the [`CREATE VIEW`](/sql/create-view/) command, create a
   [**view**](/concepts/views/ "Saved name/alias for a query") to find the
   winning (highest) bids.

   ```mzsql
   CREATE VIEW winning_bids AS
   SELECT DISTINCT ON (a.id) b.*, a.item, a.seller
   FROM auctions AS a
    JOIN bids AS b
      ON a.id = b.auction_id
   WHERE b.bid_time < a.end_time
     AND mz_now() >= a.end_time
   ORDER BY a.id,
     b.amount DESC,
     b.bid_time,
     b.buyer;
   ```

   Materialize provides an idiomatic way to perform [Top-K queries](/transform-data/idiomatic-materialize-sql/top-k/)
   using the [`DISTINCT ON`](/transform-data/idiomatic-materialize-sql/top-k/#for-k--1-1)
   clause. This clause is used to group by account `id` and return the first
   element within that group according to the specified ordering.

1. [`SELECT`](/sql/select/) from the view to execute the underlying query.
   For example:

   ```mzsql
   SELECT * FROM winning_bids
   ORDER BY bid_time DESC
   LIMIT 10;
   ```

   ```mzsql
   SELECT * FROM winning_bids
   WHERE item = 'Best Pizza in Town'
   ORDER BY bid_time DESC
   LIMIT 10;
   ```

   Since new data is continually being ingested, you must rerun the query to get
   the up-to-date results. Each time you query the view, you are re-running the
   underlying statement, which becomes less performant as the amount of data
   grows.

   In Materialize, to make the queries more performant even as data
   continues to grow, you can create [**indexes**](/concepts/indexes/) on views.
   Indexes provide always fresh view results in memory within a cluster by
   performing incremental updates as new data arrives. Queries can then read
   from the in-memory, already up-to-date results instead of re-running the
   underlying statement, making queries **computationally free and more
   performant**.

   In the next step, you will create an index on `winning_bids`.

## Step 4. Create an index to provide up-to-date results

Indexes in Materialize represents query results stored in memory within a
cluster. In Materialize, you can create [indexes](/concepts/indexes/) on views
to provide always fresh, up-to-date view results in memory within a cluster.
Queries can then read from the in-memory, already up-to-date results instead of
re-running the underlying statement.

To provide the up-to-date results, indexes **perform incremental updates** as
inputs change instead of recalculating the results from scratch. Additionally,
indexes can also help [optimize operations](/transform-data/optimization/) like
point lookups and joins.

1. Use the [`CREATE INDEX`](/sql/create-index/) command to create the following
   index on the `winning_bids` view.

    ```mzsql
    CREATE INDEX wins_by_item ON winning_bids (item);
    ```

   During the index creation, the underlying `winning_bids` query is executed,
   and the view results are stored in memory within the cluster. As new data
   arrives, the index **incrementally updates** the view results in memory.
   Because incremental work is performed on writes, reads from indexes return
   up-to-date results and are computationally **free**.

   This index can **also** help [optimize
   operations](/transform-data/optimization/) like point lookups and [delta
   joins](/transform-data/optimization/#optimize-multi-way-joins-with-delta-joins)
   on the index column(s) as well as support ad-hoc queries.

1. Rerun the previous queries on `winning_bids`.

    ```mzsql
    SELECT * FROM winning_bids
    ORDER BY bid_time DESC
    LIMIT 10;
    ```

    ```mzsql
    SELECT * FROM winning_bids
    WHERE item = 'Best Pizza in Town'
    ORDER BY bid_time DESC
    LIMIT 10;
    ```

   The queries should be faster since they use the in-memory, already up-to-date
   results computed by the index.

## Step 5. Create views and a table to find flippers in real time

For this quickstart, auction flipping activities are defined as when a user buys
an item in one auction and resells the same item at a higher price within an
8-day period. This step finds auction flippers in real time, based on
auction flipping activity data and known flippers data. Specifically, this step
creates:

- A view to find auction flipping activities. Results are updated as new data
  comes in (at 1 second intervals) from the data generator.

- A table that maintains known auction flippers. You will manually enter new
  data to this table.

- A view to immediately see auction flippers based on both the flipping
  activities view and the known auction flippers table.

1. Create a view to detect auction flipping activities.

    ```mzsql
    CREATE VIEW flip_activities AS
    SELECT w2.seller as flipper_id,
           w2.item AS item,
           w2.amount AS sold_amount,
           w1.amount AS purchased_amount,
           w2.amount - w1.amount AS diff_amount,
           datediff('days', w2.bid_time, w1.bid_time) AS timeframe_days
     FROM  winning_bids AS w1
       JOIN winning_bids AS w2
         ON w1.buyer = w2.seller   -- Buyer and seller are the same
            AND w1.item = w2.item  -- Item is the same
     WHERE w2.amount > w1.amount   -- But sold at a higher price
       AND datediff('days', w2.bid_time, w1.bid_time) < 8;
    ```

    The `flip_activities` view can use the index created on `winning_bids` view
    to provide up-to-date data.

    To view a sample row in `flip_activities`, run the following
    [`SELECT`](/sql/select) command:

    ```mzsql
    SELECT * FROM flip_activities LIMIT 10;
    ```

1. Use [`CREATE TABLE`](/sql/create-table) to create a `known_flippers` table
   that you can manually populate with known flippers. That is, assume that
   separate from your auction activities data, you receive independent data
   specifying users as flippers.

   ```mzsql
   CREATE TABLE known_flippers (flipper_id bigint);
   ```

1. Create a view `flippers` to flag known flipper accounts if a user has more
   than 2 flipping activities or the user is listed in the `known_flippers`
   table.

   ```mzsql
    CREATE VIEW flippers AS
    SELECT flipper_id
    FROM (
        SELECT flipper_id
        FROM flip_activities
        GROUP BY flipper_id
        HAVING count(*) >= 2

        UNION ALL

        SELECT flipper_id
        FROM known_flippers
    );
   ```

{{< note >}}

Both the `flip_activities` and `flippers` views can use the index created on
`winning_bids` view to provide up-to-date data. Depending upon your query
patterns and usage, an existing index may be sufficient, such as in this
quickstart. In other use cases, creating an index only on the view(s) from which
you will serve results may be preferred.

{{</ note >}}

## Step 6. Subscribe to see results change

[`SUBSCRIBE`](/sql/subscribe/) to `flippers` to see new flippers appear as new
data arrives (either from the known_flippers table or the flip_activities view).

1. Use [`SUBSCRIBE`](/sql/subscribe/) command to see flippers
   as new data arrives (either from the `known_flippers` table or the
   `flip_activities` view). [`SUBSCRIBE`](/sql/subscribe/) returns data from a
   source, table, view, or materialized view as they occur, in this case, the
   view `flippers`.

   ```mzsql
   SUBSCRIBE TO (
      SELECT *
      FROM flippers
   ) WITH (snapshot = false)
   ;
   ```

   The optional [`WITH (snapshot = false)`
   option](/sql/subscribe/#with-options) indicates that the command displays
   only the new flippers that come in after the start of the `SUBSCRIBE`
   operation, and not the flippers in the view at the start of the operation.

1. In the Materialize Console quickstart page, enter an id (for example `450`)
   into the text input field to insert a new user into the `known-flippers`
   table. You can specify any number for the flipper id.

   The flipper should immediately appear in the `SUBSCRIBE` results.

   You should also see flippers who are flagged by their flip activities.
   Because of the randomness of the auction data being generated, user
   activity data that match the definition of a flipper may take some time
   even though auction data is constantly being ingested. However, once
   new matching data comes in, you will see it immediately in the `SUBSCRIBE`
   results. While waiting, you can enter additional flippers into the
   `known_flippers` table.

1. To cancel out of the `SUBSCRIBE`, click the **Stop streaming** button.

## Step 7. Create views to verify that Materialize returns consistent data

To verify that Materialize serves **consistent results**, even as new
data comes in, this step creates the following views for completed auctions:

- A view to keep track of each seller's credits.

- A view to keep track of each buyer's debits.

- A view that sums all sellers' credits, all buyers' debits, and calculates the
  difference, which should be `0`.

1. Create a view to track credited amounts for sellers of completed auctions.

    ```mzsql
    CREATE VIEW seller_credits AS
    SELECT seller, SUM(amount) as credits
    FROM winning_bids
    GROUP BY seller;
    ```

1. Create a view to track  debited amounts for the winning bidders of completed
   auctions.

    ```mzsql
    CREATE VIEW buyer_debits AS
    SELECT buyer, SUM(amount) as debits
    FROM winning_bids
    GROUP BY buyer;
    ```

1. To verify that the total credit and total debit amounts equal for completed
   auctions (i.e., to verify that the results are correct and consistent even as
   new data comes in), create a `funds_movement` view that calculates the total
   credits across sellers, total debits across buyers, and the difference
   between the two.

    ```mzsql
    CREATE VIEW funds_movement AS
    SELECT SUM(credits) AS total_credits,
           SUM(debits) AS total_debits,
           SUM(credits) - SUM(debits) AS total_difference
    FROM (
      SELECT SUM(credits) AS credits, 0 AS debits
      FROM seller_credits

      UNION

      SELECT 0 AS credits, SUM(debits) AS debits
      FROM buyer_debits
    );

    ```

   To see that the sums always equal even as new data comes in, you can
   `SUBSCRIBE` to this query:

   ```mzsql
   SUBSCRIBE TO (
      SELECT *
      FROM funds_movement
   );
   ```

   Toggle `Show diffs` to see changes to `funds_movement`.

   - As new data comes in and auctions complete, the `total_credits` and
     `total_debits` values should change but the `total_difference` should
     remain `0`.
   - To cancel out of the `SUBSCRIBE`, click the **Stop streaming** button.

## Step 8. Clean up

To clean up the quickstart environment:

1. Use the [`DROP SOURCE ... CASCADE`](/sql/drop-source/) command to drop
   `auction_house` source and its dependent objects, including views and indexes
   created on the `auction_house` subsources.

   ```mzsql
   DROP SOURCE auction_house CASCADE;
   ```

1. Use the [`DROP TABLE`](/sql/drop-table) command to drop the separate
   `known_flippers` table.

   ```mzsql
   DROP TABLE known_flippers;
   ```

## Summary

In Materialize, [indexes](/concepts/indexes/) represent query results stored in
memory within a cluster. When you create an index on a view, the index
incrementally updates the view results (instead of recalculating the results
from scratch) as Materialize ingests new data. These up-to-date results are then
immediately available and computationally free for reads within the cluster.

### General guidelines

This quickstart created an index on a view to maintain in-memory up-to-date
results in the cluster. In Materialize, both materialized views and indexes on
views incrementally update the view results. Materialized views persist the
query results in durable storage and is available across clusters while indexes
maintain the view results in memory within a single cluster.

{{% views-indexes/table-usage-pattern %}}

The quickstart used an index since:

- The examples did not need to store the results in durable storage.

- All activities were limited to the single `quickstart` cluster.

- Although used, `SUBSCRIBE` operations were for illustrative/validation
  purposes and were not the final consumer of the views.

### Best practices

Before creating an index (which represents query results stored in memory),
consider its memory usage as well as its [compute cost
implications](/administration/billing/#compute). For best practices when
creating indexes, see [Index Best Practices](/concepts/indexes/#best-practices).

### Additional information

- [Clusters](/concepts/clusters)
- [Indexes](/concepts/indexes)
- [Sources](/concepts/sources)
- [Views](/concepts/views/)
- [Idiomatic Materialize SQL
  chart](/transform-data/idiomatic-materialize-sql/appendix/idiomatic-sql-chart/)
- [Usage & Billing](/administration/billing/#compute)
- [`CREATE INDEX`](/sql/create-index/)
- [`CREATE SCHEMA`](/sql/create-schema/)
- [`CREATE SOURCE`](/sql/create-source/)
- [`CREATE TABLE`](/sql/create-table)
- [`CREATE VIEW`](/sql/create-view/)
- [`DROP VIEW`](/sql/drop-view)
- [`DROP SOURCE`](/sql/drop-source/)
- [`DROP TABLE`](/sql/drop-table)
- [`SELECT`](/sql/select)
- [`SUBSCRIBE`](/sql/subscribe/)

## Next steps

[//]: # "TODO(morsapaes) Extend to suggest third party tools. dbt, Census and Metabase could all fit here to do interesting things as a follow-up."

To get started ingesting your own data from an external system like Kafka, MySQL
or PostgreSQL, check the documentation for [sources](/sql/create-source/), and
navigate to **Data** > **Sources** > **New source** in the [Materialize Console](/console/)
to create your first source.

For help getting started with your data or other questions about Materialize,
you can schedule a [free guided
trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).
