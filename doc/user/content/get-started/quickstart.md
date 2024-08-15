---
title: "Quickstart"
description: "Learn the basics of Materialize."
menu:
  main:
    parent: "get-started"
    weight: 15
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
objects return up-to-date results while being computationally <redb>free</redb>.

In this quickstart, you will:

- Create and query various [views](/concepts/views/) on sample auction data. The
  data is continually generated at 1 second intervals to mimic a data-intensive
  workload.

- Create an [index](/concepts/indexes "Indexes represents query results stored
  in memory within a cluster") to compute and store view results in memory. As
  new auction data arrives, the index <redb>incrementally updates</redb> view
  results instead of recalculating the results from scratch, making fresh
  results immediately available for reads.

- Create and query views to verify that Materialize always serves
  <redb>consistent results</redb>.

The quickstart uses the default `quickstart` cluster. In Materialize, a
[cluster](/concepts/clusters/) is an isolated pool of compute resources.

![Image of Quickstart in the Materialize Console](/images/quickstart-console.png "Quickstart in the Materialize Console")

## Prerequisite

A Materialize account. If you do not have an account, you can [sign up for a
free
trial](https://materialize.com/register/?utm_campaign=General&utm_source=documentation).

Alternatively, you can [download the Materialize
Emulator](/get-started/install-materialize-emulator/) to test locally. However,
the Materialize Emulator does not provide the full experience of using
Materialize.

## Step 0. Open the SQL Shell or a SQL client.

- If you have a Materialize account, navigate to the [Materialize
  Console](https://console.materialize.com/) and sign in. By default, you should
  be in the SQL Shell.  If you're already signed in, you can access the SQL Shell in the left-hand menu.

- If you are using the Materialize Emulator, [connect to the Materialize
  Emulator](/get-started/install-materialize-emulator/#materialize-emulator-connect-client)
  using your preferred SQL client.

A cluster is a pool of compute resources (CPU, memory, and scratch disk space)
for running your workloads.

## Step 1. Create a schema

By default, you are working in the `materialize.public`
[namespace](/sql/namespaces/), where:

- `materialize` is the database name, and

- `public` is the schema name.

Create a separate schema for this quickstart.

1. Use [`CREATE SCHEMA`](/sql/create-schema/) to create your own schema.

   ```mzsql
   -- Replace <schema> with the name for your schema
   CREATE SCHEMA materialize.<schema>;
   ```

1. Select your schema.

   ```mzsql
   -- Replace <schema> with the name for your schema
   SET SCHEMA <schema>;
   ```

## Step 2. Create sources

[Sources](/concepts/sources/) are external systems from which Materialize reads
in data. This tutorial uses Materialize's [sample `Auction` load
generator](/sql/create-source/load-generator/#auction) to create the sources.

1. Create the [sources](/concepts/sources "External systems from which
   Materialize reads data.") using the [`CREATE SOURCE`](/sql/create-source/)
   command.

   For the [sample `Auction` load
   generator](/sql/create-source/load-generator/#auction), use [`CREATE
   SOURCE`](/sql/create-source/) with the `FROM LOAD GENERATOR` clause that
   works specifically with Materialize's sample data generators. The tutorial
   specifies that the generator should emit new data every 1s.

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

1. Use the [`SELECT`](/sql/select) statement to query the subsources/tables. The
   quickstart uses the `auctions` and `bids` tables.

    * To view a sample row in `auctions`, run the following
      [`SELECT`](/sql/select) command:

      ```mzsql
      SELECT * FROM auctions ORDER BY end_time DESC LIMIT 1;
      ```

      The output should return a single row (your results may differ):

      ```nofmt
       id    | seller | item               | end_time
      -------+--------+--------------------+---------------------------
       29550 | 2468   | Best Pizza in Town | 2024-07-25 18:24:25.805+00
      ```

    * To view a sample row in `bids`, run the following [`SELECT`](/sql/select)
      command:

      ```mzsql
      SELECT * FROM bids ORDER BY bid_time DESC LIMIT 1;
      ```

      The output should return a single row (your results may differ):

      ```nofmt
       id     | buyer | auction_id | amount | bid_time
      --------+-------+------------+--------+---------------------------
       295641 | 737   | 29564      | 72     | 2024-07-25 18:25:42.911+00
      ```


    * To view the relationship between `auctions` and `bids`, you can join by
      the auction id. The following query returns the most recently ended
      auctions and associated bids (limited to 3 rows):

      ```mzsql
      SELECT a.*, b.*
      FROM auctions AS a
      JOIN bids AS b
        ON a.id = b.auction_id
      ORDER BY a.end_time desc
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

      As new data enters the system, above `SELECT` queries must be rerun to get
      the recent results.

      By creating a view to save repeated queries and then <redb>indexing</redb>
      the view, Materialize can <redb>compute and then, as new data is ingested,
      incrementally update</redb> view results in memory.

      Using a query to find winning bids for auctions, subsequent steps in this
      quickstart show how Materialize uses views and indexes to provide
      immediately available up-to-date results for various queries.

## Step 3. Create a view to find winning bids.

A [view](/concepts/views/) is a saved name for the underlying `SELECT`
statement, providing an alias/shorthand when referencing the query. The
underlying query is not executed during the view creation; instead, the
underlying query is executed when the view is referenced.

Assume you want to find the winning bids for auctions that have ended. The
winning bid for an auction is the highest bid entered for an auction before the
auction ended. As new auction and bid data appears, the query must be rerun to
get up-to-date results.

1. Using the [`CREATE VIEW`](/sql/create-view/) command, create a
   [**view**](/concepts/views/ "Saved name/alias for a query") `winning_bids`
   for the query that finds winning bids for auctions that have ended.

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

   [`SELECT DISTINCT ON
   (...)`](https://www.postgresql.org/docs/current/sql-select.html#SQL-DISTINCT)
   is a PostgreSQL expression that keeps only the first row of each set of
   matching rows.

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
   the up-to-date results. Each time you query the view, you are running the
   underlying statement.

   In Materialize, you can create [**indexes**](/concepts/indexes/) on views to
   keep view results incrementally updated in memory within a cluster. In the
   next step, you will create an index on `winning_bids`.

## Step 4. Create an index to provide immediately available up-to-date results.

In Materialize, you can create [indexes](/concepts/indexes/) on views to provide
always fresh view results in memory within a cluster. Instead of recalculating
the results from scratch, indexes <redb>perform incremental updates</redb> as
inputs change. In addition, indexes can also help [optimize
operations](/transform-data/optimization/) like point lookups and joins.

1. Use the [`CREATE INDEX`](/sql/create-index/) command to create the following
   index on the `winning_bids` view.

    ```mzsql
    CREATE INDEX wins_by_item ON winning_bids (item);
    ```

   During the index creation, the underlying `winning_bids` query is executed,
   and the view results are stored in memory within the cluster. As new data
   arrives, the index **incrementally updates** the view results in memory.
   Because incremental work is performed on writes, reads from indexes return
   up-to-date results and are computationally <redb>free</redb>.

   This index can **also** help [optimize
   operations](/transform-data/optimization/) like point lookups and delta joins
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

   The queries should be faster since they can use the in-memory, already
   up-to-date results computed by the index.

1. To verify that an index is used, you can run explain on the queries:

   - The following [`EXPLAIN`](/sql/explain/) results should show that the query
     uses the index.

     ```mzsql
     EXPLAIN
     SELECT * FROM winning_bids
     ORDER BY bid_time DESC
     LIMIT 10;
     ```

   - The following [`EXPLAIN`](/sql/explain/) results should show that the query
     uses the index and performs a [point lookup](/transform-data/optimization/)
     on the indexed column.

      ```mzsql
      EXPLAIN
      SELECT * FROM winning_bids
      WHERE item = 'Best Pizza in Town'
      ORDER BY bid_time DESC
      LIMIT 10;
      ```

## Step 5. Find auction flippers in real time.

For this quickstart, auction flipping activities are defined as when a user buys
an item in one auction and resells the same item at a higher price within an
8-day period. This step finds auction flippers in real time, based on
auction flipping activity data and known flippers data. Specifically, this step
creates:

- A view to find auction flipping activities. Results are updated as new data
  comes in (at 1 second intervals) from the data generator.

- A table that maintains known auction flippers. You will manually enter new
  data to this table.

- A view to immediately flag auction flippers based on both the flipping
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

    *Optional exercise*. To verify that the view uses the index, run an
    `EXPLAIN` on the query.

    ```mzsql
    EXPLAIN
    SELECT * FROM flip_activities LIMIT 10;
    ```

    Depending upon your query patterns and usage, an existing index may be
    sufficient, such as in this quickstart. In other use cases, creating an
    index only on the view(s) from which you will serve results may be
    preferred.

1. Use [`CREATE TABLE`](/sql/create-table) to create a `known_flippers` table
   that can be manually populated with known flippers.  That is, assume that
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

    The `flippers` view can use the index created on `winning_bids` view to
    provide up-to-date flipper data. To view a sample row, run the following
    query on `flippers`, run the following [`SELECT`](/sql/select) command:

    ```mzsql
    SELECT *
    FROM flippers
    LIMIT 10;
    ```

    *Optional exercise*. To see that this view can use the index created earlier, run an `EXPLAIN` on the query.

    ```mzsql
    EXPLAIN
    SELECT *
    FROM flippers
    LIMIT 10;
    ```

    Depending upon your query patterns and usage, an existing index may be
    sufficient. In other use cases, creating an index only on the view(s) from
    which you will serve results may be preferred.

1. Use the [`SUBSCRIBE`](/sql/subscribe/) command to see flippers
   as new data comes in (either from the `known_flippers` table or the
   `flip_activities` view). [`SUBSCRIBE`](/sql/subscribe/) returns data from a
   source, table, view, or materialized view as they occur, in this case, the
   view `flippers`. The optional [`WITH (snapshot = false)`
   option](/sql/subscribe/#with-options) indicates that the command displays
   only the new flippers that come in after the start of the `SUBSCRIBE`
   operation, and not the flippers in the view at the start of the operation.

   {{< tabs >}}
   {{< tab "Materialize Console" >}}
   ```mzsql
   SUBSCRIBE TO (
      SELECT *
      FROM flippers
   ) WITH (snapshot = false)
   ;
   ```

   1. In the Materialize Console quickstart page, enter an id (for example
      `450`) into the text input field to insert a new user into the
      `known-flippers` table.

      The user should immediately appear in the `SUBSCRIBE` results.

      You should also see flippers that are flagged by their multiple flipping
      activities. Because of the randomness of the auction data being generated,
      user activity data that match the definition of a flipper may take some
      time even though auction data is constantly being ingested. However, once
      new matching data comes in, you will see it immediately in the `SUBSCRIBE`
      results.

   1. To cancel out of the `SUBSCRIBE`, click **Stop streaming**.

   {{< /tab >}}
   {{< tab "Other Clients" >}}

   If running Materialize in a Docker container, run the following command in
   your preferred SQL client:
   ```mzsql
   COPY (SUBSCRIBE TO (
        SELECT *
        FROM flippers
   ) WITH (snapshot = false)) TO STDOUT;
   ```

   1. Open another SQL client and connect to your Materialize.

   1. Select your schema.

      ```mzsql
      -- Replace <schema> with the name for your schema
      SET SCHEMA <schema>;
      ```

   1. Manually insert a row into the `known_flippers` table:

      ```mzsql
      INSERT INTO known_flippers values (450);
      ```

      In the other client, the user should immediately appear in the `SUBSCRIBE`
      results.

      You should also see flippers that are flagged by their multiple flipping
      activities. Because of the randomness of the auction data being generated,
      user activity data that match the definition of a flipper may take some
      time even though auction data is constantly being ingested. However, once
      new matching data comes in, you will see it immediately in the `SUBSCRIBE`
      results.

   1. Cancel out of the `SUBSCRIBE`.

   {{< /tab >}}
   {{< /tabs >}}

   *Optional exercise*. You can issue the `SUBSCRIBE` statement without the
   `WITH (snapshot = false)` to display both the flippers that exist at the
   start of the command as well as display new flippers as they come in. If the
   results are paginated, you may need to go to the end to see the new flippers.

## Step 6. Create views to verify that Materialize returns consistent data.

To verify that Materialize serves <red>consistent results</red>, even as new
data comes in, this step creates the following views for completed auctions:

- A view to keep track of each seller's credits.

- A view to keep track of each buyer's debits.

- A view that sums all sellers' credits, all buyers' debits, and calculates the
  difference, which should be `0`.

1. Create a view to track of the financial information for the seller of
   completed auctions.

    ```mzsql
    CREATE VIEW seller_credits AS
    SELECT seller, SUM(amount) as credits
    FROM winning_bids
    GROUP BY seller;
    ```

    As new auction data is generated, the sellers' `credits` will
    update accordingly.

1. Create a view to track of the financial information for the winning bidder of
   completed auctions.

    ```mzsql
    CREATE VIEW buyer_debits AS
    SELECT buyer, SUM(amount) as debits
    FROM winning_bids
    GROUP BY buyer;
    ```

    As new auction data is generated, the buyers' `debits` will
    update accordingly.

1. To verify that the total credit and total debit amounts equal for closed
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

   To verify that the total credits equal the total debits, select from
   `funds_movement`:

   ```mzsql
   SELECT *
   FROM funds_movement
   ;
   ```

   To see that the sums always equal even as new data comes in, you can
   `SUBSCRIBE` to this query:

   {{< tabs >}}
   {{< tab "Materialize Console" >}}
   ```mzsql
   SUBSCRIBE TO (
      SELECT *
      FROM funds_movement
   );
   ```

   - As new data comes in and auctions complete, the `total_credits` and
     `total_debits` values should change but the `total_difference` should
     remain `0`.
   - To cancel out of the `SUBSCRIBE`, click **Stop streaming**.
   {{< /tab >}}
   {{< tab "Other Clients" >}}
   If running Materialize in a Docker container, run the following command in
   your preferred SQL client:
   ```mzsql
   COPY (SUBSCRIBE TO (
      SELECT *
      FROM funds_movement
   )) TO STDOUT;
   ```
   - As new data comes in and auctions complete, the `total_credits` and
    `total_debits` values should change but the `total_difference` should
    remain `0`.
   - You can cancel out of the `SUBSCRIBE`.
   {{< /tab >}}
   {{< /tabs >}}
    **Optional exercise*. Alternatively, you can use a common table expression
    (CTE) to implement the view.

    ```mzsql
    CREATE VIEW funds_movement_cte_view AS
    SELECT * FROM (WITH cr AS
      (
        SELECT SUM(credits) AS credits, 0 AS debits FROM seller_credits
        UNION
        SELECT 0, SUM(debits) FROM buyer_debits
      ),
      tot AS
      (
        SELECT SUM(credits) AS total_credits, SUM(debits) AS total_debits FROM cr
      )
      SELECT total_credits, total_debits, total_credits - total_debits AS
      total_difference
      FROM tot
    );

    ```

    You can then `SUBSCRIBE` to the changes in the `funds_movement_cte_view`
    view.


## Step 7. Additional exercises

1. The views in the quickstart used the `wins_by_item` index on the
   `winning_bids` view.

   Depending upon your query usage patterns, creating an index only on the
   view(s) from which you will serve results may be preferred. For example,
   assume you only serve queries from auction flippers and all the other views
   serve only as building blocks to the `flippers` views.

   In that case, you may want to

   1. [`DROP`](/sql/drop-index/) the `wins_by_item` index:

      ```mzsql
      DROP index wins_by_item;
      ```

   1. Create an index on `flippers`.

      ```mzsql
      CREATE index flippers_flipper_id ON flippers(flipper_id);
      ```

   1. Run a query on `flippers`.

      ```mzsql
      SELECT *
      FROM flippers
      WHERE flipper_id = 450;
      ```

1. In general, you may find that you rarely need to index a source.  However, if
   for example, you frequently serve results directly from a source, you can
   also create indexes on sources to maintain in-memory up-to-date source data
   within the cluster.
   
   1. For example, run the following queries on `bids` without an index:

      ```mzsql
       SELECT * FROM bids ORDER BY bid_time DESC LIMIT 10;
       SELECT * from bids WHERE amount > 50 LIMIT 10;
      ```

    1. Create an index on `bids`.

       ```mzsql
       CREATE INDEX bids_by_time ON bids (bid_time DESC);
       ```

    1. Run various queries on `bids`:

       ```mzsql
       SELECT * FROM bids ORDER BY bid_time DESC LIMIT 10;
       SELECT * from bids WHERE amount > 50 LIMIT 10;
       ```

       The queries should be faster.

       The above exercise is to illustrate how to create an index on
       sources. In practice, you may find that you rarely need to index a
       source.

## Step 8. Clean up

To clean up the quickstart environment:

1. Use the [`DROP SOURCE ... CASCADE`](/sql/drop-source/) command to drop
   `auction_house` source and its dependent objects, including views and indexes
   created on the `auction_house` subsources.

   ```mzsql
   DROP SOURCE auction_house CASCADE;
   ```

1. Use the [`DROP TABLE`](/sql/drop-table) command to drop the separate
   `known_flippers` table and its indexes.

   ```mzsql
   DROP TABLE known_flippers;
   ```

## Summary

In Materialize, [indexes](/concepts/indexes/) <red>incrementally
update</red> results when Materialize ingests new data. These up-to-date results
are then immediately available and computationally free for reads within the cluster.

### Use of indexed views

This quickstart created an index on a view to maintain in-memory up-to-date
results in the cluster. In Materialize, both indexes on views and materialized
views incrementally update the view results. Indexes maintain the view results
in memory within a cluster while the materialized views persist the query
results in durable storage and is available across clusters. 

{{% views-indexes/table-usage-pattern %}}

The quickstart used an index since:

- The examples did not need to store the results in durable storage.

- All activities were limited to the single `quickstart` cluster.

- Although used, `SUBSCRIBE` operations were for illustrative purposes and were
  not the final consumer of the views.

### Considerations

Before creating an index, consider its memory usage as well as its [compute cost
implications](/administration/billing/#compute). For best practices when
creating indexes, see [Index Best Practices](/concepts/indexes/#best-practices)

### Additional information

- [Clusters](/concepts/clusters)
- [Indexes](/concepts/indexes)
- [Sources](/concepts/sources)
- [Views](/concepts/views/)
- [`CREATE VIEW`](/sql/create-view/)
- [`CREATE INDEX`](/sql/create-index/)
- [`CREATE SCHEMA`](/sql/create-schema/)
- [`CREATE SOURCE`](/sql/create-source/)
- [`CREATE TABLE`](/sql/create-table)
- [`DROP INDEX`](/sql/drop-index/)
- [`DROP VIEW`](/sql/drop-view)
- [`DROP SOURCE`](/sql/drop-source/)
- [`DROP TABLE`](/sql/drop-table)
- [`EXPLAIN`](/sql/explain/)
- [`SELECT`](/sql/select)
- [`SUBSCRIBE`](/sql/subscribe/)

## Next steps

[//]: # "TODO(morsapaes) Extend to suggest third party tools. dbt, Census and Metabase could all fit here to do interesting things as a follow-up."

To get started ingesting your own data from an external system like Kafka, MySQL
or PostgreSQL, check the documentation for [sources](/sql/create-source/), and
navigate to **Data** > **Sources** > **New source** in the [Materialize Console](https://console.materialize.com/)
to create your first source.

For help getting started with your data or other questions about Materialize,
you can schedule a [free guided
trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).
