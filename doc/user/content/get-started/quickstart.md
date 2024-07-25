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
Materialize is the Cloud Operational Data Store that delivers the speed of
streaming with the ease of a data warehouse. With Materialize, organizations can
use SQL to transform, deliver, and act on fast-changing data. Materialize provides up-to-date
results while also providing strong consistency guarantees, including strict
serializability (the default level). Materialize maintains these up-to-date
results via incrementally maintained [indexes](/concepts/indexes/) and
incrementally maintained [materialized
views](/concepts/views/#materialized-views).

In this quickstart, you will:

- Create various [views](/concepts/views/) on Materialize-provided auction
  example data;

- Create an [index](/concepts/indexes) to <red>maintain and incrementally update
  view results in memory</red> as new data arrives.

- Create views to verify that Materialize serves <red>consistent results</red>.

{{< note >}}

For the purpose of this quickstart, the results do not need to be persisted in
durable storage. As such, the quickstart uses an index on a view to maintain
up-to-date view results.

{{</ note >}}

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

- If you are using the Materialize Emulator, connect to the Materialize Emulator
  using your preferred SQL client.

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
generator](/sql/create-source/load-generator/#auction) to create the
[sources](/concepts/sources/).

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

    `CREATE SOURCE` creates **multiple** sources when ingesting data from
    multiple upstream tables. For each upstream table that is selected for
    ingestion, Materialize creates a subsource.

1. Use the [`SHOW SOURCES`](/sql/show-sources/) command to see the results of
   the previous step.

    ```mzsql
    SHOW SOURCES;
    ```

    The output should resemble the following:

    ```nofmt
<<<<<<< HEAD
            name            |      type
    ------------------------+-----------------
     accounts               | subsource
     auction_house          | load-generator
     auction_house_progress | progress
     auctions               | subsource
     bids                   | subsource
     organizations          | subsource
     users                  | subsource
=======
    | name                   | type           | size | cluster    |
    | ---------------------- | -------------- | ---- | ---------- |
    | accounts               | subsource      | 25cc | quickstart |
    | auction_house          | load-generator | 25cc | quickstart |
    | auction_house_progress | progress       | null | null       |
    | auctions               | subsource      | 25cc | quickstart |
    | bids                   | subsource      | 25cc | quickstart |
    | organizations          | subsource      | 25cc | quickstart |
    | users                  | subsource      | 25cc | quickstart |
>>>>>>> 06aae46a2a (Update quickstart content)
    ```

    A [`subsource`](/sql/show-subsources) is a relation(table).

    - A subsource can only be written by the source; in this case, the
      load-generator.

    - Users can read from subsources.

1. Use the [`SELECT`](/sql/select) statement to query the subsources. The
   quickstart uses the `auctions` and `bids` subsources.

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

      As new data enters the system, above `SELECT` queries could be rerun to
      get the recent results.

      To efficiently serve queries that have no supporting indexes, Materialize
      uses the same underlying mechanism it does for indexes. However, without
      an index, the underlying work is discarded after the results are served.
      By creating a view to save repeated queries and then <redb>indexing</redb>
      the view, Materialize can <redb>maintain and incrementally update</redb>
      view results in memory.

      Using a query to find winning bids for auctions, subsequent steps in this
      quickstart show how Materialize uses views and indexes to provide
      up-to-date results.

## Step 3. Create a view to find winning bids

A view is a saved name for the underlying `SELECT` statement, providing an
alias/shorthand when referencing the query. The underlying query is not executed
during the view creation; instead, the underlying query is executed when the
view is referenced.

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

   Each time you query the view, you are running the underlying statement. Even
   without an index, Materialize uses the same mechanics used by indexes to
   optimize computations. However, without an index, the underlying work is
   discarded after each query run.

   In Materialize, you can create [**indexes**](/concepts/indexes/) on views to
   keep view results incrementally updated and immediately accessible in memory.

   By indexing the `winning_bids` view, Materialize can:

   - Maintain and incrementally update view results in memory; and

   - Further [optimize query performance](/transform-data/optimization/) by
     supporting point lookups on the indexed field.

## Step 4. Create an index on the view

In Materialize, you can create [indexes](/concepts/indexes/) on views. In
Materialize, indexes maintain and, as data changes (inserts/updates/deletes),
incrementally update view results in memory. That is, the incremental work is
performed on writes such that reads from indexes are computationally
<redb>free</redb>. In addition, indexes can also help [optimize
operations](/transform-data/optimization/) like point lookups and joins.

1. Use the [`CREATE INDEX`](/sql/create-index/) command to create the following
   index on the `winning_bids` view.

    ```mzsql
    CREATE INDEX wins_by_item ON winning_bids (item);
    ```

   The index **maintains and incrementally updates** the results of
   `winning_bids` in memory. Because incremental work is performed on writes,
   reads from indexes are computationally <redb>free</redb>.

   This index can **also**  help [optimize
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

   The queries should be faster as the reads are served from memory.

1. To verify that an index is used (i.e., the results are served from memory),
   you can run explain on the queries:

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

{{< tip >}}

- Before creating an index, consider its memory usage as well as its compute
  cost implications.

- In Materialize, underlying data structures that are being maintained for an
  index (or a materialized view) can be reused by other views/queries. As
  such, an existing index may be sufficient to support your queries.

- If you have created various views to act as the building blocks for a view
  from which you will serve the results, creating an index only on the serving
  view may be sufficient depending upon your query patterns and usage.

- Even without indexes, Materialize can optimize computations for queries.
  However, this underlying work is discarded after each query run.

{{</ tip >}}

## Step 5. Create views to find auction flippers in real time.

In Materialize, underlying data structures that are being maintained for an
index (or a materialized view) can be reused by other views/queries. This step
creates two views:

- One view to find auction flipping activities.

- Another view to find auction flippers based on flipping activities.

The two views can use the index on `winning_bids`.

1. Create a view to detect auction flipping activities. For the purposes of this
   quickstart (where data is emitted every 1 second), auction flipping
   activities are those activities where the winning bidder of an item in one
   auction is the seller of the same item at a higher price in another auction
   within an 8-day period.

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

    To view a sample row in `flip_activities`, run the following
    [`SELECT`](/sql/select) command:

    ```mzsql
    SELECT * FROM flip_activities LIMIT 10;
    ```

    *Optional exercise*. To see that this view can use the index created
    earlier, run an `EXPLAIN` on the query.

    ```mzsql
    EXPLAIN
    SELECT * FROM flip_activities LIMIT 10;
    ```

    Depending upon your query patterns and usage, an existing index may be
    sufficient, such as in this quickstart. In other use cases, creating an
    index only on the view(s) from which you will serve results may be
    preferred.

1. Create a view `flippers` to flag known flipper accounts. Flippers are those
   users with more than 2 flipping activities.

    ```mzsql
    CREATE VIEW flippers AS
    SELECT flipper_id
    FROM flip_activities
    GROUP BY flipper_id
    HAVING count(*) >= 2;
    ```

    To view a sample row, run the following query on `flippers`, run the
    following [`SELECT`](/sql/select) command:

    ```mzsql
    SELECT *
    FROM flippers
    LIMIT 10;
    ```

    As new data comes in for users matching the definition of a flipper,
    Materialize keeps the `flippers` view up to date.

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

1. Use the [`SUBSCRIBE`](/sql/subscribe/) command to see new flippers as new
   data comes. [`SUBSCRIBE`](/sql/subscribe/) returns data from a source, table,
   view, or materialized view as they occur, in this case, the view `flippers`.
   The optional [`WITH (snapshot = false)` option](/sql/subscribe/#with-options)
   indicates that the command displays only the new flippers that come in after
   the start of the `SUBSCRIBE` operation, and not the flippers in the view at
   the start of the operation.

   {{< tabs >}}
   {{< tab "Materialize Console" >}}
   ```mzsql
   SUBSCRIBE TO (
     SELECT *
     FROM flippers
   ) WITH (snapshot = false)
   ;
   ```

   Because of the randomness of the data being generated, it may take some time
   (~up to <red>1 minute</red>) for a new data to come in for users matching the
   definition of a flipper. However, once new matching data comes in, you can
   see it immediately.

   To cancel out of the `SUBSCRIBE`, click **Stop streaming**.

   *Optional exercise*. You can issue the `SUBSCRIBE` statement without the
   `WITH (snapshot = false)` to display both the flippers that exist at the
   start of the command as well as display new flippers as they come in. If the
   results are paginated, you may need to go to the end to see the new flippers.

## Step 6. Include a table for known flippers

Currently, flippers are determined by the auction activities data being
auto-generated. Assume that, separate from your auction data, you receive
independent notifice that certain users are flippers. Instead of waiting for
their auction data to flag them as flippers, you want to immediately store this
data and incorporate the table in flagging flippers.

1. Use [`CREATE TABLE`](/sql/create-table) to create a `known_flippers` table
   that can be manually populated with known flippers.

   ```mzsql
   CREATE TABLE known_flippers (flipper_id bigint);
   ```

1. Drop the existing `flippers` view and recreate using both the
   `flipper_activities` and the new `known_flippers` table.

   1. Use the [`DROP VIEW`](/sql/drop-view) command to drop the existing
      `flippers` view.

      ```mzsql
       DROP VIEW flippers;
      ```

   1. Recreate the view to include the `known_flippers` table:

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

   1. Subscribe to `flippers`.

      ```mzsql
      SUBSCRIBE TO (
        SELECT *
        FROM flippers
      ) WITH (snapshot = false)
      ;
      ```

1. In another [Materialize console](https://console.materialize.com/), insert
   new data into the `known_flippers` table .

   1. Open a new browser window and navigate to the [Materialize
      console](https://console.materialize.com/).

   1. Select your schema.

      ```mzsql
      -- Replace <schema> with the name for your schema
      SET SCHEMA <schema>;
      ```

   1. In the SQL Shell, use the [INSERT](/sql/insert/) command to insert new
      data into the `known_flippers` table.

      ```mzsql
      INSERT INTO known_flippers VALUES (450);
      ```

      In the other Console window,

      - The inserted value should display in the `SUBSCRIBE` results.

      - Cancel out of the `SUBSCRIBE` by clicking **Stop streaming**.

    1. Close one of the Console windows.

## Step 7. Create views to verify that Materialize returns consistent data.

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

    To see that the sums always equal even as new data comes in, you can `SUBSCRIBE` to this query:

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

## Step 8. Optional exercises

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

1. In general, you may find that you rarely need to index a source. However, if
   for some use case, you need to index a source, you can also create indexes on
   sources, tables, and materialized views.

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

       However, the above exercise is to illustrate how to create an index on
       sources. In practice, you may find that you rarely need to index a
       source.


## Step 9. Clean up

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

In Materialize, [indexes on a view](/concepts/indexes) (and [materialized
views](/concepts/views#materialized-views)) maintain and incrementally update
view results in memory. This quickstart used indexes instead of [materialized
views](/concepts/views#materialized-views) because the results did not need to
persist to a durable storage.

Before creating an index, consider:

- Its memory usage as well as its compute cost implications.

- In Materialize, underlying data structures that are being maintained for an
  index (or a materialized view) can be reused by other views/queries. As
  such, an existing index may be sufficient to support your queries.

- If you have created various views to act as the building blocks for a view
  from which you will serve the results, creating an index only on the serving
  view may be sufficient depending upon your query patterns and usage.

- Even without indexes, Materialize can optimize computations for queries.
  However, this underlying work is discarded after each query run.

### Additional information

- [Views](/concepts/views/)
- [Indexes](/concepts/indexes)
- [Sources](/concepts/sources)
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

## What's next?

[//]: # "TODO(morsapaes) Extend to suggest third party tools. dbt, Census and Metabase could all fit here to do interesting things as a follow-up."

To get started ingesting your own data from an external system like Kafka, MySQL
or PostgreSQL, check the documentation for [sources](/sql/create-source/), and
navigate to **Data** > **Sources** > **New source** in the [Materialize Console](https://console.materialize.com/)
to create your first source.

For help getting started with your data or other questions about Materialize,
you can schedule a [free guided
trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).
