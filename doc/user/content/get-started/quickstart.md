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

The following quickstart provides a hands-on overview of some of the key
features of Materialize.

In this quickstart, you will:

- Create sources from streaming data. This quickstart uses a
  Materialize-provided sample data generator.

- Create a view to save a query on the sources.

- Index a view to <red>maintain and incrementally update view results in
  memory</red>.

- Create views to find auction flippers in real time.

- Create views to verify that Materialize serves <red>correct and consistent
  results</red>, even as new data arrives.

![Image of Quickstart in the Materialize Console](/images/quickstart-console.png "Quickstart in the Materialize Console")

## Prerequisite

A Materialize account. If you do not have an account, you can [sign up for a free trial](https://materialize.com/register/?utm_campaign=General&utm_source=documentation).

## Step 0. Sign in to Materialize

Navigate to the [Materialize Console](https://console.materialize.com/) and sign
in. By default, you should land in the SQL Shell. If you're already signed in,
you can access the SQL Shell in the left-hand menu.

{{< note >}}

This quickstart is available from within the [Materialize
Console](https://console.materialize.com/) itself.

{{</ note>}}

## Step 1. Create sources

[Sources](/concepts/sources/) are external systems from which Materialize reads
in data. This tutorial uses Materialize's [sample `Auction` load
generator](/sql/create-source/load-generator/#auction) to create the
[sources](/concepts/sources/).

1. Create the [sources](/concepts/sources "External systems from which
   Materialize reads data.") using the  [`CREATE SOURCE`](/sql/create-source/)
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
    ingestion, Materialize creates a subsource of type subsource.

1. Use the [`SHOW SOURCES`](/sql/show-sources/) command to see the results of
   the previous step.

    ```mzsql
    SHOW SOURCES;
    ```

    The output should resemble the following:

    ```nofmt
            name            |      type      | size
    ------------------------+----------------+------
     accounts               | subsource      | null
     auction_house          | load-generator | null
     auction_house_progress | progress       | null
     auctions               | subsource      | null
     bids                   | subsource      | null
     organizations          | subsource      | null
     users                  | subsource      | null
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
      SELECT auctions.*, bids.*
      FROM auctions, bids
      WHERE auctions.id = bids.auction_id
      ORDER BY auctions.end_time desc
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
      By creating a view to save repeated queries and then indexing the view,
      Materialize can <redb>maintain and incrementally update</redb> view
      results in memory.

      Using a query to find winning bids for auctions, subsequent steps in this
      quickstart show how Materialize uses views and indexes to provide up-to-date results.

## Step 2. Create a view to find winning bids

A view is a saved name for the underlying `SELECT` statement, providing an
alias/shorthand when referencing the query. The underlying query is not executed
during the view creation; instead, the underlying query is executed when the
view is referenced.

Assume you want to find the winning bids for auctions that have ended. The
winning bid for an auction is the highest bid entered for an auction before the
auction ended. As new auction and bid data appears, the query must be rerun to
get up-to-date results.

1. Using the [CREATE VIEW](/sql/create-view/) command, create a
   [**view**](/concepts/views/ "Saved name/alias for a query") `winning_bids`
   for the query that finds winning bids for auctions that have ended.

   ```mzsql
   CREATE VIEW winning_bids AS
   SELECT DISTINCT ON (auctions.id) bids.*, auctions.item, auctions.seller
   FROM auctions, bids
   WHERE auctions.id = bids.auction_id
     AND bids.bid_time < auctions.end_time
     AND mz_now() >= auctions.end_time
   ORDER BY auctions.id,
     bids.amount DESC,
     bids.bid_time,
     bids.buyer;
   ```

   [`SELECT DISTINCT ON
   (...)`](https://www.postgresql.org/docs/current/sql-select.html#SQL-DISTINCT)
   is a PostgreSQL expression that keeps only the first row of each set of
   matching rows.

1. Query the view to execute the underlying query. For example:

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

   By indexing the `winning_bids`, Materialize can:

   - Maintain and incrementally update view results in memory; and

   - Further [optimize query performance](/transform-data/optimization/)by
     supporting point lookups on the indexed field.

## Step 3. Create indexes on the view

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
     uses the index and perform a point lookup on the indexed column.

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

- In Materialize, underlying data structures that is being maintained for an
  index (or a materialized view) can be reused by other views/queries. As
  such, an existing index may be sufficient to support your queries.

- If you have created various views to act as the building blocks for a view
  from which you will serve the results, indexing the serving view only may be
  sufficient.

- Even without indexes, Materialize can optimize computations for queries.
  However, this underlying work is discarded after each query run.

{{</ tip >}}

## Step 4. Create views to find auction flippers in real time.

This step creates two views from other views:

- One view to find auction flipping activities.

- Another view to find auction flippers based on flipping activities.

1. Create a view to detect auction flipping activities. For the purposes of this
   quickstart (where data is emitted every 1 second), auction flipping
   activities are those activities where the winning bidder of an item in one
   auction is the seller of the same item at a higher price in another auction
   within a 30 minute period.

    ```mzsql
    CREATE VIEW flip_activities AS
    SELECT w2.seller,
           w2.item AS item,
           w2.amount AS sold_amount,
           w1.amount AS purchased_amount,
           w2.amount - w1.amount AS diff_amount,
           datediff('minutes', w2.bid_time, w1.bid_time) AS timeframe_minutes
    FROM winning_bids w1,
          winning_bids w2
    WHERE w1.buyer = w2.seller     -- Buyer and seller are the same
      AND w1.item = w2.item        -- The item is the same
      AND w2.amount > w1.amount    -- And sold at a higher price
      AND datediff('minutes', w2.bid_time, w1.bid_time) < 30;
    ```

    To view a sample row in `flip_activities`, run the following
    [`SELECT`](/sql/select) command:

    ```mzsql
    SELECT * FROM flip_activities LIMIT 10;
    ```

1. Create a view `flippers` to flag known flipper accounts. Flippers are those
   users with more than 2 flipping activities.

    ```mzsql
    CREATE VIEW flippers AS
    SELECT seller, count(*) AS flip_count
    FROM flip_activities
    GROUP BY seller
    HAVING count(*) >= 2;
    ```

1. In the `SQL Shell`, use the [`SUBSCRIBE`](/sql/subscribe/) command to see
   flippers as new data come in.

   ```mzsql
   SUBSCRIBE TO (
     SELECT *
     FROM flippers
   ) with (snapshot = false);
   ```

   To cancel out of the `SUBSCRIBE`, click **Stop streaming**.

## Step 5. Create a view to verify that reads return correct and consistent data.

1. Create a view to track the sales and purchases of each auction house user.

    ```mzsql
    CREATE VIEW funds_movement AS
    SELECT id, SUM(credits) as credits, SUM(debits) as debits
    FROM (
      SELECT seller as id, amount as credits, 0 as debits
      FROM winning_bids
      UNION ALL
      SELECT buyer as id, 0 as credits, amount as debits
      FROM winning_bids
    )
    GROUP BY id;
    ```

    If you `SELECT` from this view, you'll get many results, and results that
    are changing as new data is generated. This makes it hard to eyeball
    whether user funds _really_ add up, in the first place.

1. To double check, you can write a diagnostic query that makes it easier to
spot that results are correct and consistent. As an example, the total credit
and total debit amounts should always add up.

    ```mzsql
    SELECT SUM(credits), SUM(debits) FROM funds_movement;
    ```

    You can also `SUBSCRIBE` to this query, and watch the sums change in lock step as auctions close.

    ```mzsql
    SUBSCRIBE TO (
        SELECT SUM(credits), SUM(debits) FROM funds_movement
    );
    ```

   It is never wrong, is it?

## Step 6. Clean up

Use the `DROP SOURCE ... CASCADE` command to clean up your environment:

```mzsql
DROP SOURCE auction_house CASCADE;
```

## Summary


## What's next?

[//]: # "TODO(morsapaes) Extend to suggest third party tools. dbt, Census and Metabase could all fit here to do interesting things as a follow-up."

To get started ingesting your own data from an external system like Kafka, MySQL
or PostgreSQL, check the documentation for [sources](/sql/create-source/), and
navigate to **Data** > **Sources** > **New source** in the [Materialize Console](https://console.materialize.com/)
to create your first source.

For help getting started with your data or other questions about Materialize,
you can schedule a [free guided
trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).
