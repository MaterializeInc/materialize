<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)  /  [What is
Materialize?](/docs/self-managed/v25.2/get-started/)

</div>

# Quickstart

Materialize provides always-fresh results while also providing [strong
consistency
guarantees](/docs/self-managed/v25.2/get-started/isolation-level/). In
Materialize, both
[indexes](/docs/self-managed/v25.2/concepts/indexes/ "Indexes represents query results stored in memory
within a cluster") and [materialized
views](/docs/self-managed/v25.2/concepts/views/#materialized-views)
**incrementally update** results when Materialize ingests new data;
i.e., work is performed on writes. Because work is performed on writes,
reads from these objects return up-to-date results while being
computationally **free**.

In this quickstart, you will continuously ingest a sample auction data
set to build an operational use case around finding auction winners and
auction flippers. Specifically, you will:

- Create and query various
  [views](/docs/self-managed/v25.2/concepts/views/) on sample auction
  data. The data is continually generated at 1 second intervals to mimic
  a data-intensive workload.

- Create an
  [index](/docs/self-managed/v25.2/concepts/indexes "Indexes represents query results stored
  in memory within a cluster") to compute and store view results in
  memory. As new auction data arrives, the index **incrementally
  updates** view results instead of recalculating the results from
  scratch, making fresh results immediately available for reads.

- Create and query views to verify that Materialize always serves
  **consistent results**.

![Image of Quickstart in the Materialize
Console](/docs/self-managed/v25.2/images/quickstart-console.png "Quickstart in the Materialize Console")

## Step 0. Open the SQL Shell

- If you have self-managed Materialize, open the Materialize Console in
  your browser at <http://localhost:8080>. By default, you should be in
  the SQL Shell. If you’re already signed in, you can access the SQL
  Shell in the left-hand menu.

- If you are using the Materialize Emulator, open the Materialize
  Console in your browser at <http://localhost:6874>.

## Step 1. Create a schema

By default, you are using the `quickstart` cluster, working in the
`materialize.public`
[namespace](/docs/self-managed/v25.2/sql/namespaces/), where:

- A [cluster](/docs/self-managed/v25.2/concepts/clusters/) is an
  isolated pool of compute resources (CPU, memory, and scratch disk
  space) for running your workloads),

- `materialize` is the database name, and

- `public` is the schema name.

Create a separate schema for this quickstart. For a schema name to be
valid:

- The first character must be either: an ASCII letter (`a-z` and `A-Z`),
  an underscore (`_`), or a non-ASCII character.

- The remaining characters can be: an ASCII letter (`a-z` and `A-Z`),
  ASCII numbers (`0-9`), an underscore (`_`), dollar signs (`$`), or a
  non-ASCII character.

Alternatively, by double-quoting the name, you can bypass the
aforementioned constraints with the following exception: schema names,
whether double-quoted or not, cannot contain the dot (`.`).

See also [Naming
restrictions](/docs/self-managed/v25.2/sql/identifiers/#naming-restrictions).

1.  Enter a schema name in the text field and click the `Create` button.

2.  Switch to the new schema. From the top of the SQL Shell, select your
    schema from the namespace dropdown.

## Step 2. Create the source

[Sources](/docs/self-managed/v25.2/concepts/sources/) are external
systems from which Materialize reads in data. This tutorial uses
Materialize’s [sample `Auction` load
generator](/docs/self-managed/v25.2/sql/create-source/load-generator/#auction)
to create the source.

1.  Create the
    [source](/docs/self-managed/v25.2/concepts/sources "External systems from which
    Materialize reads data.") using the
    [`CREATE SOURCE`](/docs/self-managed/v25.2/sql/create-source/)
    command.

    For the [sample `Auction` load
    generator](/docs/self-managed/v25.2/sql/create-source/load-generator/#auction),
    the quickstart uses
    [`CREATE SOURCE`](/docs/self-managed/v25.2/sql/create-source/) with
    the `FROM LOAD GENERATOR` clause that works specifically with
    Materialize’s sample data generators. The tutorial specifies that
    the generator should emit new data every 1s.

    <div class="highlight">

    ``` chroma
    CREATE SOURCE auction_house
    FROM LOAD GENERATOR AUCTION
    (TICK INTERVAL '1s', AS OF 100000)
    FOR ALL TABLES;
    ```

    </div>

    `CREATE SOURCE` can create **multiple** tables (referred to as
    `subsources` in Materialize) when ingesting data from multiple
    upstream tables. For each upstream table that is selected for
    ingestion, Materialize creates a subsource.

2.  Use the [`SHOW SOURCES`](/docs/self-managed/v25.2/sql/show-sources/)
    command to see the results of the previous step.

    <div class="highlight">

    ``` chroma
    SHOW SOURCES;
    ```

    </div>

    The output should resemble the following:

    ```
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

    A [`subsource`](/docs/self-managed/v25.2/sql/show-subsources) is how
    Materialize refers to a table that has the following properties:

    - A subsource can only be written by the source; in this case, the
      load-generator.

    - Users can read from subsources.

3.  Use the [`SELECT`](/docs/self-managed/v25.2/sql/select) statement to
    query `auctions` and `bids`.

    - View a sample row in `auctions`:

      <div class="highlight">

      ``` chroma
      SELECT * FROM auctions LIMIT 1;
      ```

      </div>

      The output should return a single row (your results may differ):

      ```
       id    | seller | item               | end_time
      -------+--------+--------------------+---------------------------
       29550 | 2468   | Best Pizza in Town | 2024-07-25 18:24:25.805+00
      ```

    - View a sample row in `bids`:

      <div class="highlight">

      ``` chroma
      SELECT * FROM bids LIMIT 1;
      ```

      </div>

      The output should return a single row (your results may differ):

      ```
       id     | buyer | auction_id | amount | bid_time
      --------+-------+------------+--------+---------------------------
       295641 | 737   | 29564      | 72     | 2024-07-25 18:25:42.911+00
      ```

    - To view the relationship between `auctions` and `bids`, you can
      join by the auction id:

      <div class="highlight">

      ``` chroma
      SELECT a.*, b.*
      FROM auctions AS a
      JOIN bids AS b
        ON a.id = b.auction_id
      LIMIT 3;
      ```

      </div>

      The output should return (at most) 3 rows (your results may
      differ):

      ```
      | id    | seller | item               | end_time                   | id     | buyer | auction_id | amount | bid_time                   |
      | ----- | ------ | ------------------ | -------------------------- | ------ | ----- | ---------- | ------ | -------------------------- |
      | 15575 | 158    | Signed Memorabilia | 2024-07-25 20:30:25.085+00 | 155751 | 215   | 15575      | 27     | 2024-07-25 20:30:16.085+00 |
      | 15575 | 158    | Signed Memorabilia | 2024-07-25 20:30:25.085+00 | 155750 | 871   | 15575      | 63     | 2024-07-25 20:30:15.085+00 |
      | 15575 | 158    | Signed Memorabilia | 2024-07-25 20:30:25.085+00 | 155752 | 2608  | 15575      | 16     | 2024-07-25 20:30:17.085+00 |
      ```

      Subsequent steps in this quickstart uses a query to find winning
      bids for auctions to show how Materialize uses views and indexes
      to provide immediately available up-to-date results for various
      queries.

## Step 3. Create a view to find winning bids

A [view](/docs/self-managed/v25.2/concepts/views/) is a saved name for
the underlying `SELECT` statement, providing an alias/shorthand when
referencing the query. The underlying query is not executed during the
view creation; instead, the underlying query is executed when the view
is referenced.

Assume you want to find the winning bids for auctions that have ended.
The winning bid for an auction is the highest bid entered for an auction
before the auction ended. As new auction and bid data appears, the query
must be rerun to get up-to-date results.

1.  Using the [`CREATE VIEW`](/docs/self-managed/v25.2/sql/create-view/)
    command, create a
    [**view**](/docs/self-managed/v25.2/concepts/views/ "Saved name/alias for a query")
    to find the winning (highest) bids.

    <div class="highlight">

    ``` chroma
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

    </div>

    Materialize provides an idiomatic way to perform [Top-K
    queries](/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/top-k/)
    using the
    [`DISTINCT ON`](/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/top-k/#for-k--1-1)
    clause. This clause is used to group by account `id` and return the
    first element within that group according to the specified ordering.

2.  [`SELECT`](/docs/self-managed/v25.2/sql/select/) from the view to
    execute the underlying query. For example:

    <div class="highlight">

    ``` chroma
    SELECT * FROM winning_bids
    ORDER BY bid_time DESC
    LIMIT 10;
    ```

    </div>

    <div class="highlight">

    ``` chroma
    SELECT * FROM winning_bids
    WHERE item = 'Best Pizza in Town'
    ORDER BY bid_time DESC
    LIMIT 10;
    ```

    </div>

    Since new data is continually being ingested, you must rerun the
    query to get the up-to-date results. Each time you query the view,
    you are re-running the underlying statement, which becomes less
    performant as the amount of data grows.

    In Materialize, to make the queries more performant even as data
    continues to grow, you can create
    [**indexes**](/docs/self-managed/v25.2/concepts/indexes/) on views.
    Indexes provide always fresh view results in memory within a cluster
    by performing incremental updates as new data arrives. Queries can
    then read from the in-memory, already up-to-date results instead of
    re-running the underlying statement, making queries
    **computationally free and more performant**.

    In the next step, you will create an index on `winning_bids`.

## Step 4. Create an index to provide up-to-date results

Indexes in Materialize represents query results stored in memory within
a cluster. In Materialize, you can create
[indexes](/docs/self-managed/v25.2/concepts/indexes/) on views to
provide always fresh, up-to-date view results in memory within a
cluster. Queries can then read from the in-memory, already up-to-date
results instead of re-running the underlying statement.

To provide the up-to-date results, indexes **perform incremental
updates** as inputs change instead of recalculating the results from
scratch. Additionally, indexes can also help [optimize
operations](/docs/self-managed/v25.2/transform-data/optimization/) like
point lookups and joins.

1.  Use the [`CREATE INDEX`](/docs/self-managed/v25.2/sql/create-index/)
    command to create the following index on the `winning_bids` view.

    <div class="highlight">

    ``` chroma
    CREATE INDEX wins_by_item ON winning_bids (item);
    ```

    </div>

    During the index creation, the underlying `winning_bids` query is
    executed, and the view results are stored in memory within the
    cluster. As new data arrives, the index **incrementally updates**
    the view results in memory. Because incremental work is performed on
    writes, reads from indexes return up-to-date results and are
    computationally **free**.

    This index can **also** help [optimize
    operations](/docs/self-managed/v25.2/transform-data/optimization/)
    like point lookups and [delta
    joins](/docs/self-managed/v25.2/transform-data/optimization/#optimize-multi-way-joins-with-delta-joins)
    on the index column(s) as well as support ad-hoc queries.

2.  Rerun the previous queries on `winning_bids`.

    <div class="highlight">

    ``` chroma
    SELECT * FROM winning_bids
    ORDER BY bid_time DESC
    LIMIT 10;
    ```

    </div>

    <div class="highlight">

    ``` chroma
    SELECT * FROM winning_bids
    WHERE item = 'Best Pizza in Town'
    ORDER BY bid_time DESC
    LIMIT 10;
    ```

    </div>

    The queries should be faster since they use the in-memory, already
    up-to-date results computed by the index.

## Step 5. Create views and a table to find flippers in real time

For this quickstart, auction flipping activities are defined as when a
user buys an item in one auction and resells the same item at a higher
price within an 8-day period. This step finds auction flippers in real
time, based on auction flipping activity data and known flippers data.
Specifically, this step creates:

- A view to find auction flipping activities. Results are updated as new
  data comes in (at 1 second intervals) from the data generator.

- A table that maintains known auction flippers. You will manually enter
  new data to this table.

- A view to immediately see auction flippers based on both the flipping
  activities view and the known auction flippers table.

1.  Create a view to detect auction flipping activities.

    <div class="highlight">

    ``` chroma
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

    </div>

    The `flip_activities` view can use the index created on
    `winning_bids` view to provide up-to-date data.

    To view a sample row in `flip_activities`, run the following
    [`SELECT`](/docs/self-managed/v25.2/sql/select) command:

    <div class="highlight">

    ``` chroma
    SELECT * FROM flip_activities LIMIT 10;
    ```

    </div>

2.  Use [`CREATE TABLE`](/docs/self-managed/v25.2/sql/create-table) to
    create a `known_flippers` table that you can manually populate with
    known flippers. That is, assume that separate from your auction
    activities data, you receive independent data specifying users as
    flippers.

    <div class="highlight">

    ``` chroma
    CREATE TABLE known_flippers (flipper_id bigint);
    ```

    </div>

3.  Create a view `flippers` to flag known flipper accounts if a user
    has more than 2 flipping activities or the user is listed in the
    `known_flippers` table.

    <div class="highlight">

    ``` chroma
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

    </div>

<div class="note">

**NOTE:** Both the `flip_activities` and `flippers` views can use the
index created on `winning_bids` view to provide up-to-date data.
Depending upon your query patterns and usage, an existing index may be
sufficient, such as in this quickstart. In other use cases, creating an
index only on the view(s) from which you will serve results may be
preferred.

</div>

## Step 6. Subscribe to see results change

[`SUBSCRIBE`](/docs/self-managed/v25.2/sql/subscribe/) to `flippers` to
see new flippers appear as new data arrives (either from the
known_flippers table or the flip_activities view).

1.  Use [`SUBSCRIBE`](/docs/self-managed/v25.2/sql/subscribe/) command
    to see flippers as new data arrives (either from the
    `known_flippers` table or the `flip_activities` view).
    [`SUBSCRIBE`](/docs/self-managed/v25.2/sql/subscribe/) returns data
    from a source, table, view, or materialized view as they occur, in
    this case, the view `flippers`.

    <div class="highlight">

    ``` chroma
    SUBSCRIBE TO (
       SELECT *
       FROM flippers
    ) WITH (snapshot = false)
    ;
    ```

    </div>

    The optional [`WITH (snapshot = false)`
    option](/docs/self-managed/v25.2/sql/subscribe/#with-options)
    indicates that the command displays only the new flippers that come
    in after the start of the `SUBSCRIBE` operation, and not the
    flippers in the view at the start of the operation.

2.  In the Materialize Console quickstart page, enter an id (for example
    `450`) into the text input field to insert a new user into the
    `known-flippers` table. You can specify any number for the flipper
    id.

    The flipper should immediately appear in the `SUBSCRIBE` results.

    You should also see flippers who are flagged by their flip
    activities. Because of the randomness of the auction data being
    generated, user activity data that match the definition of a flipper
    may take some time even though auction data is constantly being
    ingested. However, once new matching data comes in, you will see it
    immediately in the `SUBSCRIBE` results. While waiting, you can enter
    additional flippers into the `known_flippers` table.

3.  To cancel out of the `SUBSCRIBE`, click the **Stop streaming**
    button.

## Step 7. Create views to verify that Materialize returns consistent data

To verify that Materialize serves **consistent results**, even as new
data comes in, this step creates the following views for completed
auctions:

- A view to keep track of each seller’s credits.

- A view to keep track of each buyer’s debits.

- A view that sums all sellers’ credits, all buyers’ debits, and
  calculates the difference, which should be `0`.

1.  Create a view to track credited amounts for sellers of completed
    auctions.

    <div class="highlight">

    ``` chroma
    CREATE VIEW seller_credits AS
    SELECT seller, SUM(amount) as credits
    FROM winning_bids
    GROUP BY seller;
    ```

    </div>

2.  Create a view to track debited amounts for the winning bidders of
    completed auctions.

    <div class="highlight">

    ``` chroma
    CREATE VIEW buyer_debits AS
    SELECT buyer, SUM(amount) as debits
    FROM winning_bids
    GROUP BY buyer;
    ```

    </div>

3.  To verify that the total credit and total debit amounts equal for
    completed auctions (i.e., to verify that the results are correct and
    consistent even as new data comes in), create a `funds_movement`
    view that calculates the total credits across sellers, total debits
    across buyers, and the difference between the two.

    <div class="highlight">

    ``` chroma
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

    </div>

    To see that the sums always equal even as new data comes in, you can
    `SUBSCRIBE` to this query:

    <div class="highlight">

    ``` chroma
    SUBSCRIBE TO (
       SELECT *
       FROM funds_movement
    );
    ```

    </div>

    Toggle `Show diffs` to see changes to `funds_movement`.

    - As new data comes in and auctions complete, the `total_credits`
      and `total_debits` values should change but the `total_difference`
      should remain `0`.
    - To cancel out of the `SUBSCRIBE`, click the **Stop streaming**
      button.

## Step 8. Clean up

To clean up the quickstart environment:

1.  Use the
    [`DROP SOURCE ... CASCADE`](/docs/self-managed/v25.2/sql/drop-source/)
    command to drop `auction_house` source and its dependent objects,
    including views and indexes created on the `auction_house`
    subsources.

    <div class="highlight">

    ``` chroma
    DROP SOURCE auction_house CASCADE;
    ```

    </div>

2.  Use the [`DROP TABLE`](/docs/self-managed/v25.2/sql/drop-table)
    command to drop the separate `known_flippers` table.

    <div class="highlight">

    ``` chroma
    DROP TABLE known_flippers;
    ```

    </div>

## Summary

In Materialize, [indexes](/docs/self-managed/v25.2/concepts/indexes/)
represent query results stored in memory within a cluster. When you
create an index on a view, the index incrementally updates the view
results (instead of recalculating the results from scratch) as
Materialize ingests new data. These up-to-date results are then
immediately available and computationally free for reads within the
cluster.

### General guidelines

This quickstart created an index on a view to maintain in-memory
up-to-date results in the cluster. In Materialize, both materialized
views and indexes on views incrementally update the view results.
Materialized views persist the query results in durable storage and is
available across clusters while indexes maintain the view results in
memory within a single cluster.

Some general guidelines for usage patterns include:

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Usage Pattern</th>
<th>General Guideline</th>
</tr>
</thead>
<tbody>
<tr>
<td>View results are accessed from a single cluster only;<br />
such as in a 1-cluster or a 2-cluster architecture.</td>
<td>View with an <a
href="/docs/self-managed/v25.2/sql/create-index">index</a></td>
</tr>
<tr>
<td>View used as a building block for stacked views; i.e., views not
used to serve results.</td>
<td>View</td>
</tr>
<tr>
<td>View results are accessed across <a
href="/docs/self-managed/v25.2/concepts/clusters">clusters</a>;<br />
such as in a 3-cluster architecture.</td>
<td>Materialized view (in the transform cluster)<br />
Index on the materialized view (in the serving cluster)</td>
</tr>
<tr>
<td>Use with a <a
href="/docs/self-managed/v25.2/serve-results/sink/">sink</a> or a <a
href="/docs/self-managed/v25.2/sql/subscribe"><code>SUBSCRIBE</code></a>
operation</td>
<td>Materialized view</td>
</tr>
<tr>
<td>Use with <a
href="/docs/self-managed/v25.2/transform-data/patterns/temporal-filters/">temporal
filters</a></td>
<td>Materialized view</td>
</tr>
</tbody>
</table>

The quickstart used an index since:

- The examples did not need to store the results in durable storage.

- All activities were limited to the single `quickstart` cluster.

- Although used, `SUBSCRIBE` operations were for illustrative/validation
  purposes and were not the final consumer of the views.

### Best practices

Before creating an index (which represents query results stored in
memory), consider its memory usage as well as its [compute cost
implications](/docs/self-managed/v25.2/administration/usage/#compute).
For best practices when creating indexes, see [Index Best
Practices](/docs/self-managed/v25.2/concepts/indexes/#best-practices).

### Additional information

- [Clusters](/docs/self-managed/v25.2/concepts/clusters)
- [Indexes](/docs/self-managed/v25.2/concepts/indexes)
- [Sources](/docs/self-managed/v25.2/concepts/sources)
- [Views](/docs/self-managed/v25.2/concepts/views/)
- [Idiomatic Materialize SQL
  chart](/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/appendix/idiomatic-sql-chart/)
- [Usage](/docs/self-managed/v25.2/administration/usage/#compute)
- [`CREATE INDEX`](/docs/self-managed/v25.2/sql/create-index/)
- [`CREATE SCHEMA`](/docs/self-managed/v25.2/sql/create-schema/)
- [`CREATE SOURCE`](/docs/self-managed/v25.2/sql/create-source/)
- [`CREATE TABLE`](/docs/self-managed/v25.2/sql/create-table)
- [`CREATE VIEW`](/docs/self-managed/v25.2/sql/create-view/)
- [`DROP VIEW`](/docs/self-managed/v25.2/sql/drop-view)
- [`DROP SOURCE`](/docs/self-managed/v25.2/sql/drop-source/)
- [`DROP TABLE`](/docs/self-managed/v25.2/sql/drop-table)
- [`SELECT`](/docs/self-managed/v25.2/sql/select)
- [`SUBSCRIBE`](/docs/self-managed/v25.2/sql/subscribe/)

## Next steps

To get started ingesting your own data from an external system like
Kafka, MySQL or PostgreSQL, check the documentation for
[sources](/docs/self-managed/v25.2/sql/create-source/), and navigate to
**Data** \> **Sources** \> **New source** in the [Materialize
Console](/docs/self-managed/v25.2/console/) to create your first source.

For help getting started with your data or other questions about
Materialize, you can schedule a [free guided
trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/get-started/quickstart.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
