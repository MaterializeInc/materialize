---
title: "Quickstart"
description: "Learn the basics of Materialize, the operational data warehouse."
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

Materialize is a new kind of **data warehouse built for operational workloads**: the
instant your data changes, Materialize reacts. This quickstart will get you up
and running in a few minutes and with no dependencies, so you can experience
the superpowers of an operational data warehouse first-hand:

* **Interactivity**: get immediate responses from indexed warehouse relations and derived results.

* **Freshness**: watch results change immediately in response to *your* input changes.

* **Consistency**: results are always correct; never even transiently wrong.

## Before you begin

All you need is a Materialize account. If you already have one —
great! If not, [sign up for a playground account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation) first.

When you're ready, head over to the [Materialize console](https://console.materialize.com/),
and pop open the SQL Shell.

## Step 1. Ingest streaming data

You'll use a sample auction house data set to build an operational use
case around auctions, bidders and (gasp) fraud. 🦹

As the auction house operator, you want to detect fraudulent behavior as soon as
it happens, so you can act on it immediately. Lately, you've been struggling
with [auction flippers](https://en.wikipedia.org/wiki/Flipping) — users that
purchase items only to quickly resell them for profit.

1. Let's start by kicking off the built-in [auction load generator](/sql/create-source/load-generator/#auction), so you have some data to work
with.

    ```sql
    CREATE SOURCE auction_house
    FROM LOAD GENERATOR AUCTION
    (TICK INTERVAL '1s')
    FOR ALL TABLES;
    ```
1. Use the [`SHOW SOURCES`](/sql/show-sources/) command to get an idea of the data being generated:

    ```sql
    SHOW SOURCES;
    ```
    <p></p>

    ```nofmt
            name            |      type      | size
    ------------------------+----------------+------
     accounts               | subsource      | null
     auction_house          | load-generator | 3xsmall
     auction_house_progress | progress       | null
     auctions               | subsource      | null
     bids                   | subsource      | null
     organizations          | subsource      | null
     users                  | subsource      | null
    ```

    For now, you'll focus on the `auctions` and `bids` data sets. Data will
    be **continually produced** as you walk through the quickstart.

1. Before moving on, get a sense for the data you'll be working with:

    ```sql
    SELECT * FROM auctions LIMIT 1;
    ```
    <p></p>

    ```nofmt
     id | seller |        item        |          end_time
    ----+--------+--------------------+----------------------------
      1 |   1824 | Best Pizza in Town | 2023-09-10 21:24:54.838+00
    ```

    ```sql
    SELECT * FROM bids LIMIT 1;
    ```
    <p></p>

    ```nofmt
     id | buyer | auction_id | amount |          bid_time
    ----+-------+------------+--------+----------------------------
     10 |  3844 |          1 |     59 | 2023-09-10 21:25:00.465+00
    ```

## Step 2. Use indexes for speed

**Operational work requires interactive access to data as soon as it's
  available**. To identify potential auction flippers, you need to keep track
  of the winning bids for each completed auction.

1. Create a [**view**](/get-started/key-concepts/#non-materialized-views) that
joins data from `auctions` and `bids` to get the bid with the highest `amount`
for each auction at its `end_time`.

    ```sql
    CREATE VIEW winning_bids AS
    SELECT DISTINCT ON (auctions.id) bids.*, auctions.item, auctions.seller
    FROM auctions, bids
    WHERE auctions.id = bids.auction_id
      AND bids.bid_time < auctions.end_time
      AND mz_now() >= auctions.end_time
    ORDER BY auctions.id,
      bids.bid_time DESC,
      bids.amount,
      bids.buyer;
    ```

    Like in other SQL databases, a view in Materialize is just an alias for the
    embedded `SELECT` statement; results are computed only when the view is
    called.

1. You can query the view directly, but this shouldn't be very impressive just
yet! Querying the view re-runs the embedded statament, which comes at some cost
on growing amounts of data.

    ```sql
    SELECT * FROM winning_bids;
    ```

   Yikes! In Materialize, you use [**indexes**](/sql/create-index/) to
   keep results incrementally updated and immediately accessible.

1. Next, try creating several indexes on the `winning_bids` view using columns
that can help optimize operations like point lookups and joins.

    ```sql
    CREATE INDEX wins_by_item ON winning_bids (item);
    CREATE INDEX wins_by_bidder ON winning_bids (buyer);
    CREATE INDEX wins_by_seller ON winning_bids (seller);
    ```

   These indexes will hold the results of `winning_bids` **in memory**, and work
   like a cache — except you don't need to wire up one, or worry about the
   results getting stale.

1. If you now try to read out of `winning_bids` while hitting one of these
indexes (e.g., with a point lookup), things should be a whole lot more
interactive.

    ```sql
    SELECT * FROM winning_bids WHERE item = 'Best Pizza in Town' ORDER BY bid_time DESC;
    ```

   But to detect and act upon fraud, you can't rely on manual checks, right? You
   want to keep a running tab on these flippers. Luckily, the indexes you
   created in the previous step also make joins more interactive (as in other
   databases)!

    [//]: # "NOTE(morsapaes) This query is borderline unpredictable since we're
    relying on a load generator, but _mostly_ returns some results. The experience
    won't be great when it doesn't."

1. Create a view that detects when a user wins an auction as a bidder, and then
is identified as a seller for an item at a higher price.

    ```sql
    CREATE VIEW fraud_activity AS
    SELECT w2.seller,
           w2.item AS seller_item,
           w2.amount AS seller_amount,
           w1.item buyer_item,
           w1.amount buyer_amount
    FROM winning_bids w1,
         winning_bids w2
    WHERE w1.buyer = w2.seller
      AND w2.amount > w1.amount;
    ```

    Aha! You can now catch any auction flippers in real time, based on the results of this view.

    ```sql
    SELECT * FROM fraud_activity;
    ```

## Step 3. See results change!

**Operational work needs to surface and act on the most recent data.** The
  moment your data changes, Materialize reacts. Let's verify that this _really_
  happens by manually flagging some accounts as fraudulent, and observing
  results change in real time!

1. Create a [**table**](/sql/create-table/) that allows you to manually flag
fraudulent accounts.

    ```sql
    CREATE TABLE fraud_accounts (id bigint);
    ```

1. In a new browser window, side-by-side with this one, navigate to the [Materialize console](https://console.materialize.com/),
   and pop open another SQL Shell.

1. To see results change over time, let's `SUBSCRIBE` to a query that returns
the Top 5 auction winners, overall.

   ```sql
   SUBSCRIBE TO (
     SELECT buyer, count(*)
     FROM winning_bids
     WHERE buyer NOT IN (SELECT id FROM fraud_accounts)
     GROUP BY buyer
     ORDER BY 2 DESC LIMIT 5
   );
   ```

   You can keep an eye on the results, but these may not change much at the
   moment. You'll fix that in the next step!

1. Pick one of the buyers from the list maintained in the window that is running
the `SUBSCRIBE`, and mark them as fraudulent by adding them to the
`fraud_accounts` table.

   ```sql
   INSERT INTO fraud_accounts VALUES (<id>);
   ```

   This should cause the flagged buyer to immediately drop out of the Top 5! If
   you click **Show diffs**, you'll notice that the picked buyer was kicked
   out, and the next non-fraudulent buyer in line automatically entered the
   Top 5.

   When you're done, cancel out of the `SUBSCRIBE` using **Stop streaming**, and close
   the secondary browser window.

## Step 4. Serve correct results

With fraud out of the way, you can now shift your focus to a different
operational use case: profit & loss alerts.

**Operational work needs to act on correct and consistent data.** Before you
  warn a user that they've spent much more than they've earned, you want to be
  sure your results are trustworthy — it's real money we're talking about,
  after all!

1. Create a view to track the sales and purchases of each auction house user.

    ```sql
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

    ```sql
    SELECT SUM(credits), SUM(debits) FROM funds_movement;
    ```

    You can also `SUBSCRIBE` to this query, and watch the sums change in lock step as auctions close.

    ```sql
    SUBSCRIBE TO (
        SELECT SUM(credits), SUM(debits) FROM funds_movement
    );
    ```

   It is never wrong, is it?

## Step 5. Clean up

As the auction house operator, you should now have a high degree of confidence that Materialize can help you implement and automate operational use cases that depend on fresh, consistent results served in an interactive way.

Once you’re done exploring the auction house source, remember to clean up your environment:

```sql
DROP SOURCE auction_house CASCADE;

DROP TABLE fraud_accounts;
```

## What's next?

[//]: # "TODO(morsapaes) Extend to suggest third party tools. dbt, Census and Metabase could all fit here to do interesting things as a follow-up."

To get started with your own data, [upgrade your playground to a trial account](https://materialize.com/trial/?utm_campaign=General&utm_source=documentation).
