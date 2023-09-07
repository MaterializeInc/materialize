---
title: "Quickstart"
description: "Learn how to "
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

[//]: # "TODO(morsapaes) Introduction highlighting the three points below."

Our step-by-step will hit three of Materialize's main superpowers:
1. Interactivity:   get immediate responses from indexed warehouse relations and derived results.
2. Freshness:       watch results change immediately in response to *your* input changes.
3. Consistency:     results are always correct; never even transiently wrong.

## Before you begin

This quickstart uses a built-in load generator to get you up and running in a
few minutes: all you need is a Materialize account. If you already have one —
great! If not, [sign up for a sandbox account](https://materialize.com/register/) first.

When you're ready, head over to the [Materialize console](https://console.materialize.com/),
and pop open the SQL Shell.

## Step 1.

You'll use a sample auction house data set to build an operational use
case around auctions, bidders and (gasp) fraud. 🦹

As the auction house operator, you want to detect fraudulent behavior as soon as
it happens, so you can act on it immediately. Lately, you've been struggling
with [auction flippers](https://en.wikipedia.org/wiki/Flipping) — users that
purchase items only to quickly resell them for profit.

1. Let's start by kicking off the built-in [auction load generator](/sql/create-source/load-generator/#auction),
   so you have some data to work with.

    ```sql
    CREATE SOURCE auction_house
    FROM LOAD GENERATOR AUCTION
    (TICK INTERVAL '1s')
    FOR ALL TABLES
    WITH (SIZE = '3xsmall');
    ```
1. ...

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

    For now, you'll focus on the `auctions` and `bids` data sets.

1. Before moving on, get a sense of the data you'll be working with:

    ```sql
    SELECT * FROM auctions LIMIT 1;
    ```
    <p></p>

    ```nofmt
     id | seller |        item        |          end_time
    ----+--------+--------------------+----------------------------
      1 |   1824 | Best Pizza in Town | 2023-06-21 21:25:04.838+00
    ```

    ```sql
    SELECT * FROM bids LIMIT 1;
    ```
    <p></p>

    ```nofmt
     id | buyer | auction_id | amount |          bid_time
    ----+-------+------------+--------+----------------------------
     10 |  3844 |          1 |     59 | 2023-06-21 21:24:54.838+00
    ```

## Step 2.

To identify potential auction flippers, you need to keep track of the winning
bids (or, the bid with the highest `amount`) for each completed auction.

1. ...

    ```sql
    CREATE VIEW winning_bids AS
    SELECT DISTINCT ON (auctions.id) bids.*, auctions.seller
    FROM auctions, bids
    WHERE auctions.id = bids.auction_id
      AND bids.bid_time < auctions.end_time
      AND mz_now() >= auctions.end_time
    ORDER BY auctions.id, bids.amount DESC;
    ```

    - Note on DISTINCT ON vs ROW_NUMBER(), and why we use it here (it's still a
      little exotic even for me, so I expect users to be confused here)?
    - Note about the temporal filter, which guarantees we're only looking at
      auctions that have ended. It's a dense query to parse.

1. You can query the view directly, but this shouldn't be satisfying yet. This
re-runs the embedded statament, which on growing amounts of data comes at some
cost.

    ```sql
    SELECT * FROM winning_bids;
    ```

1. Yikes! **Operational work requires interactive access to data as soon as
it's available**. In Materialize, you use [indexes](/sql/create-index/) to keep
results incrementally updated and immediately accessible.

    Let's try creating several indexes on the `winning_bids` view using columns
    that can help optimize operations like point lookups and joins.

    ```sql
    CREATE INDEX wins_by_auction ON winning_bids (auction_id);
    CREATE INDEX wins_by_bidder ON winning_bids (buyer);
    CREATE INDEX wins_by_seller ON winning_bids (seller);
    ```

    These indexes will hold the results of `winning_bids` in memory, and work
    like a cache — except you don't need to wire up one, or worry about the
    results getting stale.

    This gives you more time to worry about your actual
    use case!

[//]: # "NOTE(morsapaes) There's a risk that these don't return anything. Swapping auction_id for item_name and index on that would be better? Change."

1. If you now try to read out of `winning_bids` while hitting one of these
indexes (e.g., with a point lookup), things should be a whole lot more
interactive.

    ```sql
    SELECT * FROM winning_bids WHERE auction_id = 51;
    SELECT * FROM winning_bids WHERE buyer = 3546;
    ```

    But to detect and act upon fraud, you can't rely on manual checks, right? You want
    to keep a running tab on these flippers.

[//]: # "NOTE(morsapaes) This query is borderline unpredictable since we're
relying on a load generator, but _mostly_ returns some results. The experience
won't be great when it doesn't."

1. Luckily, the indexes you created in the previous step also make joins more interactive (as in any other database).

    ```sql
    CREATE VIEW fraud_activity
    SELECT w2.seller,
           w2.auction_id,
           w1.amount original_amount,
           w2.amount resell_amount
    FROM winning_bids w1,
         winning_bids w2
    WHERE w1.buyer = w2.seller
      AND w2.amount > w1.amount;
    ```

    Aha! You can now catch any auction flippers in real time.

    ```sql
    SELECT * FROM fraud_activity;
    ```

[//]: # "NOTE(morsapaes) This section seems cumbersome, and like we could skip
it? ISTM like we could spin the previous section to show the freshness
principle and it'd be enough/more engaging."

## Step 3.

1. **Operational work needs to surface and act on the most recent data.** Let's interact with the `fraud_accounts` table and change some auctions.

    We'll also imagine a second source of data about potentially fraudulent accounts.
    We'll use a `TABLE` to represent this, as the load generator does not provide it.
    You'll be able to directly manipulate this table, and see the consequences immediately!

    ```sql
    -- Create a table to hold fraudulent account ids.
    CREATE TABLE fraud_accounts (id bigint);
    ```

    You can insert, update, and delete rows in a table like so:
    ```sql
    -- Manipulate fraudulent accounts.
    INSERT INTO fraud_accounts SELECT 17;
    UPDATE fraud_accounts SET id = 14 WHERE id = 17;
    DELETE FROM fraud_accounts WHERE id = 14;
    ```

1. ...

    ```sql
    CREATE VIEW winning_bids_fraud AS
    SELECT * FROM winning_bids
    WHERE NOT EXISTS ( SELECT * FROM fraud_accounts );
    ```

    ```sql
    SELECT * FROM winning_bids_fraud;
    ```

1. In a new browser window, navigate to the [Materialize console](https://console.materialize.com/),
and pop open the SQL Shell.

    To see the outcomes as the happen, we'll `SUBSCRIBE` to a summary query.
    This is best done in a new window, open to the side of your main one.
    Do that right now, and enter the following command:

    ```sql
    -- Observe the top 5 winners of auctions
    SUBSCRIBE TO (
      SELECT buyer, count(*)
      FROM winning_bids
      GROUP BY buyer
      ORDER BY 2 DESC LIMIT 5
    );
    ```

    You can watch these results change, though they may not change much at the moment.
    We'll fix that in the next step!

1. ...

    Pick your least favorite buyer from the list maintained in the `SUBSCRIBE` window.
    We'll enter that as a fraudulent account.
    This should cause them to immediately drop out of the top five.

    ```sql
    -- Back in the first shell, let's mark one of these buyers as fraudulent.
    -- Before you press enter, get your eyes back on that `SUBSCRIBE` output.
    INSERT INTO fraud_accounts SELECT <least-favorite-buyer>;
    ```

    Now is a great time to use your `INSERT`, `UPDATE`, and `DELETE` skills to manipulate `fraud_accounts` and see the results change.

## Step 4. 

With fraud out of the way, you can now shift your focus to a different
operational use case: profit & loss alerts.

Before you warn a user that they've spent much more than they've earned, you
want to be sure your results are trustworthy — it's real money we're talking
about, after all! **Operational work needs to act on correct and consistent
data.**

1. -- Each auction win moves funds from the buyer to the seller.

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

[//]: # "NOTE(morsapaes) SUBSCRIBE is a cool way to see this, but with the
source ticking every second, it looks pretty...disappointing? It's also hard to
notice that the numbers are changing."

1. There are so many results, and they change, so it's hard to eyeball whether they are correct.
Instead, let's write some diagnostic queries that should tell us.
The total credits and total debits should always add up.

    ```sql
    -- Inspect the net movement of funds.
    SELECT SUM(credits), SUM(debits) FROM funds_movement;
    ```

    You can also `SUBSCRIBE` to this query, and watch the sums change in lock step.

    ```sql
    -- Monitor the net movement of funds.
    SUBSCRIBE TO (
        SELECT SUM(credits), SUM(debits) FROM funds_movement
    );
    ```

It is never wrong, is it?

## What's next?

[//]: # "TODO(morsapaes) Conclusion + extend to suggest third party tools? dbt, Census and Metabase could all fit here to do interesting things as a follow-up."
