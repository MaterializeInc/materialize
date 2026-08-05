# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test verifies the Quickstart page works: https://materialize.com/docs/get-started/quickstart/
# Uses shared compute+storage cluster
#
# Deviations from the page, and why:
#
# - The generator runs with (TICK INTERVAL '0.05s', UP TO 1000) instead of the
#   page's (TICK INTERVAL '1s', AS OF 100000), so the dataset is finite and the
#   row contents are deterministic under the generator's fixed RNG seed.
# - All five auction relations get a table, not just the page's `auctions` and
#   `bids`, to keep coverage of ingesting every relation of a multi-output
#   source.
# - `auctions.end_time` is wall-clock derived (generation time + 10s), so
#   `winning_bids` grows over the run as `mz_now()` passes each auction's end.
#   Assertions therefore either pin rows that are append-only (testdrive retries
#   until they appear, and neither the generator nor an ended auction's winning
#   bid ever changes) or assert invariants that hold at every timestamp. Absolute
#   sums over the whole of `winning_bids` are not deterministic, so the funds
#   check below asserts only that credits and debits balance, not their values.

$ set-sql-timeout duration=60s

> CREATE CLUSTER quickstart_tutorial REPLICAS (r1 (SIZE 'scale=1,workers=4'));

> SET CLUSTER = quickstart_tutorial

> BEGIN
> CREATE SOURCE auction_house
  IN CLUSTER quickstart_tutorial
  FROM LOAD GENERATOR AUCTION
  (TICK INTERVAL '0.05s', UP TO 1000)

> CREATE TABLE accounts FROM SOURCE auction_house (REFERENCE accounts);
> CREATE TABLE auctions FROM SOURCE auction_house (REFERENCE auctions);
> CREATE TABLE bids FROM SOURCE auction_house (REFERENCE bids);
> CREATE TABLE organizations FROM SOURCE auction_house (REFERENCE organizations);
> CREATE TABLE users FROM SOURCE auction_house (REFERENCE users);
> COMMIT

> SHOW SOURCES
name                   type            cluster                comment
--------------------------------------------------------------------------
auction_house          load-generator  quickstart_tutorial    ""

> SHOW TABLES
name                   comment
--------------------------------
accounts               ""
bids                   ""
auctions               ""
organizations          ""
users                  ""

> SELECT id, seller, item FROM auctions WHERE id = 1
1 1824 "Best Pizza in Town"

> SELECT id, buyer, auction_id, amount FROM bids WHERE id = 10
10 3844 1 59

# Step 3: the winning bid for an auction is the highest bid placed before the
# auction ended, so `amount DESC` leads the DISTINCT ON ordering.
> CREATE VIEW winning_bids AS
  SELECT DISTINCT ON (a.id) b.*, a.item, a.seller
  FROM auctions AS a
   JOIN bids AS b
     ON a.id = b.auction_id
  WHERE b.bid_time < a.end_time
    AND mz_now() >= a.end_time
  ORDER BY a.id,
    b.amount DESC,
    b.bid_time,
    b.buyer

> SELECT id, buyer, auction_id, amount, item, seller FROM winning_bids WHERE auction_id = 1
12 3338 1 97 "Best Pizza in Town" 1824

# Step 4: index for point lookups on `item`.
> CREATE INDEX wins_by_item ON winning_bids (item)

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM winning_bids WHERE item = 'Best Pizza in Town' ORDER BY bid_time DESC
Explained Query (fast path):
  Finish order_by=[#4 desc nulls_first] output=[#0..=#6]
    Project (#1..=#5, #0, #6)
      ReadIndex on=materialize.public.winning_bids wins_by_item=[lookup value=("Best Pizza in Town")]

Used Indexes:
  - materialize.public.wins_by_item (lookup)

Target cluster: quickstart_tutorial

> SELECT id, buyer, auction_id, amount, item, seller FROM winning_bids WHERE item = 'Best Pizza in Town' AND auction_id < 10 ORDER BY auction_id
12 3338 1 97 "Best Pizza in Town" 1824
44 719 4 96 "Best Pizza in Town" 2822
62 3016 6 77 "Best Pizza in Town" 2500

# Step 5: auction flipping activity. Buying an item and reselling the same item
# at a higher price within 8 days.
> CREATE VIEW flip_activities AS
  SELECT w2.seller as flipper_id,
         w2.item AS item,
         w2.amount AS sold_amount,
         w1.amount AS purchased_amount,
         w2.amount - w1.amount AS diff_amount,
         datediff('days', w2.bid_time, w1.bid_time) AS timeframe_days
   FROM  winning_bids AS w1
     JOIN winning_bids AS w2
       ON w1.buyer = w2.seller
          AND w1.item = w2.item
   WHERE w2.amount > w1.amount
     AND datediff('days', w2.bid_time, w1.bid_time) < 8

# Indexes on the complete join keys, so the self-join does not have to form
# every pair of rows sharing an `item` before filtering on buyer/seller.
> CREATE INDEX wins_by_item_seller ON winning_bids (item, seller)
> CREATE INDEX wins_by_item_buyer ON winning_bids (item, buyer)

# The join arranges on the complete keys `[item, buyer]` and `[item, seller]`,
# so it never has to pair every row sharing an `item` and filter afterwards.
? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM flip_activities
Explained Query:
  Project (#1, #5, #10, #3, #15, #14)
    Filter (#14 < 8) AND (#10{amount} > #3{amount})
      Map (datediff("days", #11{bid_time}, #4{bid_time}), (#10{amount} - #3{amount}))
        Join on=(#1{buyer} = #13{seller} AND #5{item} = #12{item}) type=differential
          ArrangeBy keys=[[#5{item}, #1{buyer}]]
            ReadIndex on=winning_bids wins_by_item_buyer=[differential join]
          ArrangeBy keys=[[#5{item}, #6{seller}]]
            ReadIndex on=winning_bids wins_by_item_seller=[differential join]

Used Indexes:
  - materialize.public.wins_by_item_seller (differential join)
  - materialize.public.wins_by_item_buyer (differential join)

Target cluster: quickstart_tutorial

# Invariants that hold at every timestamp, regardless of how many auctions have
# ended, so these fail fast if the view definition drifts.
> SELECT count(*) FROM flip_activities WHERE diff_amount != sold_amount - purchased_amount
0

> SELECT count(*) FROM flip_activities WHERE sold_amount <= purchased_amount
0

# The generated data contains exactly one flip, even once every auction has
# ended: user 2646 bought a "City Bar Crawl" for 46 and resold it for 99. Since
# `flip_activities` is append-only here and never exceeds this one row, matching
# the full view is stable rather than a race against ingestion.
> SELECT * FROM flip_activities
2646 "City Bar Crawl" 99 46 53 0

# Only `known_flippers` can flag a flipper in this dataset. The single flip
# activity does not reach the view's `HAVING count(*) >= 2` threshold.
> SELECT count(*) FROM (SELECT flipper_id FROM flip_activities GROUP BY flipper_id HAVING count(*) >= 2)
0

> CREATE TABLE known_flippers (flipper_id bigint)

> CREATE VIEW flippers AS
  SELECT flipper_id
  FROM (
      SELECT flipper_id
      FROM flip_activities
      GROUP BY flipper_id
      HAVING count(*) >= 2
      UNION ALL
      SELECT flipper_id
      FROM known_flippers
  )

$ set-regex match=\d{13,20} replacement=<TIMESTAMP>

# Step 6: a flipper entered by hand shows up in `flippers` immediately. The id
# is outside the generator's user id range (1..=4076) so that the generated
# auction data cannot also produce it via `flip_activities`.
#
# The page uses WITH (snapshot = false) here. That option delivers nothing when
# the SUBSCRIBE is driven through a cursor in an explicit transaction, which is
# the only form testdrive can drive, so this relies on the snapshot being empty
# for the filtered id instead. FETCH then blocks until the INSERT lands, which
# is the same "appears live" behavior the page describes.
> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE TO (
    SELECT * FROM flippers WHERE flipper_id = 999999
  )

$ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
INSERT INTO known_flippers VALUES (999999)

> FETCH 1 c WITH (timeout='30s')
<TIMESTAMP> 1 999999

> COMMIT

# Step 7: credits and debits over completed auctions always balance, because
# every winning bid credits its seller and debits its buyer by the same amount.
# The individual sums grow as auctions end, but the difference is always 0.
> CREATE VIEW seller_credits AS
  SELECT seller, SUM(amount) as credits
  FROM winning_bids
  GROUP BY seller

> CREATE VIEW buyer_debits AS
  SELECT buyer, SUM(amount) as debits
  FROM winning_bids
  GROUP BY buyer

> CREATE VIEW funds_movement AS
  SELECT SUM(credits) AS total_credits,
         SUM(debits) AS total_debits,
         SUM(credits) - SUM(debits) AS total_difference
  FROM (
    SELECT SUM(credits) AS credits, 0 AS debits
    FROM seller_credits
    UNION
    SELECT 0 AS credits, SUM(debits) AS debits
    FROM buyer_debits
  )

> SELECT total_credits = total_debits, total_difference FROM funds_movement
true 0

> BEGIN

> DECLARE c2 CURSOR FOR SUBSCRIBE TO (
    SELECT total_difference FROM funds_movement
  )

> FETCH 1 c2 WITH (timeout='30s')
<TIMESTAMP> 1 0

> COMMIT

# Step 8: clean up.
> DROP SOURCE auction_house CASCADE

> DROP TABLE known_flippers

> SHOW SOURCES

> SHOW TABLES
