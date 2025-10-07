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

$ set-sql-timeout duration=60s

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_create_table_from_source = true

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

> CREATE VIEW winning_bids AS
  SELECT DISTINCT ON (auctions.id) bids.*, auctions.item, auctions.seller
  FROM auctions, bids
  WHERE auctions.id = bids.auction_id
    AND bids.bid_time < auctions.end_time
    AND mz_now() >= auctions.end_time
  ORDER BY auctions.id,
    bids.bid_time DESC,
    bids.amount,
    bids.buyer

> SELECT id, buyer, auction_id, amount, item, seller FROM winning_bids WHERE id = 18
18 1036 1 86 "Best Pizza in Town" 1824

> CREATE INDEX wins_by_item ON winning_bids (item)
> CREATE INDEX wins_by_bidder ON winning_bids (buyer)
> CREATE INDEX wins_by_seller ON winning_bids (seller)

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM winning_bids WHERE item = 'Best Pizza in Town' ORDER BY bid_time DESC
Explained Query (fast path):
  Finish order_by=[#4 desc nulls_first] output=[#0..=#6]
    Project (#1..=#5, #0, #6)
      ReadIndex on=materialize.public.winning_bids wins_by_item=[lookup value=("Best Pizza in Town")]

Used Indexes:
  - materialize.public.wins_by_item (lookup)

Target cluster: quickstart_tutorial

> SELECT id, buyer, auction_id, amount, item, seller FROM winning_bids WHERE item = 'Best Pizza in Town' AND id < 100 ORDER BY bid_time DESC
62 3016 6 77 "Best Pizza in Town" 2500
46 2530 4 49 "Best Pizza in Town" 2822
18 1036 1 86 "Best Pizza in Town" 1824

> CREATE VIEW fraud_activity AS
  SELECT w2.seller,
         w2.item AS seller_item,
         w2.amount AS seller_amount,
         w1.item buyer_item,
         w1.amount buyer_amount
  FROM winning_bids w1,
       winning_bids w2
  WHERE w1.buyer = w2.seller
    AND w2.amount > w1.amount

> SELECT * FROM fraud_activity where seller_item = 'Gift Basket' and seller = 3408
3408 "Gift Basket" 69 "Gift Basket" 58

> CREATE TABLE fraud_accounts (id bigint)

$ set-regex match=\d{13,20} replacement=<TIMESTAMP>

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE TO (
    SELECT buyer
    FROM winning_bids
    WHERE buyer NOT IN (SELECT id FROM fraud_accounts) AND buyer = 12
    GROUP BY buyer
    ORDER BY 1 ASC LIMIT 5
  )

> FETCH 1 c WITH (timeout='30s')
<TIMESTAMP> 1 12

$ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
INSERT INTO fraud_accounts VALUES (12)

> FETCH 1 c WITH (timeout='30s')
<TIMESTAMP> -1 12

> COMMIT

> CREATE VIEW funds_movement AS
  SELECT id, SUM(credits) as credits, SUM(debits) as debits
  FROM (
    SELECT seller as id, amount as credits, 0 as debits
    FROM winning_bids
    UNION ALL
    SELECT buyer as id, 0 as credits, amount as debits
    FROM winning_bids
  )
  GROUP BY id

# > SELECT SUM(credits), SUM(debits) FROM funds_movement
# 37529 37529
#
# > BEGIN
# > DECLARE c CURSOR FOR SUBSCRIBE TO (
#     SELECT SUM(credits), SUM(debits) FROM funds_movement
#   )
# > FETCH 1 c WITH (timeout='1s')
