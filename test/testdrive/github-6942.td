# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=scale=1,workers=1

> DROP CLUSTER IF EXISTS gh_6942_cluster CASCADE;
> CREATE CLUSTER gh_6942_cluster SIZE '${arg.default-storage-size}', REPLICATION FACTOR 1;

> BEGIN
> CREATE SOURCE auction_house
  IN CLUSTER gh_6942_cluster
  FROM LOAD GENERATOR AUCTION
  (TICK INTERVAL '0.01s', UP TO 1000)

> CREATE TABLE accounts FROM SOURCE auction_house (REFERENCE accounts);
> CREATE TABLE auctions FROM SOURCE auction_house (REFERENCE auctions);
> CREATE TABLE bids FROM SOURCE auction_house (REFERENCE bids);
> CREATE TABLE organizations FROM SOURCE auction_house (REFERENCE organizations);
> CREATE TABLE users FROM SOURCE auction_house (REFERENCE users);
> COMMIT

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

> CREATE INDEX wins_by_item ON winning_bids (item)
> CREATE INDEX wins_by_bidder ON winning_bids (buyer)
> CREATE INDEX wins_by_seller ON winning_bids (seller)

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

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
DROP DATABASE materialize

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
CREATE DATABASE materialize
