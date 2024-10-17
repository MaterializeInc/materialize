# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Tests that assert the privileges that are assumed to be always granted to #
# the mz_support user. This test can be rewritten to validate the output of the
# `connection=mz_support` command once we have `SET ROLE` working.

$ postgres-connect name=mz_support url=postgres://mz_support:materialize@${testdrive.materialize-internal-sql-addr}

> CREATE SOURCE auction_house
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION (UP TO 100);

> CREATE TABLE accounts FROM SOURCE auction_house (REFERENCE accounts);
> CREATE TABLE auctions FROM SOURCE auction_house (REFERENCE auctions);
> CREATE TABLE bids FROM SOURCE auction_house (REFERENCE bids);
> CREATE TABLE organizations FROM SOURCE auction_house (REFERENCE organizations);
> CREATE TABLE users FROM SOURCE auction_house (REFERENCE users);

> CREATE VIEW v AS SELECT b.auction_id, b.buyer, b.amount, b.bid_time, u.org_id FROM bids b JOIN users u ON(b.buyer = u.id);

# The mz_support user can list database sources.
$ postgres-execute connection=mz_support
SHOW SOURCES;

# The mz_support user can execute `SHOW CREATE ...` commands.
$ postgres-execute connection=mz_support
SHOW CREATE TABLE bids;

# The mz_support user can execute `EXPLAIN PLAN ... FOR SELECT` commands.
$ postgres-execute connection=mz_support
EXPLAIN OPTIMIZED PLAN FOR SELECT b.auction_id, b.buyer, b.amount, b.bid_time, u.org_id FROM bids b JOIN users u ON(b.buyer = u.id);

# The mz_support user can execute `EXPLAIN PLAN ... FOR CREATE MATERIALIZED VIEW` commands.
$ postgres-execute connection=mz_support
EXPLAIN OPTIMIZED PLAN FOR CREATE MATERIALIZED VIEW mv AS SELECT b.auction_id, b.buyer, b.amount, b.bid_time, u.org_id FROM bids b JOIN users u ON(b.buyer = u.id);

# The mz_support user can execute `EXPLAIN PLAN ... FOR CREATE INDEX` commands.
$ postgres-execute connection=mz_support
EXPLAIN OPTIMIZED PLAN FOR CREATE INDEX ON v(auction_id);

# The mz_support user can execute `EXPLAIN TIMESTAMP ...` commands.
$ postgres-execute connection=mz_support
EXPLAIN TIMESTAMP FOR SELECT b.auction_id, b.buyer, b.amount, b.bid_time, u.org_id FROM bids b JOIN users u ON(b.buyer = u.id);

# The mz_support user can filter SHOW commands on user objects.
$ postgres-execute connection=mz_support
SHOW INDEXES ON bids;

> DROP SOURCE auction_house CASCADE;
