# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


# The raw source that brings in data as a single text column containing JSON
> CREATE SOURCE market_orders_raw
  FROM PUBNUB
  SUBSCRIBE KEY 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe'
  CHANNEL 'pubnub-market-orders';

# Extract a couple JSON fields
> CREATE VIEW market_orders AS
  SELECT
    val->>'symbol' AS symbol,
    (val->'bid_price')::float AS bid_price
  FROM (SELECT text::jsonb AS val FROM market_orders_raw);

# Create a materialized aggregation
> CREATE MATERIALIZED VIEW avg_bid AS
  SELECT symbol, AVG(bid_price) FROM market_orders GROUP BY symbol;

# See the data change
> SELECT COUNT(*) > 0 FROM avg_bid;
true
