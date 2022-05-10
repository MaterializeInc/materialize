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

# Test that the volatility bit is set correctly.
> SELECT name, volatility FROM mz_sources WHERE name = 'market_orders_raw'
market_orders_raw   volatile
> SELECT name, volatility from mz_views WHERE name = 'avg_bid'
avg_bid   volatile
> CREATE MATERIALIZED VIEW nonvol AS SELECT 1
> SELECT name, volatility from mz_views WHERE name = 'nonvol'
nonvol   nonvolatile
> CREATE MATERIALIZED VIEW depends_vol_nonvol AS SELECT * FROM nonvol, avg_bid
> SELECT name, volatility from mz_views WHERE name = 'depends_vol_nonvol'
depends_vol_nonvol   volatile
$ file-append path=test
ignored
> CREATE MATERIALIZED SOURCE kafka_src
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'ignored' FORMAT BYTES
> SELECT name, volatility FROM mz_sources WHERE name = 'kafka_src'
kafka_src  unknown
> CREATE MATERIALIZED VIEW depends_nonvol_unknown AS SELECT * FROM nonvol, kafka_src
> SELECT name, volatility from mz_views WHERE name = 'depends_nonvol_unknown'
depends_nonvol_unknown   unknown
> CREATE MATERIALIZED VIEW depends_vol_unknown AS SELECT * FROM avg_bid, kafka_src
> SELECT name, volatility from mz_views WHERE name = 'depends_vol_unknown'
depends_vol_unknown   volatile
