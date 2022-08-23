# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test verifies the get started page works.

> CREATE SOURCE demo FROM LOAD GENERATOR AUCTION TICK INTERVAL '50ms'

> CREATE VIEWS FROM SOURCE demo

> SHOW VIEWS
 accounts
 auctions
 bids
 organizations
 users

> SHOW COLUMNS FROM auctions
end_time true "timestamp with time zone"
id true bigint
item true text
seller true bigint

> SHOW COLUMNS FROM bids
amount true integer
auction_id true bigint
bid_time true "timestamp with time zone"
buyer true bigint
id true bigint

> CREATE VIEW on_time_bids AS
  SELECT
    bids.id       AS bid_id,
    auctions.id   AS auction_id,
    auctions.item,
    bids.bid_time,
    auctions.end_time,
    bids.amount
  FROM bids
  JOIN auctions ON bids.auction_id = auctions.id
  WHERE bids.bid_time < auctions.end_time

> CREATE MATERIALIZED VIEW avg_bids AS
  SELECT auction_id, avg(amount) AS amount
  FROM on_time_bids
  GROUP BY auction_id

> SELECT auction_id, MAX(amount)
  FROM on_time_bids
  GROUP BY auction_id
  ORDER BY auction_id LIMIT 5
1 97
2 44
3 67
4 96
5 95

> CREATE VIEW highest_bid_per_auction AS
  SELECT grp.auction_id, bid_id, item, amount, end_time FROM
    (SELECT DISTINCT auction_id FROM on_time_bids) grp,
    LATERAL (
        SELECT * FROM on_time_bids
        WHERE auction_id = grp.auction_id
        ORDER BY amount DESC LIMIT 1
    )


> CREATE MATERIALIZED VIEW winning_bids AS
  SELECT * FROM highest_bid_per_auction WHERE extract(epoch FROM end_time) * 1000 < mz_logical_timestamp()

> SELECT auction_id, bid_id, item, amount FROM winning_bids ORDER BY auction_id LIMIT 5
1 12 "Best Pizza in Town" 97
2 20 "Custom Art" 44
3 31 "City Bar Crawl" 67
4 44 "Best Pizza in Town" 96
5 54 "Signed Memorabilia" 95

# Shut down the source so it doesn't keep running until next testdrive reset.
> DROP SOURCE demo CASCADE
