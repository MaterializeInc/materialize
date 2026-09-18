-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- =============================================================================
-- Auctions & Bids. The canonical example from the blog post.
--
-- Demonstrates:
--   * deterministic PK derivation from moment hash
--   * FK by re-derivation: bids re-hash their auction's random bytes, so when
--     an auction's moment falls out of the retention window, its bids vanish
--     with it. Referential integrity for free.
--   * temporal lifecycle: auctions have an end_time computed from random bytes.
--
-- Standalone domain: does not reference `people`. (The seller/buyer ids are a
-- larger space than the shared pool, by design — auctions are typically a
-- long-tail marketplace.)
--
-- Load with:  \i scaffold.sql
--             \i domains/auctions.sql
-- =============================================================================

-- Item-type lookup. Five categories cycled via `auction.item % 5`.
CREATE VIEW items (id, item) AS VALUES
    (0, 'Signed Memorabilia'),
    (1, 'City Bar Crawl'),
    (2, 'Best Pizza in Town'),
    (3, 'Gift Basket'),
    (4, 'Custom Art');

-- Raw auction stream. One auction per moment.
-- Byte budget for `random` (16 bytes):
--   [0..2] id (24-bit ⇒ ~16M space, sparse → unique with high prob)
--   [3..4] seller (16-bit ⇒ 65k sellers)
--   [5]    item type (used both as item lookup and as bid-count fanout below)
--   [6]    auction duration in minutes (0..255)
CREATE VIEW auctions_core AS
SELECT
    moment,
    random,
    get_byte(random, 0) +
    get_byte(random, 1) * 256 +
    get_byte(random, 2) * 65536                                  AS id,
    get_byte(random, 3) +
    get_byte(random, 4) * 256                                    AS seller,
    get_byte(random, 5)                                          AS item,
    moment + (get_byte(random, 6)::text || ' minutes')::interval AS end_time
FROM random;

-- The published auctions table — joined to items, materialized for fast joins.
CREATE MATERIALIZED VIEW auctions AS
SELECT auctions_core.id, seller, items.item, end_time
FROM auctions_core, items
WHERE auctions_core.item % 5 = items.id;

-- Bids. Each auction spawns up to 255 bids via `generate_series`, where the
-- number is itself a random byte (item / bid-count share a byte by design —
-- popular categories get more bids).
--
-- Each bid re-hashes (auction.random || bid_index) to get its own 16 bytes.
CREATE MATERIALIZED VIEW bids AS
WITH prework AS (
    SELECT
        id          AS auction_id,
        moment      AS auction_start,
        end_time    AS auction_end,
        digest(random::text || generate_series(1, get_byte(random, 5))::text, 'md5') AS random
    FROM auctions_core
)
SELECT
    get_byte(random, 0) +
    get_byte(random, 1) * 256 +
    get_byte(random, 2) * 65536                                          AS id,
    get_byte(random, 3) +
    get_byte(random, 4) * 256                                            AS buyer,
    auction_id,
    get_byte(random, 5)::numeric                                         AS amount,
    auction_start + (get_byte(random, 6)::text || ' minutes')::interval  AS bid_time
FROM prework;

-- -----------------------------------------------------------------------------
-- Validation. Paste either of these into a fresh session to confirm liveness:
-- -----------------------------------------------------------------------------
--
-- Heartbeat — should tick continuously, count stays near retention/tick:
--   COPY (SUBSCRIBE (SELECT COUNT(*) FROM auctions) WITH (progress = true)) TO STDOUT;
--
-- Winning bid per auction (the classic Materialize demo):
--   SELECT auction_id, MAX(amount) AS winning_bid
--   FROM bids GROUP BY auction_id;
--
-- Invariant: every bid references an existing auction (FK by re-derivation).
-- Should always return 0.
--   SELECT COUNT(*) FROM bids b
--   LEFT JOIN auctions a ON b.auction_id = a.id WHERE a.id IS NULL;
--
-- Note on bid_time: the bid's time offset and the auction's duration come
-- from INDEPENDENT random bytes, so ~50% of bids fire after their auction
-- ends. That's intentional — late-bid attempts are realistic, and showing
-- Materialize handle them as a "filter to live auctions" view is a fine
-- demo on its own.
