-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- =============================================================================
-- Banking: accounts + double-entry transactions
--
-- Demonstrates:
--   * the "invariant by construction" pattern: every transaction emits TWO
--     ledger entries summing to zero, so SUM(entries.amount) is trivially 0
--     at every consistent timestamp. This is Materialize's headline correctness
--     property and is hard to demo with synthetic data any other way.
--   * cross-domain join to `people` for account holders
--
-- Prerequisites: scaffold.sql, common/people.sql
-- =============================================================================

-- Static account directory: 64 accounts. Each held by someone from `people`.
-- account_id is 0..63 so a single byte mod 64 picks one uniformly.
CREATE VIEW accounts AS
SELECT
    id::int                                                      AS id,
    'ACCT-' || lpad(id::text, 4, '0')                            AS number,
    -- Hash the account id with a salt to pick a holder, so the assignment
    -- isn't a trivial id == holder_id mapping.
    get_byte(digest('acct:' || id::text, 'md5'), 0)              AS holder_id,
    (ARRAY['Checking','Savings','Credit'])[1 + mod(id, 3)]       AS account_type
FROM generate_series(0, 63) AS id;

CREATE DEFAULT INDEX ON accounts;

-- One transaction per moment.
-- Byte budget:
--   [0..2] transaction_id
--   [3]    from_account, mod 64
--   [4]    to_account: derived from `from` + offset in [1..63] (mod 64) so the
--          two accounts are guaranteed distinct.
--   [5..6] amount in cents (16-bit ⇒ up to $655.35 per txn)
CREATE VIEW transactions_core AS
SELECT
    moment,
    random,
    get_byte(random, 0) +
    get_byte(random, 1) * 256 +
    get_byte(random, 2) * 65536                                  AS id,
    mod(get_byte(random, 3)::int, 64)                            AS from_account,
    mod(
        mod(get_byte(random, 3)::int, 64) +
        1 + mod(get_byte(random, 4)::int, 63),
        64
    )                                                            AS to_account,
    ((get_byte(random, 5) + get_byte(random, 6) * 256))::numeric
        / 100.0                                                  AS amount
FROM random;

CREATE MATERIALIZED VIEW transactions AS
SELECT
    t.id,
    t.moment    AS posted_at,
    t.from_account,
    t.to_account,
    t.amount
FROM transactions_core t;

-- Double-entry: each transaction emits two ledger rows that sum to zero.
-- generate_series(1,2) fans out each txn; we pick from vs. to by row index.
CREATE MATERIALIZED VIEW ledger_entries AS
WITH expanded AS (
    SELECT id, moment, from_account, to_account, amount,
           generate_series(1, 2) AS leg
    FROM transactions_core
)
SELECT
    id                                                       AS transaction_id,
    moment                                                   AS posted_at,
    CASE WHEN leg = 1 THEN from_account ELSE to_account END  AS account_id,
    CASE WHEN leg = 1 THEN -amount      ELSE  amount END     AS amount
FROM expanded;

-- Running balance per account. The headline view: stays correct under
-- arbitrary concurrent reads + writes because of strict serializability.
CREATE VIEW account_balances AS
SELECT
    a.id,
    a.number,
    a.account_type,
    p.name                                  AS holder_name,
    p.region                                AS holder_region,
    COALESCE(SUM(le.amount), 0)             AS balance
FROM accounts a
LEFT JOIN ledger_entries le ON le.account_id = a.id
LEFT JOIN people p ON p.id = a.holder_id
GROUP BY a.id, a.number, a.account_type, p.name, p.region;

-- -----------------------------------------------------------------------------
-- Validation:
--
-- Heartbeat (count of in-flight transactions):
--   COPY (SUBSCRIBE (SELECT COUNT(*) FROM transactions) WITH (progress = true)) TO STDOUT;
--
-- THE invariant: total of all balances is exactly zero. This is the demo:
-- you can hit this query repeatedly while millions of transactions land,
-- and it will return 0 every single time.
--   SELECT SUM(balance) FROM account_balances;
--
-- Per-holder net position (joins to people, useful for cross-domain demos):
--   SELECT holder_name, SUM(balance) FROM account_balances GROUP BY holder_name;
-- -----------------------------------------------------------------------------
