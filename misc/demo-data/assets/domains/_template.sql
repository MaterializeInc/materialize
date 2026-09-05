-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- =============================================================================
-- TEMPLATE — copy to <yourdomain>.sql, fill in the TODOs, delete this header.
--
-- This file is a fill-in-the-blank skeleton for a new domain. It encodes the
-- four design rules from SKILL.md:
--
--   1. PKs derived deterministically from `random` bytes
--   2. FKs by re-derivation (re-hash parent random || child index)
--   3. Distributions by byte mask
--   4. Time-relative fields via `moment + interval`
--
-- Prerequisites: scaffold.sql (always). Add `\i common/people.sql` to the
-- prereq list if your domain references people (recommended for cross-domain
-- demos).
-- =============================================================================

-- TODO: replace `domain` with your domain's name (e.g. `trip`, `shipment`).
SELECT EXISTS (SELECT 1 FROM mz_views WHERE name = 'TODO_core') AS already_loaded \gset
\if :already_loaded
\echo TODO domain already loaded; skipping.
\else

-- -----------------------------------------------------------------------------
-- (Optional) Static lookups. Use for small enumerations: product types,
-- statuses, depots, channels. Skip if not needed.
-- -----------------------------------------------------------------------------
-- CREATE VIEW TODO_kinds (id, name) AS VALUES
--     (0, 'Kind A'),
--     (1, 'Kind B'),
--     (2, 'Kind C');

-- -----------------------------------------------------------------------------
-- The `_core` view: raw fields extracted directly from random bytes.
--
-- Byte budget (16 bytes available; allocate by cardinality):
--   [0..2] entity id              24-bit → ~16M space
--   [3]    foreign key to people  mod 256
--   [4]    lookup index           mod N
--   [5]    quantity / count       1 + mod(., K) for fanout
--   [6]    minor field / jitter
--   [7..]  free
--
-- Edit the SELECT below to match your byte plan.
-- -----------------------------------------------------------------------------
CREATE VIEW TODO_core AS
SELECT
    moment,
    random,
    get_byte(random, 0) +
    get_byte(random, 1) * 256 +
    get_byte(random, 2) * 65536                                  AS id,
    get_byte(random, 3)                                          AS person_id,        -- FK into people
    mod(get_byte(random, 4)::int, 3)                             AS kind_id,           -- FK into TODO_kinds
    1 + mod(get_byte(random, 5)::int, 5)                         AS n_children,        -- fanout for child rows
    moment + (get_byte(random, 6)::text || ' minutes')::interval AS due_at             -- time-relative field
FROM random;

-- -----------------------------------------------------------------------------
-- The public top-level view. Materialize and join lookups for fast reads.
-- This is what demos and validation queries hit.
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW TODO AS
SELECT
    c.id,
    c.person_id,
    p.name                                                       AS person_name,
    -- k.name                                                       AS kind,
    c.moment                                                     AS started_at,
    c.due_at
FROM TODO_core c
JOIN people p ON p.id = c.person_id;
-- LEFT JOIN TODO_kinds k ON k.id = c.kind_id;

-- -----------------------------------------------------------------------------
-- (Optional) Child rows via fanout. The pattern:
--   1. generate_series(1, parent.n_children) to expand
--   2. digest(parent.random || child_index) to re-hash for each child
--   3. extract child fields from the new random
--
-- This is rule #2 — FK by re-derivation. When the parent moment falls out
-- of retention, every child generated from it vanishes simultaneously.
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW TODO_children AS
WITH expanded AS (
    SELECT
        id AS parent_id,
        moment AS parent_moment,
        generate_series(1, n_children) AS child_no,
        digest(random::text || generate_series(1, n_children)::text, 'md5') AS random
    FROM TODO_core
)
SELECT
    parent_id,
    child_no,
    get_byte(random, 0)                                          AS field_a,
    get_byte(random, 1) + get_byte(random, 2) * 256              AS field_b,
    parent_moment
        + (child_no::text || ' seconds')::interval               AS occurred_at
FROM expanded;

-- -----------------------------------------------------------------------------
-- (Optional) Aggregates. The classic Materialize moment: a view that stays
-- correct as data flows in. Group by some FK and sum/count/max.
-- -----------------------------------------------------------------------------
CREATE VIEW TODO_rollup AS
SELECT
    parent_id,
    COUNT(*)         AS n,
    SUM(field_a)     AS total_a,
    MAX(field_b)     AS max_b
FROM TODO_children
GROUP BY parent_id;

\endif

-- -----------------------------------------------------------------------------
-- Validation queries — paste these into a session to confirm the domain works.
-- -----------------------------------------------------------------------------
--
-- Heartbeat — should tick continuously:
--   COPY (SUBSCRIBE (SELECT COUNT(*) FROM TODO) WITH (progress = true)) TO STDOUT;
--
-- Invariant: every child references an existing parent (should be 0).
--   SELECT COUNT(*) FROM TODO_children c
--   LEFT JOIN TODO t ON t.id = c.parent_id WHERE t.id IS NULL;
--
-- Invariant: total child rows = total declared fanout (aggregate form).
-- A per-parent version of this looks tempting but fails because the 24-bit
-- random `id` space has birthday collisions at 86k+ rows — see SKILL.md
-- "Invariants vs. id collisions". The aggregate form is unaffected.
--   SELECT (SELECT COUNT(*)      FROM TODO_children) =
--          (SELECT SUM(n_children) FROM TODO_core)  AS fanout_balances;
--
-- Add at least one *domain-specific* invariant of your own — that's what
-- makes a demo land. Pick a shape that survives id collisions: sum-to-zero
-- over all rows, total count = declared total, monotone timestamps,
-- mutually-exclusive states, subset relationships (X ⊆ Y). Avoid invariants
-- that depend on synthetic ids being unique.
-- -----------------------------------------------------------------------------
