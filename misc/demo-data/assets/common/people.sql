-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- =============================================================================
-- Shared identity pool: 256 deterministic people, used as customers / holders
-- / users across multiple domains. Letting ecommerce + banking + clickstream
-- all reference the same id space is the "data products staying in sync" demo:
-- "show me Person 042's orders and her recent transactions, live."
--
-- 256 is chosen so a single `get_byte(random, N)` picks a person uniformly
-- without modulus.
--
-- Static (not moment-driven). Names/emails are deterministic for repeatability.
--
-- Load with:   \i common/people.sql   (after scaffold.sql)
-- =============================================================================

SELECT EXISTS (SELECT 1 FROM mz_views WHERE name = 'people') AS already_loaded \gset
\if :already_loaded
\echo people already loaded; skipping.
\else

CREATE VIEW people AS
SELECT
    id::int                                                            AS id,
    'Person ' || lpad(id::text, 3, '0')                                AS name,
    'person' || lpad(id::text, 3, '0') || '@example.com'               AS email,
    (ARRAY['US-W','US-E','EU','APAC','LATAM'])[1 + mod(id, 5)]         AS region,
    digest('person:' || id::text, 'md5')                               AS attrs
FROM generate_series(0, 255) AS id;

CREATE DEFAULT INDEX ON people;

\endif
