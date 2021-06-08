-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE SOURCE IF NOT EXISTS wikirecent
FROM FILE '/tmp/wikidata/recentchanges' WITH (tail = true)
FORMAT REGEX '^data: (?P<data>.*)';

CREATE MATERIALIZED VIEW IF NOT EXISTS recentchanges AS
    SELECT
        val->>'$schema' AS r_schema,
        (val->'bot')::bool AS bot,
        val->>'comment' AS comment,
        (val->'id')::float::int AS id,
        (val->'length'->'new')::float::int AS length_new,
        (val->'length'->'old')::float::int AS length_old,
        val->'meta'->>'uri' AS meta_uri,
        val->'meta'->>'id' as meta_id,
        (val->'minor')::bool AS minor,
        (val->'namespace')::float AS namespace,
        val->>'parsedcomment' AS parsedcomment,
        (val->'revision'->'new')::float::int AS revision_new,
        (val->'revision'->'old')::float::int AS revision_old,
        val->>'server_name' AS server_name,
        (val->'server_script_path')::text AS server_script_path,
        val->>'server_url' AS server_url,
        (val->'timestamp')::float AS r_ts,
        val->>'title' AS title,
        val->>'type' AS type,
        val->>'user' AS user,
        val->>'wiki' AS wiki
    FROM (SELECT data::jsonb AS val FROM wikirecent);

CREATE MATERIALIZED VIEW IF NOT EXISTS counter AS
    SELECT COUNT(*) FROM recentchanges;

CREATE MATERIALIZED VIEW IF NOT EXISTS user_edits AS
    SELECT user, count(*) FROM recentchanges GROUP BY user;

CREATE MATERIALIZED VIEW IF NOT EXISTS top10 AS
    SELECT * FROM user_edits ORDER BY count DESC LIMIT 10;
