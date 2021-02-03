-- Copyright Materialize, Inc. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License in the LICENSE file at the
-- root of this repository, or online at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

{{ config(materialized='materializedview') }}

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
FROM (SELECT data::jsonb AS val FROM {{ source('wikimedia', 'wikirecent') }})
