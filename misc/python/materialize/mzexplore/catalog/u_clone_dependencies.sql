-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Starting from a set of `source_ids` that share the same `cluster_id`, find
-- all catalog objects that need to be cloned in order to perform a runtime
-- experiment to asses the impact plan changes to the source_ids.
--
-- The returned set of objects is the union of these cases:
-- 1. The original set of source_ids themselves.
-- 2. All source_ids representing transitive dependencies of (1) reachable
--    through paths consisting only of regular views.
-- 3. All indexes on objects adjacent to (2) that are in the same cluster.
WITH
  -- Pre-define all direct dependencies on views.
  direct_view_dependencies(source_id, target_id) AS (
    SELECT
      od.object_id, od.referenced_object_id
    FROM
      mz_internal.mz_object_dependencies od
    WHERE
      od.object_id IN (SELECT id FROM mz_catalog.mz_views) AND
      od.referenced_object_id ilike 'u%'
  ),
  -- Find the transitive dependencies reachable along view paths for all source_ids.
  transitive_dependencies(source_id, target_id) AS (
    WITH MUTUALLY RECURSIVE
      reach(source_id text, target_id text) AS (
        -- Start from requested source_ids.
        SELECT object_id, referenced_object_id FROM mz_internal.mz_object_dependencies WHERE object_id IN ({source_ids})

        UNION

        -- In each iteration, enrich with adjacent direct_view_dependencies.
        SELECT x, z FROM reach r(x, y) JOIN direct_view_dependencies d(y, z) USING(y)
      )
    SELECT source_id, target_id FROM reach
  ),
  -- Find indexes on transitive_dependencies targets that:
  -- 1. would be available for at least one of the requested source_ids, and
  -- 2. live in the requested cluster.
  dependency_indexes(id) AS (
    SELECT
      id
    FROM
      mz_catalog.mz_indexes i JOIN
      transitive_dependencies d ON (i.on_id = d.target_id)
    WHERE
      substring(id from 2)::bigint <= (SELECT max(substring(id from 2)::bigint) FROM unnest(array[{source_ids}]) AS src(id)) AND -- (1)
      cluster_id = {cluster_id} -- (2)
  ),
  -- Union all relevant dependencies.
  all_dependencies(id) AS (
    SELECT source_id FROM transitive_dependencies
    UNION
    SELECT target_id FROM transitive_dependencies WHERE target_id IN (SELECT id FROM mz_catalog.mz_views)
    UNION
    SELECT id FROM dependency_indexes
  )
  -- Return enriched and ordered all_dependencies info.
  SELECT
    o.id as id,
    o.name as name,
    s.name as schema,
    d.name as database,
    o.type as type
  FROM
    mz_catalog.mz_objects AS o JOIN
    mz_catalog.mz_schemas AS s ON (o.schema_id = s.id) JOIN
    mz_catalog.mz_databases AS d ON (s.database_id = d.id)
  WHERE
    o.id IN (SELECT id FROM all_dependencies)
  ORDER BY
    substring(o.id from 2)::bigint ASC;
