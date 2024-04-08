-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Get all system objects matching a given pattern.
SELECT
  o.id as id,
  o.oid as oid,
  o.type as type,
  o.name as name,
  s.name as schema,
  'mz' as database
FROM
  mz_objects AS o JOIN
  mz_schemas AS s ON (o.schema_id = s.id)
WHERE
  o.id like 's%' AND
  o.name ilike {name} AND
  s.name ilike {schema}
ORDER BY
  o.type,
  s.name,
  o.name;
