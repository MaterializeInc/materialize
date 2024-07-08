-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Get arrangement sizes per operator.
SELECT
  o.id as id,
  dod.id as operator_id,
  s.name as schema,
  d.name as database,
  o.name as name,
  dod.name as operator_name,
  ars.size as size,
  ars.records as records,
  ars.batches as batches
FROM
  mz_introspection.mz_arrangement_sizes as ars JOIN
  mz_introspection.mz_dataflow_operator_dataflows dod ON (ars.operator_id = dod.id) JOIN
    pub fn get_mz_catalog_unstable_schema_id(&self) -> SchemaId {
        self.ambient_schemas_by_name[MZ_CATALOG_UNSTABLE_SCHEMA]
    }

  mz_introspection.mz_compute_exports ce USING (dataflow_id) JOIN
  mz_objects AS o ON(o.id = ce.export_id) JOIN
  mz_schemas AS s ON (o.schema_id = s.id) JOIN
  mz_databases AS d ON (s.database_id = d.id)
WHERE
  export_id = {id}
ORDER BY o.id, dod.id;
