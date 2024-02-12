-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

select *
from mz_internal.mz_source_statuses
join mz_sources using (id)
join mz_schemas on (schema_id = mz_schemas.id)
join mz_databases on (database_id = mz_databases.id)
where mz_databases.name = 'qa_canary_environment'
and not (
  status = 'running' or
  (mz_sources.type = 'progress' and status = 'created') or
  (mz_sources.type = 'subsource' and status = 'starting')
)
