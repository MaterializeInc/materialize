-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Every object in the compiled project. `kind` is one of 'view',
-- 'materialized_view', 'sink', 'table', 'table_from_source', 'source',
-- 'secret', 'connection'. The rules derive IsSink and IsApplyManaged from it.

CREATE TEMPORARY TABLE project_object (
    db text, sch text, obj text, kind text
)
