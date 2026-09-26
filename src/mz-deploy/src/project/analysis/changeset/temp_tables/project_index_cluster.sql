-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- The `IN CLUSTER` of a CREATE INDEX on an object. No rule reads `idx`. It
-- keeps two indexes on the same cluster as distinct rows.

CREATE TEMPORARY TABLE project_index_cluster (
    db text, sch text, obj text, idx text, cluster text
)
