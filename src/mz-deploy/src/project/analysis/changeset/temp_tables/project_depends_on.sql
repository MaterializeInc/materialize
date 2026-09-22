-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- The child references the parent in its query. A parent outside the
-- project, such as a system catalog relation, has `parent_db = ''` and no
-- `project_object` row.
--
-- Column semantics and what populates this table are documented in the
-- header of `../dirty_propagation.sql`, which owns the fact contract.

CREATE TEMPORARY TABLE project_depends_on (
    child_db text, child_sch text, child_obj text,
    parent_db text, parent_sch text, parent_obj text
)
