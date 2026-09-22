-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Every object in the compiled project, tagged with its statement kind.
-- `kind` drives IsSink and IsApplyManaged in `dirty_propagation.sql`.
--
-- Column semantics and what populates this table are documented in the
-- header of `../dirty_propagation.sql`, which owns the fact contract.

CREATE TEMPORARY TABLE project_object (
    db text, sch text, obj text, kind text
)
