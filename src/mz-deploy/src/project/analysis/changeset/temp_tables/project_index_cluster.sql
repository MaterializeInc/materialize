-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- `IN CLUSTER` on a CREATE INDEX attached to an object. No rule reads
-- `idx`; it is kept so the fact carries the index's identity.
--
-- Column semantics and what populates this table are documented in the
-- header of `../dirty_propagation.sql`, which owns the fact contract.

CREATE TEMPORARY TABLE project_index_cluster (
    db text, sch text, obj text, idx text, cluster text
)
