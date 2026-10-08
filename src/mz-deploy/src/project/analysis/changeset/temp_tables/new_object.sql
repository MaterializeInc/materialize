-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- One content hash for each object in the compiled project. Two hashes are
-- equal exactly when deploying the two objects produces the same result.
-- Apply-managed kinds have no row, so they never read as changed.

CREATE TEMPORARY TABLE new_object (
    db text, sch text, obj text, hash text
)
