-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- A content hash per object in the project being deployed. Two objects hash
-- equal exactly when deploying them would produce the same thing. Excludes
-- the apply-managed kinds, so those never diff.
--
-- Column semantics and what populates this table are documented in the
-- header of `../dirty_propagation.sql`, which owns the fact contract.

CREATE TEMPORARY TABLE new_object (
    db text, sch text, obj text, hash text
)
