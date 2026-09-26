-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- One content hash for each object in the production deployment, read from
-- the deployment history. Hashes are computed as for `new_object`, so an
-- object is unchanged exactly when its two hashes are equal. The table is
-- empty on a first deploy, so every object reads as added.

CREATE TEMPORARY TABLE old_object (
    db text, sch text, obj text, hash text
)
