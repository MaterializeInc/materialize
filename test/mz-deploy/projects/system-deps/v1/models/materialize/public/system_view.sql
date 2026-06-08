-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Exercises the dependency resolver's 2-part system-schema path:
-- references mz_catalog.mz_objects without prepending a database.
CREATE VIEW system_view AS
    SELECT id, name, type
    FROM mz_catalog.mz_objects
    WHERE type = 'view';
