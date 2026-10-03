-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- How each schema of the production deployment was deployed, read from the
-- deployment history. `kind` is one of 'objects', 'replacement', 'sinks',
-- 'tables'. The rules match only 'replacement'.

CREATE TEMPORARY TABLE old_schema_kind (
    db text, sch text, kind text
)
