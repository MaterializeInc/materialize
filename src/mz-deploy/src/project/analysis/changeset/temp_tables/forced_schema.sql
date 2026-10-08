-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Schemas the caller redeploys unconditionally: those named by
-- `--redeploy-schema`, or every project schema under `--redeploy-all`.

CREATE TEMPORARY TABLE forced_schema (
    db text, sch text
)
