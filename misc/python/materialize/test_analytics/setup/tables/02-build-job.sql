-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- meta data of the build step
CREATE TABLE build_job (
    -- build_job_id is assumed to be globally unique (build_step_id is reused on shared and retried build jobs)
    build_job_id TEXT NOT NULL,
    build_step_id TEXT NOT NULL,
    build_id TEXT NOT NULL,
    build_step_key TEXT NOT NULL,
    shard_index UINT4,
    retry_count UINT4 NOT NULL,
    insert_date TIMESTAMPTZ NOT NULL,
    is_latest_retry BOOL NOT NULL,
    success BOOL NOT NULL,
    aws_instance_type TEXT NOT NULL,
    remarks TEXT
);
