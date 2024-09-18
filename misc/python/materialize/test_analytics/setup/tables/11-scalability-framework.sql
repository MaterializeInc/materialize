-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.


-- result of individual workloads
CREATE TABLE scalability_framework_result (
   build_job_id TEXT NOT NULL,
   framework_version TEXT NOT NULL,
   workload_name TEXT NOT NULL,
   workload_group TEXT NOT NULL,
   workload_version TEXT NOT NULL,
   concurrency INT NOT NULL,
   count INT,
   tps DOUBLE
);

ALTER TABLE scalability_framework_result OWNER TO qa;
GRANT SELECT, INSERT, UPDATE ON TABLE scalability_framework_result TO "hetzner-ci";
