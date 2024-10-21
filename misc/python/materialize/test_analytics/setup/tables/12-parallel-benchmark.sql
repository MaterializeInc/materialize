-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.


-- result of individual benchmark scenarios
CREATE TABLE parallel_benchmark_result (
   build_job_id TEXT NOT NULL,
   framework_version TEXT NOT NULL,
   scenario_name TEXT NOT NULL,
   scenario_version TEXT NOT NULL,
   query TEXT NOT NULL,
   load_phase_duration INT,
   queries INT NOT NULL,
   qps DOUBLE NOT NULL,
   min DOUBLE NOT NULL,
   max DOUBLE NOT NULL,
   avg DOUBLE NOT NULL,
   p50 DOUBLE NOT NULL,
   p95 DOUBLE NOT NULL,
   p99 DOUBLE NOT NULL,
   p99_9 DOUBLE,
   p99_99 DOUBLE,
   p99_999 DOUBLE,
   p99_9999 DOUBLE,
   p99_99999 DOUBLE,
   p99_999999 DOUBLE,
   std DOUBLE,
   slope DOUBLE NOT NULL
);

ALTER TABLE parallel_benchmark_result OWNER TO qa;
GRANT SELECT, INSERT, UPDATE ON TABLE parallel_benchmark_result TO "hetzner-ci";
