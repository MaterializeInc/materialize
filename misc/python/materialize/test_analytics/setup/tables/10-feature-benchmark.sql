-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.


-- result of individual benchmark scenarios
CREATE TABLE feature_benchmark_result (
   build_job_id TEXT NOT NULL,
   framework_version TEXT NOT NULL,
   scenario_name TEXT NOT NULL,
   scenario_group TEXT, -- nullable for now to not break earlier versions
   scenario_version TEXT NOT NULL,
   cycle INT, -- nullable for now to not break earlier versions
   scale TEXT NOT NULL,
   is_regression BOOL, -- nullable for now to not break earlier versions (introduced with data version 21)
   wallclock DOUBLE,
   messages INT,
   memory_mz DOUBLE,
   memory_clusterd DOUBLE,
   wallclock_min DOUBLE,
   wallclock_max DOUBLE,
   wallclock_mean DOUBLE,
   wallclock_variance DOUBLE
);

-- This table holds results of runs that were discarded.
CREATE TABLE feature_benchmark_discarded_result (
   build_job_id TEXT NOT NULL,
   scenario_name TEXT NOT NULL,
   cycle INT NOT NULL,
   is_regression BOOL, -- nullable for now to not break earlier versions (introduced with data version 21)
   wallclock DOUBLE,
   messages INT,
   memory_mz DOUBLE,
   memory_clusterd DOUBLE,
   wallclock_min DOUBLE,
   wallclock_max DOUBLE,
   wallclock_mean DOUBLE,
   wallclock_variance DOUBLE
);

ALTER TABLE feature_benchmark_result OWNER TO qa;
ALTER TABLE feature_benchmark_discarded_result OWNER TO qa;
GRANT SELECT, INSERT, UPDATE ON TABLE feature_benchmark_result TO "hetzner-ci";
GRANT SELECT, INSERT, UPDATE ON TABLE feature_benchmark_discarded_result TO "hetzner-ci";
