-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.


-- result of individual product limits scenarios
CREATE TABLE cluster_spec_sheet_result (
   build_job_id TEXT NOT NULL,
   framework_version TEXT NOT NULL,
   scenario TEXT NOT NULL,
   scenario_version TEXT NOT NULL,
   scale INT NOT NULL,
   mode TEXT NOT NULL,
   category TEXT NOT NULL,
   test_name TEXT NOT NULL,
   cluster_size TEXT NOT NULL,
   repetition INT NOT NULL,
   size_bytes BIGINT NOT NULL,
   time_ms BIGINT NOT NULL
);

ALTER TABLE cluster_spec_sheet_result OWNER TO qa;
GRANT SELECT, INSERT, UPDATE ON TABLE cluster_spec_sheet_result TO "hetzner-ci";
