-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.


CREATE TABLE upgrade_downtime_result (
   build_job_id TEXT NOT NULL,
   framework_version TEXT NOT NULL,
   scenario TEXT NOT NULL,
   scenario_version TEXT NOT NULL,
   downtime_initial TEXT NOT NULL,
   downtime_upgrade TEXT NOT NULL
);

ALTER TABLE upgrade_downtime_result OWNER TO qa;
GRANT SELECT, INSERT, UPDATE ON TABLE upgrade_downtime_result TO "hetzner-ci";
