-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE OR REPLACE VIEW v_output_consistency_stats AS
SELECT
    b.build_number,
    b.branch,
    b.date,
    ocs.*
FROM output_consistency_stats ocs
INNER JOIN build_job bj
ON ocs.build_job_id = bj.build_job_id
INNER JOIN build b
ON bj.build_id = b.build_id;

ALTER VIEW v_output_consistency_stats OWNER TO qa;
