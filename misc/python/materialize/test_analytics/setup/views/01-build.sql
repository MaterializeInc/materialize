-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE OR REPLACE VIEW v_most_recent_build AS
SELECT
    b.branch,
    b.pipeline,
    max(b.build_number) AS highest_build_number
FROM build b
GROUP BY
    b.branch,
    b.pipeline
;

ALTER VIEW v_most_recent_build OWNER TO qa;
