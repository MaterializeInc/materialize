-- Copyright Materialize, Inc. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Get time spent in each Worker
SELECT mz_cluster_id() AS mz_cluster_id,
       mz_logical_timestamp()::float AS mz_logical_timestamp,
       mz_scheduling_elapsed.worker AS mz_scheduling_worker_id,
       sum(mz_scheduling_elapsed.elapsed_ns)::bigint AS mz_scheduling_elapsed_ns
FROM mz_scheduling_elapsed
GROUP BY mz_scheduling_elapsed.worker
ORDER BY mz_scheduling_elapsed.worker ASC;
