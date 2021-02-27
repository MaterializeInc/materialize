-- Copyright Materialize, Inc. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Get time spent in each Operator per Worker
SELECT mz_cluster_id() AS mz_cluster_id,
       mz_logical_timestamp()::float AS mz_logical_timestamp,
       mz_dataflow_operators.id AS mz_dataflow_operator_id,
       mz_dataflow_operators.worker AS mz_dataflow_operator_worker,
       mz_dataflow_operators.name AS mz_dataflow_operator_name,
       mz_scheduling_elapsed.elapsed_ns mz_scheduling_elapsed_ns
FROM mz_scheduling_elapsed,
     mz_dataflow_operators
WHERE mz_scheduling_elapsed.id = mz_dataflow_operators.id
  AND mz_scheduling_elapsed.worker = mz_dataflow_operators.worker
ORDER BY mz_dataflow_operators.id ASC;
