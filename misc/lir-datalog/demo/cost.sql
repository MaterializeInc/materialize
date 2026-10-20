-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- CPU time and arrangement sizes of the demo's dataflows.
SELECT
    replace(c.dataflow_name, 'Dataflow: materialize.public.', '') AS dataflow,
    round(c.cpu_ms) AS cpu_ms,
    a.records AS arranged_records,
    pg_size_pretty(a.size) AS arranged_size
FROM (
    SELECT d.dataflow_name, sum(e.elapsed_ns) / 1e6 AS cpu_ms
    FROM mz_introspection.mz_scheduling_elapsed e
    JOIN mz_introspection.mz_dataflow_operator_dataflows d ON d.id = e.id
    GROUP BY d.dataflow_name
) c
JOIN mz_introspection.mz_dataflow_arrangement_sizes a ON a.name = c.dataflow_name
WHERE c.dataflow_name LIKE '%flagged_transfers%'
ORDER BY 1;
