-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- CPU time and arrangement sizes of the demo's dataflows. The scheduling time of
-- an operator includes that of the operators nested in it, so the CPU time is
-- the time of each dataflow's root operator.
SELECT
    replace(df.name, 'Dataflow: materialize.public.', '') AS dataflow,
    round(e.elapsed_ns / 1e6) AS cpu_ms,
    s.records AS arranged_records,
    pg_size_pretty(s.size) AS arranged_size
FROM mz_introspection.mz_dataflows df
JOIN mz_introspection.mz_dataflow_addresses a ON a.address = list[df.id]
JOIN mz_introspection.mz_scheduling_elapsed e ON e.id = a.id
JOIN mz_introspection.mz_dataflow_arrangement_sizes s ON s.id = df.id
WHERE df.name LIKE '%flagged_transfers%'
ORDER BY 1;
