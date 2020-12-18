-- Copyright Materialize, Inc. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Get time spent in each Operator

-- Average the total time spent by each operator across all workers.
create view if not exists avg_elapsed_by_id as
select
    id,
    avg(elapsed_ns) as avg_ns
from
    mz_scheduling_elapsed
group by
    id;

-- Get operators where one worker has spent more than 2 times the average
-- amount of time spent. The number 2 can be changed according to the threshold
-- for the amount of skew deemed problematic.
select
    mse.id,
    dod.name,
    mse.worker,
    elapsed_ns,
    avg_ns,
    elapsed_ns/avg_ns as ratio
from
    mz_scheduling_elapsed mse,
    avg_elapsed_by_id aebi,
    mz_dataflow_operator_dataflows dod
where
    mse.id = aebi.id and
    mse.elapsed_ns > 2 * aebi.avg_ns and
    mse.id = dod.id
order by ratio desc;

drop view avg_elapsed_by_id;
