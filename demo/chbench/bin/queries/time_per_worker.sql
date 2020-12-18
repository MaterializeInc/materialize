-- Copyright Materialize, Inc. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Get time spent in each Operator
select mse.worker, sum(mse.elapsed_ns) as elapsed_ns
from mz_scheduling_elapsed as mse
group by mse.worker
order by mse.worker ASC;
