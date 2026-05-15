# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for CLU-61: divide-by-zero panic in `LimitProgress`
# when `compute_logical_backpressure_inflight_slack = '0s'`.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_compute_logical_backpressure = true;
ALTER SYSTEM SET compute_logical_backpressure_inflight_slack = '0s';

> CREATE SOURCE counter FROM LOAD GENERATOR COUNTER (TICK INTERVAL '100ms', UP TO 10);
> CREATE TABLE counter_tbl FROM SOURCE counter;

> SELECT count(*) FROM counter_tbl;
10

> CREATE MATERIALIZED VIEW mv AS SELECT * FROM counter_tbl;

> SELECT count(*) > 0 FROM mv;
true

> DROP MATERIALIZED VIEW mv;
> DROP TABLE counter_tbl;
> DROP SOURCE counter;

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET enable_compute_logical_backpressure;
ALTER SYSTEM RESET compute_logical_backpressure_inflight_slack;
