# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Change compute_replica_expiration to 31 days, should keep retractions

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_replica_expiration = 2678400000

> CREATE CLUSTER test (SIZE = '1')
> SET CLUSTER TO test

> CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
  );

> CREATE VIEW last_30_days AS
  SELECT event_ts, content
  FROM events
  WHERE mz_now() <= event_ts + INTERVAL '30 days';

> CREATE DEFAULT INDEX ON last_30_days;

> INSERT INTO events SELECT x::text, now() FROM generate_series(1, 1000) AS x;

> SELECT records FROM mz_introspection.mz_arrangement_sizes mas JOIN mz_introspection.mz_dataflow_operators mdo ON mas.operator_id = mdo.id AND name = 'ArrangeBy[[Column(0), Column(1)]]';
2000

> DROP VIEW last_30_days;

# compute_replica_expiration defaults to 22 days, so should have no need for
# retractions after a 30 day window

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_replica_expiration = 1900800000

> DROP CLUSTER test;
> CREATE CLUSTER test (SIZE = '1')
> SET CLUSTER TO test

> CREATE VIEW last_30_days AS
  SELECT event_ts, content
  FROM events
  WHERE mz_now() <= event_ts + INTERVAL '30 days';

> CREATE DEFAULT INDEX ON last_30_days;

> INSERT INTO events SELECT x::text, now() FROM generate_series(1, 1000) AS x;

> SELECT records FROM mz_introspection.mz_arrangement_sizes mas JOIN mz_introspection.mz_dataflow_operators mdo ON mas.operator_id = mdo.id AND name = 'ArrangeBy[[Column(0), Column(1)]]';
1000

> DROP VIEW last_30_days;
> DROP CLUSTER test;
