# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for https://github.com/MaterializeInc/database-issues/issues/6335.
#
# This test confirms that subscribes that advance to the empty frontier are
# fully cleaned up.
#
# This test relies on testdrive's automatic retries, since it queries
# introspection sources that take a while to update.

$ set-sql-timeout duration=120s

# This test uses introspection queries that need to be targeted to a replica
> SET cluster_replica = r1

# Create a collection with an empty frontier to subscribe from.
> CREATE MATERIALIZED VIEW mv AS SELECT 1 AS a

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE mv
$ set-regex match=0|\d{13,20} replacement=<TIMESTAMP>
> FETCH c
<TIMESTAMP> 1 1
$ unset-regex
> COMMIT

# To ensure that the subscribe has reached the introspection sources, we
# install another dataflow and wait for that to show up. Introspection sources
# are not serializable, but dataflows still show up in order.
> CREATE INDEX mv_idx ON mv(a)
> SELECT count(*)
  FROM
    mz_indexes i,
    mz_introspection.mz_compute_exports e
  WHERE
    i.name = 'mv_idx' AND
    e.export_id = i.id
1

> SELECT count(*) FROM mz_internal.mz_subscriptions
0

> SELECT count(*)
  FROM mz_introspection.mz_compute_exports e
  JOIN mz_introspection.mz_dataflows d ON d.id = e.dataflow_id
  WHERE e.export_id LIKE 't%' AND
        d.name NOT LIKE '%introspection-subscribe%'
0
