# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SCHEMA prod_deploy
> CREATE MATERIALIZED VIEW prod_deploy.counter_mv IN CLUSTER prod_deploy AS SELECT count(*), 'some new value' FROM counter_tbl
> CREATE MATERIALIZED VIEW prod_deploy.counter_mv2 IN CLUSTER prod_deploy WITH (REFRESH EVERY '60 seconds') AS SELECT count(*), 'some new value' FROM counter_tbl
> CREATE DEFAULT INDEX IN CLUSTER prod_deploy ON counter
> CREATE DEFAULT INDEX IN CLUSTER prod_deploy ON prod_deploy.counter_mv
> CREATE MATERIALIZED VIEW prod_deploy.tpch_mv
  IN CLUSTER prod_deploy AS
  SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order,
    'new' as state
  FROM
    lineitem
  WHERE
    l_shipdate <= date '1998-12-01' - interval '100' day
  GROUP BY
    l_returnflag,
    l_linestatus
  ORDER BY
    l_returnflag,
    l_linestatus
> CREATE DEFAULT INDEX IN CLUSTER prod_deploy ON prod_deploy.tpch_mv

> WITH
    dataflows AS (
        SELECT mz_indexes.id
        FROM mz_indexes
        JOIN mz_clusters ON mz_indexes.cluster_id = mz_clusters.id
        WHERE mz_clusters.name = 'prod_deploy'
        UNION ALL
        SELECT mz_materialized_views.id
        FROM mz_materialized_views
        JOIN mz_clusters ON mz_materialized_views.cluster_id = mz_clusters.id
        WHERE mz_clusters.name = 'prod_deploy'
    ),
    -- Collect ready dataflows.
    -- For a dataflow to be ready it must be hydrated and caught up.
    -- We define a dataflow to be caught up if its local lag is less than 4 seconds.
    ready_dataflows AS (
        SELECT id
        FROM dataflows d
        JOIN mz_internal.mz_compute_hydration_statuses h ON (h.object_id = d.id)
        -- Left join because some dataflows don't have dependencies and therefore
        -- don't have lag either.
        LEFT JOIN mz_internal.mz_materialization_lag l ON (l.object_id = d.id)
        WHERE
            h.hydrated AND
            (l.local_lag <= '4s' OR l.local_lag IS NULL)
    ),
    -- Collect dataflows that are not yet ready.
    pending_dataflows AS (
        SELECT id FROM dataflows
        EXCEPT
        SELECT id FROM ready_dataflows
    )
  SELECT * FROM pending_dataflows

> BEGIN
> ALTER SCHEMA prod SWAP WITH prod_deploy
> ALTER CLUSTER prod SWAP WITH prod_deploy
> COMMIT

# Give worker a chance to finish current query
> SELECT mz_unsafe.mz_sleep(10)
<null>

> DROP CLUSTER prod_deploy CASCADE
> DROP SCHEMA prod_deploy CASCADE

> SELECT state FROM prod.tpch_mv LIMIT 1
new
