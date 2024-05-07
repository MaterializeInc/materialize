# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_unorchestrated_cluster_replicas = true;

> DROP CLUSTER IF EXISTS prod CASCADE
> DROP CLUSTER IF EXISTS prod_deploy CASCADE
> CREATE CLUSTER prod REPLICAS (replica1 (
  STORAGECTL ADDRESSES ['clusterd1:2100'],
  STORAGE ADDRESSES ['clusterd1:2103'],
  COMPUTECTL ADDRESSES ['clusterd1:2101'],
  COMPUTE ADDRESSES ['clusterd1:2102'],
  WORKERS 1))
> CREATE CLUSTER prod_deploy REPLICAS (replica1 (
  STORAGECTL ADDRESSES ['clusterd2:2100', 'clusterd3:2100'],
  STORAGE ADDRESSES ['clusterd2:2103', 'clusterd3:2103'],
  COMPUTECTL ADDRESSES ['clusterd2:2101', 'clusterd3:2101'],
  COMPUTE ADDRESSES ['clusterd2:2102', 'clusterd3:2102'],
  WORKERS 2))
> DROP SCHEMA IF EXISTS prod CASCADE
> DROP SCHEMA IF EXISTS prod_deploy CASCADE

# For now sources are not considered in blue-green deployments, so create them separately
> DROP SOURCE IF EXISTS counter CASCADE
> CREATE SOURCE counter FROM LOAD GENERATOR counter (TICK INTERVAL '1s')

> DROP SOURCE IF EXISTS tpch CASCADE
> CREATE SOURCE tpch
  FROM LOAD GENERATOR TPCH (SCALE FACTOR 0.1)
  FOR ALL TABLES

> CREATE SCHEMA prod
> CREATE MATERIALIZED VIEW prod.counter_mv IN CLUSTER prod AS SELECT count(*) FROM counter
> CREATE MATERIALIZED VIEW prod.counter_mv2 IN CLUSTER prod_deploy WITH (REFRESH EVERY '60 seconds') AS SELECT count(*), 'some new value' FROM counter
> CREATE DEFAULT INDEX IN CLUSTER prod ON counter
> CREATE MATERIALIZED VIEW prod.tpch_mv
  IN CLUSTER prod AS
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
    'old' as state
  FROM
    lineitem
  WHERE
    l_shipdate <= date '1998-12-01' - interval '90' day
  GROUP BY
    l_returnflag,
    l_linestatus
  ORDER BY
    l_returnflag,
    l_linestatus
> CREATE DEFAULT INDEX IN CLUSTER prod ON prod.tpch_mv

> WITH
    dataflows AS (
        SELECT mz_indexes.id
        FROM mz_indexes
        JOIN mz_clusters ON mz_indexes.cluster_id = mz_clusters.id
        WHERE mz_clusters.name = 'prod'
        UNION ALL
        SELECT mz_materialized_views.id
        FROM mz_materialized_views
        JOIN mz_clusters ON mz_materialized_views.cluster_id = mz_clusters.id
        WHERE mz_clusters.name = 'prod'
    ),
    -- Collect ready dataflows.
    -- For a dataflow to be ready it must be hydrated and caught up.
    -- We define a dataflow to be caught up if its local lag is less than 1 second.
    ready_dataflows AS (
        SELECT id
        FROM dataflows d
        JOIN mz_internal.mz_compute_hydration_statuses h ON (h.object_id = d.id)
        -- Left join because some dataflows don't have dependencies and therefore
        -- don't have lag either.
        LEFT JOIN mz_internal.mz_materialization_lag l ON (l.object_id = d.id)
        WHERE
            h.hydrated AND
            (l.local_lag <= '1s' OR l.local_lag IS NULL)
    ),
    -- Collect dataflows that are not yet ready.
    pending_dataflows AS (
        SELECT id FROM dataflows
        EXCEPT
        SELECT id FROM ready_dataflows
    )
  SELECT * FROM pending_dataflows

> SELECT state FROM prod.tpch_mv LIMIT 1
old
