-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- TPC-H Query #Q01, chosen for the workload for its GROUP BY clause
CREATE MATERIALIZED VIEW tpch_q01
    IN CLUSTER qa_canary_environment_compute
    AS
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
        count(*) AS count_order
    FROM qa_canary_environment.public_tpch_sources.tpch_lineitem
    WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '60' day
    GROUP BY l_returnflag, l_linestatus
    ORDER BY l_returnflag, l_linestatus;

CREATE DEFAULT INDEX IN CLUSTER qa_canary_environment_compute ON tpch_q01;

GRANT ALL PRIVILEGES ON TABLE qa_canary_environment.public_tpch.tpch_q01 TO "infra+bot@materialize.com", "infra+qacanaryload@materialize.io";
