-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- TPC-H Query #Q18, chosen for the workload for its GROUP BY clause.
-- The predicate sum(l_quantity) > 250 ensures the result updates frequently.
CREATE MATERIALIZED VIEW tpch_q18
    IN CLUSTER qa_canary_environment_compute
    AS
    SELECT
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice,
        sum(l_quantity)
    FROM
        qa_canary_environment.public_tpch_sources.tpch_customer,
        qa_canary_environment.public_tpch_sources.tpch_orders,
        qa_canary_environment.public_tpch_sources.tpch_lineitem
    WHERE
        o_orderkey IN (
            SELECT l_orderkey
            FROM qa_canary_environment.public_tpch_sources.tpch_lineitem
            GROUP BY l_orderkey HAVING sum(l_quantity) > 250
        )
        AND c_custkey = o_custkey
        AND o_orderkey = l_orderkey
    GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
    ORDER BY o_totalprice DESC, o_orderdate;

CREATE DEFAULT INDEX IN CLUSTER qa_canary_environment_compute ON tpch_q18;

GRANT ALL PRIVILEGES ON TABLE qa_canary_environment.public_tpch.tpch_q18 TO "infra+bot@materialize.com", "infra+qacanaryload@materialize.io";
