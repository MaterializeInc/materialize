-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- TPC-H Query #Q18, chosen for the workload for its GROUP BY clause
-- We have modified the predicate to sum(l_quantity) > 250
-- to ensure the result updates frequently

-- depends_on: {{ ref('tpch') }}
{{ config(materialized='materializedview', indexes=[{'default': True}]) }}

SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
FROM
    {{ source('tpch','customer') }} ,
    {{ source('tpch','orders') }} ,
    {{ source('tpch','lineitem') }}
WHERE
    o_orderkey IN (
        SELECT
            l_orderkey
        FROM
            {{ source('tpch','lineitem') }}
        GROUP BY
            l_orderkey having
                sum(l_quantity) > 250
    )
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
