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

-- depends_on: {{ ref('pg_cdc') }}
-- depends_on: {{ ref('pg_people') }}
-- depends_on: {{ ref('pg_relationships') }}
{{ config(materialized='materialized_view', cluster="qa_canary_environment_compute", indexes=[{'default': True}]) }}

WITH MUTUALLY RECURSIVE
    symm (a int, b int) AS (
        SELECT a, b FROM {{ source('pg_cdc','pg_relationships') }}
        UNION
        SELECT b, a FROM {{ source('pg_cdc','pg_relationships') }}
    ),
    candidates (a int, b int, degree int) AS (
        SELECT a, b, 1 FROM symm
        UNION
        SELECT symm.a, reach.b, reach.degree + 1
        FROM symm, reach
        WHERE symm.b = reach.a
    ),
    reach(a int, b int, degree int) AS (
        SELECT a, b, min(degree) FROM candidates group by a, b HAVING a != b
    )
SELECT DISTINCT a_people.name AS a_name, b_people.name AS b_name, degree
FROM reach
LEFT JOIN {{ source('pg_cdc','pg_people') }} AS a_people ON (a = a_people.id)
LEFT JOIN {{ source('pg_cdc','pg_people') }} AS b_people ON (b = b_people.id)
WHERE degree >= 2
