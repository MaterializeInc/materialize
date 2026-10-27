-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Recursive query over the pg_relationships graph; degree >= 2 keeps the
-- result churning. MOD() in the upstream load keeps the graph from exploding.
CREATE MATERIALIZED VIEW pg_wmr
    IN CLUSTER qa_canary_environment_compute
    AS
    WITH MUTUALLY RECURSIVE
        symm (a int, b int) AS (
            SELECT a, b FROM qa_canary_environment.public_pg_cdc_sources.pg_relationships
            UNION
            SELECT b, a FROM qa_canary_environment.public_pg_cdc_sources.pg_relationships
        ),
        candidates (a int, b int, degree int) AS (
            SELECT a, b, 1 FROM symm
            UNION
            SELECT symm.a, reach.b, reach.degree + 1
            FROM symm, reach
            WHERE symm.b = reach.a
        ),
        reach (a int, b int, degree int) AS (
            SELECT a, b, min(degree) FROM candidates GROUP BY a, b HAVING a != b
        )
    SELECT DISTINCT a_people.name AS a_name, b_people.name AS b_name, degree
    FROM reach
    LEFT JOIN qa_canary_environment.public_pg_cdc_sources.pg_people AS a_people ON (a = a_people.id)
    LEFT JOIN qa_canary_environment.public_pg_cdc_sources.pg_people AS b_people ON (b = b_people.id)
    WHERE degree >= 2;

CREATE DEFAULT INDEX IN CLUSTER qa_canary_environment_compute ON pg_wmr;

GRANT ALL PRIVILEGES ON TABLE qa_canary_environment.public_pg_cdc.pg_wmr TO "infra+bot@materialize.com", "infra+qacanaryload@materialize.io";
