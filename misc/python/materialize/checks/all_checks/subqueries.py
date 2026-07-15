# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check


class Subqueries(Check):
    """Subquery operators in maintained queries: IN, NOT IN, EXISTS, NOT
    EXISTS, ANY, ALL, and correlated scalar subqueries."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE subqueries_outer (id INT, grp STRING)
            > CREATE TABLE subqueries_inner (id INT, outer_id INT, val INT)
            > INSERT INTO subqueries_outer VALUES (1, 'a'), (2, 'b'), (3, 'c')
            > INSERT INTO subqueries_inner VALUES (1, 1, 100), (2, 1, 200), (3, 2, 300)
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO subqueries_outer VALUES (4, 'd')

                > CREATE MATERIALIZED VIEW subqueries_in1 AS
                  SELECT id FROM subqueries_outer
                  WHERE id IN (SELECT outer_id FROM subqueries_inner)

                > CREATE MATERIALIZED VIEW subqueries_not_in1 AS
                  SELECT id FROM subqueries_outer
                  WHERE id NOT IN (SELECT outer_id FROM subqueries_inner)

                > CREATE MATERIALIZED VIEW subqueries_exists1 AS
                  SELECT o.id FROM subqueries_outer o
                  WHERE EXISTS (SELECT 1 FROM subqueries_inner i WHERE i.outer_id = o.id AND i.val >= 200)

                > CREATE MATERIALIZED VIEW subqueries_not_exists1 AS
                  SELECT o.id FROM subqueries_outer o
                  WHERE NOT EXISTS (SELECT 1 FROM subqueries_inner i WHERE i.outer_id = o.id)
                """,
                """
                > INSERT INTO subqueries_inner VALUES (4, 4, 400)

                > CREATE MATERIALIZED VIEW subqueries_any1 AS
                  SELECT id FROM subqueries_outer
                  WHERE id = ANY (SELECT outer_id FROM subqueries_inner WHERE val > 250)

                > CREATE MATERIALIZED VIEW subqueries_all1 AS
                  SELECT id FROM subqueries_outer
                  WHERE id < ALL (SELECT outer_id FROM subqueries_inner WHERE val > 250)

                > CREATE MATERIALIZED VIEW subqueries_scalar1 AS
                  SELECT o.id, (SELECT max(val) FROM subqueries_inner i WHERE i.outer_id = o.id) AS max_val
                  FROM subqueries_outer o
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT * FROM subqueries_in1
            1
            2
            4

            > SELECT * FROM subqueries_not_in1
            3

            > SELECT * FROM subqueries_exists1
            1
            2
            4

            > SELECT * FROM subqueries_not_exists1
            3

            > SELECT * FROM subqueries_any1
            2
            4

            > SELECT * FROM subqueries_all1
            1

            > SELECT * FROM subqueries_scalar1
            1 200
            2 300
            3 <null>
            4 400
            """))
