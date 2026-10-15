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
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


class ItemColumns(Check):
    """
    The columns and index keys that our builtin column relations report
    for user items come from the `ItemColumn` and `IndexColumn` rows the
    catalog records when it writes an item (e.g. view, table). Bootstrap
    also rewrites any row in case an upgrade changes column information.
    Assert that the column information each builtin relation reports reflects
    the DDL we do in this check.
    """

    def _can_run(self, e: Executor) -> bool:
        # Run for versions that support `ALTER TABLE`
        return self.base_version >= MzVersion.parse_mz("v0.134.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_alter_table_add_column = true

                > CREATE TYPE item_columns_list AS LIST (ELEMENT TYPE = int4)
                > CREATE TYPE item_columns_record AS (x int4, y text)

                > CREATE TABLE item_columns_initial_table (
                    a int4 NOT NULL,
                    b text DEFAULT 'b',
                    c numeric(5, 2),
                    d item_columns_list,
                    e item_columns_record,
                    f timestamp(3),
                    g int4[]
                  )
                > INSERT INTO item_columns_initial_table (a) VALUES (1)
                > CREATE VIEW item_columns_initial_view AS
                  SELECT a, b, c + 1 AS c1, d, e, f, g FROM item_columns_initial_table
                > CREATE MATERIALIZED VIEW item_columns_initial_mv AS
                  SELECT a, b, c1 FROM item_columns_initial_view

                > CREATE INDEX item_columns_initial_column_idx ON item_columns_initial_table (a, b)
                > CREATE INDEX item_columns_initial_expression_idx ON item_columns_initial_table (c + 1, upper(b))

                > COMMENT ON COLUMN item_columns_initial_table.b IS 'initial_table b'
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_alter_table_add_column = true

                > ALTER TABLE item_columns_initial_table ADD COLUMN i int4
                > INSERT INTO item_columns_initial_table (a, i) VALUES (2, 2)

                > CREATE TABLE item_columns_middle_table (a int4, b item_columns_list)
                > INSERT INTO item_columns_middle_table VALUES (1, LIST[1])
                > CREATE MATERIALIZED VIEW item_columns_middle_mv AS
                  SELECT a, b FROM item_columns_middle_table
                > CREATE INDEX item_columns_middle_idx ON item_columns_initial_view (a)

                > COMMENT ON COLUMN item_columns_initial_view.c1 IS 'initial_view c1'
                """,
                """
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_alter_table_add_column = true

                > ALTER TABLE item_columns_middle_table ADD COLUMN c text
                > INSERT INTO item_columns_middle_table VALUES (2, LIST[2], 'c')
                > CREATE VIEW item_columns_final_view AS SELECT * FROM item_columns_middle_table
                > CREATE INDEX item_columns_final_idx ON item_columns_middle_table (b, a)

                > COMMENT ON COLUMN item_columns_middle_table.c IS 'middle_table c'
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
                > SELECT r.name, c.position, c.name, c.nullable, c.type, c.default, t.name, c.type_mod
                  FROM mz_columns c
                  JOIN mz_relations r ON r.id = c.id
                  JOIN mz_types t ON t.oid = c.type_oid
                  WHERE r.name LIKE 'item_columns_%'
                  ORDER BY r.name, c.position
                item_columns_final_view 1 a true integer <null> int4 -1
                item_columns_final_view 2 b true item_columns_list <null> item_columns_list -1
                item_columns_final_view 3 c true text <null> text -1
                item_columns_initial_mv 1 a false integer <null> int4 -1
                item_columns_initial_mv 2 b true text <null> text -1
                item_columns_initial_mv 3 c1 true numeric <null> numeric -1
                item_columns_initial_table 1 a false integer <null> int4 -1
                item_columns_initial_table 2 b true text 'b' text -1
                item_columns_initial_table 3 c true numeric <null> numeric 2555910
                item_columns_initial_table 4 d true item_columns_list <null> item_columns_list -1
                item_columns_initial_table 5 e true item_columns_record <null> item_columns_record -1
                item_columns_initial_table 6 f true "timestamp without time zone" <null> timestamp 3
                item_columns_initial_table 7 g true integer[] <null> _int4 -1
                item_columns_initial_table 8 i true integer <null> int4 -1
                item_columns_initial_view 1 a false integer <null> int4 -1
                item_columns_initial_view 2 b true text <null> text -1
                item_columns_initial_view 3 c1 true numeric <null> numeric -1
                item_columns_initial_view 4 d true item_columns_list <null> item_columns_list -1
                item_columns_initial_view 5 e true item_columns_record <null> item_columns_record -1
                item_columns_initial_view 6 f true "timestamp without time zone" <null> timestamp 3
                item_columns_initial_view 7 g true integer[] <null> _int4 -1
                item_columns_middle_mv 1 a true integer <null> int4 -1
                item_columns_middle_mv 2 b true item_columns_list <null> item_columns_list -1
                item_columns_middle_table 1 a true integer <null> int4 -1
                item_columns_middle_table 2 b true item_columns_list <null> item_columns_list -1
                item_columns_middle_table 3 c true text <null> text -1

                > SELECT i.name, ic.index_position, ic.on_position, ic.on_expression, ic.nullable
                  FROM mz_index_columns ic
                  JOIN mz_indexes i ON i.id = ic.index_id
                  WHERE i.name LIKE 'item_columns_%'
                  ORDER BY i.name, ic.index_position
                item_columns_final_idx 1 2 <null> true
                item_columns_final_idx 2 1 <null> true
                item_columns_initial_column_idx 1 1 <null> false
                item_columns_initial_column_idx 2 2 <null> true
                item_columns_initial_expression_idx 1 <null> "c + 1" true
                item_columns_initial_expression_idx 2 <null> pg_catalog.upper(b) true
                item_columns_middle_idx 1 1 <null> false

                > SELECT r.name, c.name, cm.comment
                  FROM mz_columns c
                  JOIN mz_relations r ON r.id = c.id
                  JOIN mz_internal.mz_comments cm ON cm.id = c.id AND cm.object_sub_id = c.position
                  WHERE r.name LIKE 'item_columns_%'
                  ORDER BY r.name, c.name
                item_columns_initial_table b "initial_table b"
                item_columns_initial_view c1 "initial_view c1"
                item_columns_middle_table c "middle_table c"

                > SELECT count(*) FROM item_columns_initial_mv
                2
                > SELECT count(*) FROM item_columns_middle_mv
                2
                """))
