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
    """The columns and index keys that `mz_columns` and `mz_index_columns`
    report for user items come from the `ItemColumn` and `IndexColumn` rows
    the catalog records when it writes an item. Bootstrap rewrites the rows of
    any item whose recorded rows differ from what the running build's planner
    produces, which is also what fills them in after an upgrade from a version
    without the collections. Every relation kind and index key shape, created
    in every phase, must report its columns afterwards, and the reported
    positions must line up with column comments."""

    def _can_run(self, e: Executor) -> bool:
        # ALTER TABLE ... ADD COLUMN, behind enable_alter_table_add_column.
        return self.base_version >= MzVersion.parse_mz("v0.134.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_alter_table_add_column = true

                > CREATE TYPE item_columns_list AS LIST (ELEMENT TYPE = int4)
                > CREATE TYPE item_columns_record AS (x int4, y text)

                > CREATE TABLE item_columns_t1 (
                    a int4 NOT NULL,
                    b text DEFAULT 'b',
                    c numeric(5, 2),
                    d item_columns_list,
                    e item_columns_record,
                    f timestamp(3),
                    g int4[]
                  )
                > INSERT INTO item_columns_t1 (a) VALUES (1)
                > CREATE VIEW item_columns_v1 AS
                  SELECT a, b, c + 1 AS c1, d, e, f, g FROM item_columns_t1
                > CREATE MATERIALIZED VIEW item_columns_mv1 AS
                  SELECT a, b, c1 FROM item_columns_v1

                > CREATE INDEX item_columns_i1 ON item_columns_t1 (a, b)
                > CREATE INDEX item_columns_i2 ON item_columns_t1 (c + 1, upper(b))

                > COMMENT ON COLUMN item_columns_t1.b IS 't1 b'
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_alter_table_add_column = true

                > ALTER TABLE item_columns_t1 ADD COLUMN i int4
                > INSERT INTO item_columns_t1 (a, i) VALUES (2, 2)

                > CREATE TABLE item_columns_t2 (a int4, b item_columns_list)
                > INSERT INTO item_columns_t2 VALUES (1, LIST[1])
                > CREATE MATERIALIZED VIEW item_columns_mv2 AS
                  SELECT a, b FROM item_columns_t2
                > CREATE INDEX item_columns_i3 ON item_columns_v1 (a)

                > COMMENT ON COLUMN item_columns_v1.c1 IS 'v1 c1'
                """,
                """
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_alter_table_add_column = true

                > ALTER TABLE item_columns_t2 ADD COLUMN c text
                > INSERT INTO item_columns_t2 VALUES (2, LIST[2], 'c')
                > CREATE VIEW item_columns_v2 AS SELECT * FROM item_columns_t2
                > CREATE INDEX item_columns_i4 ON item_columns_t2 (b, a)

                > COMMENT ON COLUMN item_columns_t2.c IS 't2 c'
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        # `type_oid` is reported through the name of the type it resolves to,
        # since user type OIDs are not stable across scenarios.
        return Testdrive(dedent("""
                > SELECT r.name, c.position, c.name, c.nullable, c.type, c.default, t.name, c.type_mod
                  FROM mz_columns c
                  JOIN mz_relations r ON r.id = c.id
                  JOIN mz_types t ON t.oid = c.type_oid
                  WHERE r.name LIKE 'item_columns_%'
                  ORDER BY r.name, c.position
                item_columns_mv1 1 a false integer <null> int4 -1
                item_columns_mv1 2 b true text <null> text -1
                item_columns_mv1 3 c1 true numeric <null> numeric -1
                item_columns_mv2 1 a true integer <null> int4 -1
                item_columns_mv2 2 b true item_columns_list <null> item_columns_list -1
                item_columns_t1 1 a false integer NULL int4 -1
                item_columns_t1 2 b true text 'b' text -1
                item_columns_t1 3 c true numeric NULL numeric 2555910
                item_columns_t1 4 d true item_columns_list NULL item_columns_list -1
                item_columns_t1 5 e true item_columns_record NULL item_columns_record -1
                item_columns_t1 6 f true "timestamp without time zone" NULL timestamp 3
                item_columns_t1 7 g true integer[] NULL _int4 -1
                item_columns_t1 8 i true integer NULL int4 -1
                item_columns_t2 1 a true integer NULL int4 -1
                item_columns_t2 2 b true item_columns_list NULL item_columns_list -1
                item_columns_t2 3 c true text NULL text -1
                item_columns_v1 1 a false integer <null> int4 -1
                item_columns_v1 2 b true text <null> text -1
                item_columns_v1 3 c1 true numeric <null> numeric -1
                item_columns_v1 4 d true item_columns_list <null> item_columns_list -1
                item_columns_v1 5 e true item_columns_record <null> item_columns_record -1
                item_columns_v1 6 f true "timestamp without time zone" <null> timestamp 3
                item_columns_v1 7 g true integer[] <null> _int4 -1
                item_columns_v2 1 a true integer <null> int4 -1
                item_columns_v2 2 b true item_columns_list <null> item_columns_list -1
                item_columns_v2 3 c true text <null> text -1

                > SELECT i.name, ic.index_position, ic.on_position, ic.on_expression, ic.nullable
                  FROM mz_index_columns ic
                  JOIN mz_indexes i ON i.id = ic.index_id
                  WHERE i.name LIKE 'item_columns_%'
                  ORDER BY i.name, ic.index_position
                item_columns_i1 1 1 <null> false
                item_columns_i1 2 2 <null> true
                item_columns_i2 1 <null> "c + 1" true
                item_columns_i2 2 <null> pg_catalog.upper(b) true
                item_columns_i3 1 1 <null> false
                item_columns_i4 1 2 <null> true
                item_columns_i4 2 1 <null> true

                > SELECT r.name, c.name, cm.comment
                  FROM mz_columns c
                  JOIN mz_relations r ON r.id = c.id
                  JOIN mz_internal.mz_comments cm ON cm.id = c.id AND cm.object_sub_id = c.position
                  WHERE r.name LIKE 'item_columns_%'
                  ORDER BY r.name, c.name
                item_columns_t1 b "t1 b"
                item_columns_t2 c "t2 c"
                item_columns_v1 c1 "v1 c1"

                > SELECT count(*) FROM item_columns_mv1
                2
                > SELECT count(*) FROM item_columns_mv2
                2
                """))
