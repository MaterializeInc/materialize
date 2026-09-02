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


class ReassignDropOwned(Check):
    """REASSIGN OWNED BY ... TO ... and DROP OWNED BY ...."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE ROLE reassign_owned_r1
            > CREATE ROLE reassign_owned_r2
            > CREATE ROLE reassign_owned_r3

            > CREATE TABLE reassign_owned_t1 (f1 INT)
            > INSERT INTO reassign_owned_t1 VALUES (1)
            > CREATE VIEW reassign_owned_v1 AS SELECT f1 + 1 AS f1 FROM reassign_owned_t1
            > ALTER TABLE reassign_owned_t1 OWNER TO reassign_owned_r1
            > ALTER VIEW reassign_owned_v1 OWNER TO reassign_owned_r1

            > CREATE TABLE reassign_owned_t3 (f1 INT)
            > CREATE SECRET reassign_owned_s3 AS 'value3'
            > ALTER TABLE reassign_owned_t3 OWNER TO reassign_owned_r3
            > ALTER SECRET reassign_owned_s3 OWNER TO reassign_owned_r3

            # DROP OWNED also revokes privileges granted to the role.
            > GRANT SELECT ON reassign_owned_t1 TO reassign_owned_r3
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                REASSIGN OWNED BY reassign_owned_r1 TO reassign_owned_r2
                """,
                """
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                DROP OWNED BY reassign_owned_r3 CASCADE
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT o.name, r.name
              FROM mz_objects o JOIN mz_roles r ON o.owner_id = r.id
              WHERE o.name LIKE 'reassign_owned_%'
            reassign_owned_t1 reassign_owned_r2
            reassign_owned_v1 reassign_owned_r2

            # reassign_owned_r3's objects are gone, but the role itself stays.
            > SELECT count(*)
              FROM mz_objects o JOIN mz_roles r ON o.owner_id = r.id
              WHERE r.name = 'reassign_owned_r3'
            0

            > SELECT name FROM mz_roles WHERE name LIKE 'reassign_owned_%'
            reassign_owned_r1
            reassign_owned_r2
            reassign_owned_r3

            # The SELECT privilege granted to reassign_owned_r3 was revoked by
            # DROP OWNED.
            > SELECT count(*)
              FROM mz_tables t, unnest(t.privileges) p
              JOIN mz_roles r ON mz_internal.mz_aclitem_grantee(p) = r.id
              WHERE t.name = 'reassign_owned_t1' AND r.name = 'reassign_owned_r3'
            0

            > SELECT * FROM reassign_owned_v1
            2
            """))
