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
from materialize.checks.checks import Check, externally_idempotent


@externally_idempotent(False)
class SourceErrors(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                # In order to avoid conflicts, user must be unique
                CREATE USER source_errors_user1 WITH SUPERUSER PASSWORD 'postgres';
                ALTER USER source_errors_user1 WITH replication;
                DROP PUBLICATION IF EXISTS source_errors_publicationA;
                DROP PUBLICATION IF EXISTS source_errors_publicationB;

                DROP TABLE IF EXISTS source_errors_table;

                CREATE TABLE source_errors_table (f1 TEXT);
                ALTER TABLE source_errors_table REPLICA IDENTITY FULL;

                INSERT INTO source_errors_table VALUES (1);

                CREATE PUBLICATION source_errors_publicationA FOR ALL TABLES;
                CREATE PUBLICATION source_errors_publicationB FOR ALL TABLES;

                > CREATE SECRET source_errors_secret AS 'postgres';

                > CREATE CONNECTION source_errors_connection FOR POSTGRES
                  HOST 'postgres',
                  DATABASE postgres,
                  USER source_errors_user1,
                  PASSWORD SECRET source_errors_secret

                > CREATE SOURCE source_errors_sourceA
                  FROM POSTGRES CONNECTION source_errors_connection
                  (PUBLICATION 'source_errors_publicationa') /* all lowercase */
                  FOR TABLES (source_errors_table AS source_errors_tableA)

                > CREATE SOURCE source_errors_sourceB
                  FROM POSTGRES CONNECTION source_errors_connection
                  (PUBLICATION 'source_errors_publicationb') /* all lowercase */
                  FOR TABLES (source_errors_table AS source_errors_tableB)

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO source_errors_table VALUES (2);

                > SELECT COUNT(*) FROM source_errors_tableA;
                2


                > SELECT COUNT(*) FROM source_errors_tableB;
                2
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                DROP PUBLICATION IF EXISTS source_errors_publicationA;
                INSERT INTO source_errors_table VALUES (3);

                # We sleep for a bit here to allow status updates to propagate to the storage controller
                # in scenarios where environmentd is killed
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s
                """,
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                DROP PUBLICATION IF EXISTS source_errors_publicationB;
                INSERT INTO source_errors_table VALUES (4);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT bool_and(error ~* 'publication .+ does not exist')
                    FROM mz_internal.mz_source_statuses
                    WHERE
                    name
                    IN (
                    SELECT name FROM (SHOW SUBSOURCES ON source_errors_sourceA WHERE type = 'subsource')
                    UNION ALL (SELECT name FROM (SHOW SUBSOURCES ON source_errors_sourceB WHERE type = 'subsource'))
                    UNION ALL SELECT 'source_errors_sourceA'
                    UNION ALL SELECT 'source_errors_sourceB'
                    );
                true
                """
            )
        )
