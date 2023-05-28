# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent
from typing import List

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check


class SourceErrors(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE SECRET source_errors_secret AS 'postgres';

                > CREATE CONNECTION source_errors_connection FOR POSTGRES
                  HOST 'postgres',
                  DATABASE postgres,
                  USER source_errors_user1,
                  PASSWORD SECRET source_errors_secret

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

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                DROP PUBLICATION source_errors_publicationA;
                INSERT INTO source_errors_table VALUES (3);
                """,
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                DROP PUBLICATION source_errors_publicationB;
                INSERT INTO source_errors_table VALUES (4);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT status, error ~* 'publication .+ does not exist'
                  FROM mz_internal.mz_source_statuses
                  WHERE name LIKE 'source_errors_source%'
                  AND type != 'subsource'
                  AND type != 'progress';
                stalled true
                stalled true
                """
            )
        )
