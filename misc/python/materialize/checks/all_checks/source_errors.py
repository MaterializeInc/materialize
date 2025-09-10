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
from materialize.checks.checks import Check, disabled, externally_idempotent


@disabled("due to database-issues#9223")
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
                > CREATE TABLE source_errors_tableA FROM SOURCE source_errors_sourceA (REFERENCE source_errors_table);

                > CREATE SOURCE source_errors_sourceB
                  FROM POSTGRES CONNECTION source_errors_connection
                  (PUBLICATION 'source_errors_publicationb') /* all lowercase */
                > CREATE TABLE source_errors_tableB FROM SOURCE source_errors_sourceB (REFERENCE source_errors_table);

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
                # We check two things: a) that the expected (sub)sources are
                # stalled, and b) that they report the expected error. Only
                # checking the error using bool_and wouldn't work because this
                # check ignores NULL values, so would succeed if, say, all
                # sources are in state 'running'.
                #
                # TODO(aljoscha): We recently migrated the status history
                # collection, so all updates are lost when upgrading. This has
                # the consequence that sources report as 'created'. We therefore
                # have to accept 'created' below, but should remove this
                # relaxation once we have enough new releasees that there won't
                # be a migration between tested versions. Plus, because of this
                # we have to wrap the error check in a coalesce: when all the
                # status updates show 'created', we'll have no error and get a
                # NULL result.
                #
                # Additionally, we also have to accept paused, because platform
                # checks might pause replicas, and these paused status updates
                # take precedence over errors. To fix this, we might want to
                # rewrite this test to look at mz_source_status_history
                # instead, which contains the full history.
                """
                $ set-sql-timeout duration=120s
                > SELECT
                        coalesce(bool_and(error ~* 'publication .+ does not exist'), true) as matches,
                        bool_and(status IN ('stalled', 'created', 'paused')) as is_stalled
                    FROM mz_internal.mz_source_statuses
                    WHERE
                        name IN ('source_errors_sourcea', 'source_errors_sourceb', 'source_errors_tablea', 'source_errors_tableb');
                true true
                """
            )
        )
