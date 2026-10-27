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


class RefreshVariants(Check):
    """REFRESH strategy variants not covered by materialized_views.py:
    ALIGNED TO, AT CREATION, and explicit ON COMMIT."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE refresh_variants_table (x INT)
            > INSERT INTO refresh_variants_table VALUES (1)

            > CREATE MATERIALIZED VIEW refresh_variants_on_commit1
              WITH (REFRESH ON COMMIT)
              AS SELECT sum(x) FROM refresh_variants_table

            > CREATE MATERIALIZED VIEW refresh_variants_at_creation1
              WITH (REFRESH AT CREATION)
              AS SELECT sum(x) FROM refresh_variants_table
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO refresh_variants_table VALUES (2)

                > CREATE MATERIALIZED VIEW refresh_variants_aligned1
                  WITH (REFRESH EVERY '2 seconds' ALIGNED TO mz_now()::text::int8 + 2000)
                  AS SELECT sum(x) FROM refresh_variants_table

                > CREATE MATERIALIZED VIEW refresh_variants_at_creation2
                  WITH (REFRESH AT CREATION)
                  AS SELECT sum(x) FROM refresh_variants_table
                """,
                """
                > INSERT INTO refresh_variants_table VALUES (4)

                > CREATE MATERIALIZED VIEW refresh_variants_combined1
                  WITH (REFRESH AT CREATION, REFRESH EVERY '2 seconds')
                  AS SELECT sum(x) FROM refresh_variants_table
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT * FROM refresh_variants_on_commit1
            7

            # AT CREATION views are frozen at the data as of their creation.
            > SELECT * FROM refresh_variants_at_creation1
            1

            > SELECT * FROM refresh_variants_at_creation2
            3

            > SELECT * FROM refresh_variants_aligned1
            7

            > SELECT * FROM refresh_variants_combined1
            7
            """))


class ScheduledCluster(Check):
    """Clusters with SCHEDULE = ON REFRESH, which automatically manage their
    replicas around the refresh times of the REFRESH materialized views they
    host."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE CLUSTER scheduled_cluster_check (
                SIZE = 'scale=1,workers=1',
                SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 second')
              )

            > CREATE TABLE scheduled_cluster_table (x INT)
            > INSERT INTO scheduled_cluster_table VALUES (1)

            > CREATE MATERIALIZED VIEW scheduled_cluster_mv1
              IN CLUSTER scheduled_cluster_check
              WITH (REFRESH EVERY '5 seconds')
              AS SELECT sum(x) AS s FROM scheduled_cluster_table
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO scheduled_cluster_table VALUES (2)

                # Flipping to MANUAL and back must work and leave the schedule
                # in place.
                > ALTER CLUSTER scheduled_cluster_check SET (SCHEDULE = MANUAL)
                > ALTER CLUSTER scheduled_cluster_check SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 second'))
                """,
                """
                > INSERT INTO scheduled_cluster_table VALUES (4)

                > CREATE MATERIALIZED VIEW scheduled_cluster_mv2
                  IN CLUSTER scheduled_cluster_check
                  WITH (REFRESH EVERY '5 seconds')
                  AS SELECT count(*) AS c FROM scheduled_cluster_table
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT type FROM mz_internal.mz_cluster_schedules cs
              JOIN mz_clusters c ON cs.cluster_id = c.id
              WHERE c.name = 'scheduled_cluster_check'
            on-refresh

            # scheduled_cluster_mv1 was created before the later inserts, so
            # seeing their data proves refreshes are happening.
            > SELECT s FROM scheduled_cluster_mv1
            7

            > SELECT c FROM scheduled_cluster_mv2
            3
            """))
