# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.feature_benchmark.measurement_source import MeasurementSource, Td
from materialize.feature_benchmark.scenario import Scenario


class SmallClusters(Scenario):
    """Materialized views across many small clusters."""

    SCALE = 1.5  # 32 clusters
    FIXED_SCALE = True

    def benchmark(self) -> MeasurementSource:
        create = "\n".join(
            dedent(
                f"""
                > DROP CLUSTER IF EXISTS cluster{i} CASCADE;

                > CREATE CLUSTER cluster{i} REPLICAS (r (SIZE 'scale=4,workers=1'));

                > CREATE MATERIALIZED VIEW v{i}
                  IN CLUSTER cluster{i}
                  AS SELECT COUNT(*) FROM t1;

                > CREATE DEFAULT INDEX ON v{i}
                """
            )
            for i in range(self.n())
        )

        # We wait until all clusters are booted and connected before starting
        # the time measurement. This is to avoid flakiness caused by the retry
        # backoff the controller uses on replica connection attempts.
        wait = "\n".join(
            dedent(
                f"""
                > SET CLUSTER = cluster{i}
                > SELECT * FROM v{i}
                0
                """
            )
            for i in range(self.n())
        )

        select = "\n".join(
            dedent(
                f"""
                > SET CLUSTER = cluster{i}
                > SELECT * FROM v{i}
                1000000
                """
            )
            for i in range(self.n())
        )

        return Td(
            dedent(
                f"""
                > DROP TABLE IF EXISTS t1 CASCADE;
                > CREATE TABLE t1 (f1 INTEGER);

                $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER SYSTEM SET max_clusters = {self.n() + 2};
                """
            )
            + create
            + wait
            + dedent(
                """
                > INSERT INTO t1
                  SELECT * FROM generate_series(1, 1000000)
                  /* A */
                """
            )
            + select
            + dedent(
                """
                > SELECT 1
                  /* B */;
                1
                """
            )
        )
