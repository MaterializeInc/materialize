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


class MetricSink(Check):
    """Boot has to re-parse a metric sink's `create_sql` back into a catalog
    item, rebuild the dependency edge to the relation it reads, and re-render
    the sink's dataflow. A sink whose dataflow does not come back publishes no
    metrics, so the check probes for the dataflow, not just the item."""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v26.39.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                > CREATE TABLE metric_sink_table (metric_name text, metric_type text, labels map[text=>text], value double, help text)

                > INSERT INTO metric_sink_table VALUES ('a', 'gauge', '{x=>y}', 1, 'help a')

                > CREATE VIEW metric_sink_view AS SELECT * FROM metric_sink_table

                > CREATE SCHEMA metric_sink_schema

                > CREATE METRIC SINK metric_sink_schema.metric_sink_one FROM metric_sink_view WITH (PREFIX = 'mz_metric_sink_one_')
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO metric_sink_table VALUES ('b', 'counter', '{x=>z}', 2, 'help b')

                > CREATE METRIC SINK metric_sink_two IN CLUSTER quickstart FROM metric_sink_view WITH (PREFIX = 'mz_metric_sink_two_')
                """,
                """
                > INSERT INTO metric_sink_table VALUES ('c', 'gauge', '{}', 3, 'help c')

                > CREATE METRIC SINK IF NOT EXISTS metric_sink_three FROM metric_sink_view WITH (PREFIX = 'mz_metric_sink_three_')

                # A sink is never the subject of a rename, but it is a bystander of every
                # rename of the relation it reads or of the schema it lives in, and both
                # rewrite its `create_sql`. Boot has to re-parse what the rewrite produced.
                > ALTER VIEW metric_sink_view RENAME TO metric_sink_view_renamed

                > ALTER SCHEMA metric_sink_schema RENAME TO metric_sink_schema_renamed
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
                > SHOW METRIC SINKS
                metric_sink_three metric_sink_view_renamed quickstart
                metric_sink_two   metric_sink_view_renamed quickstart

                > SHOW METRIC SINKS FROM metric_sink_schema_renamed
                metric_sink_one metric_sink_view_renamed quickstart

                # The FROM edge came back too, so the view is still pinned, under the
                # name the rename gave it.
                ! DROP VIEW metric_sink_view_renamed
                contains:still depended upon by metric sink

                > SELECT count(*) FROM metric_sink_view_renamed
                3

                # Each re-rendered dataflow registers a collector that stamps
                # its own sink id on a frontier gauge, so three distinct ids
                # means all three dataflows came back. The registry is only
                # sampled every `compute_prometheus_introspection_scrape_interval`,
                # so give the scrape room to land.
                $ set-sql-timeout duration=60s

                # Every replica has its own registry, so this introspection
                # relation can only be read with a replica targeted.
                $ set-from-sql var=replica-name
                SELECT r.name
                FROM mz_catalog.mz_cluster_replicas r
                JOIN mz_catalog.mz_clusters c ON c.id = r.cluster_id
                WHERE c.name = 'quickstart'
                ORDER BY r.name
                LIMIT 1

                > SET cluster_replica = ${replica-name}

                > SELECT count(DISTINCT labels -> 'sink') FROM mz_introspection.mz_cluster_prometheus_metrics
                  WHERE metric_name = 'mz_compute_metric_sink_frontier_ms'
                3

                > RESET cluster_replica
                """))
