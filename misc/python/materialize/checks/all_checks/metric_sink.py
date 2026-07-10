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


class MetricSink(Check):
    """Metric sinks are durable catalog items: their dataflow is re-optimized
    and re-shipped on `environmentd` bootstrap. This checks that a metric
    sink created before a restart still exists afterwards, and that its
    dataflow was actually rehydrated: the emitted series and companion
    gauges must reappear in the cluster's Prometheus registry, not just the
    catalog row.
    """

    def initialize(self) -> Testdrive:
        return Testdrive(dedent(f"""
                $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER SYSTEM SET enable_metric_sink = true

                > CREATE TABLE metric_sink_table (k TEXT NOT NULL, val DOUBLE NOT NULL)
                > INSERT INTO metric_sink_table VALUES ('a', 1)

                > CREATE VIEW metric_sink_view AS
                  SELECT 'mz_check_metric_sink_value'::text AS metric_name,
                         'gauge'::text AS metric_type,
                         map_build(LIST[ROW('k', k)])::map[text=>text] AS labels,
                         val AS value,
                         'metric emitted by the MetricSink platform check'::text AS help
                  FROM metric_sink_table

                > CREATE METRIC SINK metric_sink_s IN CLUSTER {self._default_cluster()} FROM metric_sink_view
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO metric_sink_table VALUES ('b', 2)
                """,
                """
                > INSERT INTO metric_sink_table VALUES ('c', 3)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent(f"""
                $ set-sql-timeout duration=60s

                > SELECT count(*) FROM mz_catalog.mz_metric_sinks WHERE name = 'metric_sink_s'
                1

                > SHOW METRIC SINKS
                metric_sink_s {self._default_cluster()} ""

                # The dataflow was re-shipped and the collector re-registered on boot: the
                # emitted series is visible again through the cluster's scraped registry,
                # one row per distinct label set.
                > SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
                  WHERE metric_name = 'mz_check_metric_sink_value'
                3

                > SELECT value = 1 FROM mz_introspection.mz_cluster_prometheus_metrics
                  WHERE metric_name = 'mz_check_metric_sink_value' AND labels -> 'k' = 'a'
                true

                > SELECT value = 2 FROM mz_introspection.mz_cluster_prometheus_metrics
                  WHERE metric_name = 'mz_check_metric_sink_value' AND labels -> 'k' = 'b'
                true

                > SELECT value = 3 FROM mz_introspection.mz_cluster_prometheus_metrics
                  WHERE metric_name = 'mz_check_metric_sink_value' AND labels -> 'k' = 'c'
                true

                # The companion frontier gauge for this sink is present too, proving the
                # sink's own dataflow (not just user series from a coincidentally-named
                # source) came back up.
                $ set-from-sql var=metric-sink-id
                SELECT id FROM mz_catalog.mz_metric_sinks WHERE name = 'metric_sink_s';

                > SELECT value > 0 FROM mz_introspection.mz_cluster_prometheus_metrics
                  WHERE metric_name = 'mz_metric_sink_frontier_ms' AND labels -> 'sink' = '${{metric-sink-id}}'
                true
           """))
