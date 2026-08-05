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
    """A metric sink ships no dataflow, so surviving a restart is the whole of
    what it does. Boot has to re-parse its `create_sql` back into a catalog item
    and rebuild the dependency edge to the relation it reads."""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v26.36.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                > CREATE TABLE metric_sink_table (metric_name text, metric_type text, labels map[text=>text], value double, help text)

                > INSERT INTO metric_sink_table VALUES ('a', 'gauge', '{x=>y}', 1, 'help a')

                > CREATE VIEW metric_sink_view AS SELECT * FROM metric_sink_table

                > CREATE METRIC SINK metric_sink_one FROM metric_sink_view
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO metric_sink_table VALUES ('b', 'counter', '{x=>z}', 2, 'help b')

                > CREATE METRIC SINK metric_sink_two IN CLUSTER quickstart FROM metric_sink_view
                """,
                """
                > INSERT INTO metric_sink_table VALUES ('c', 'gauge', '{}', 3, 'help c')

                > CREATE METRIC SINK IF NOT EXISTS metric_sink_three FROM metric_sink_view
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        # Recreating a sink that is already there is how we probe for it without
        # mutating anything: metric sinks are not in `mz_objects` yet, and
        # `mz_catalog_raw` needs a system connection.
        #
        # TODO(SQL-572): once metric sinks join `mz_objects` (and the
        # `mz_metric_sinks` builtin view), probe survival by SELECTing the sink
        # row instead of re-issuing CREATE and matching "already exists". The
        # current probe is a proxy: it confirms the catalog item was re-parsed on
        # boot without needing a system connection.
        return Testdrive(dedent("""
                ! CREATE METRIC SINK metric_sink_one FROM metric_sink_view
                contains:metric sink "materialize.public.metric_sink_one" already exists

                ! CREATE METRIC SINK metric_sink_two FROM metric_sink_view
                contains:metric sink "materialize.public.metric_sink_two" already exists

                ! CREATE METRIC SINK metric_sink_three FROM metric_sink_view
                contains:metric sink "materialize.public.metric_sink_three" already exists

                # The FROM edge came back too, so the view is still pinned.
                ! DROP VIEW metric_sink_view
                contains:still depended upon by metric sink

                > SELECT count(*) FROM metric_sink_view
                3
                """))
