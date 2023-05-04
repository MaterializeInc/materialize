# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from textwrap import dedent
from typing import List, Set, Type

from materialize.mzcompose import Composition
from materialize.zippy.framework import Action, ActionFactory, Capabilities, Capability
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.sink_capabilities import SinkExists
from materialize.zippy.storaged_capabilities import StoragedRunning
from materialize.zippy.view_capabilities import ViewExists


class CreateSinkParameterized(ActionFactory):
    """Creates a sink over an existing view. Then creates a source over that sink and a view over that source."""

    @classmethod
    def requires(self) -> List[Set[Type[Capability]]]:
        return [{MzIsRunning, ViewExists}]

    def __init__(self, max_sinks: int = 10) -> None:
        self.max_sinks = max_sinks

    def new(self, capabilities: Capabilities) -> List[Action]:
        new_sink_name = capabilities.get_free_capability_name(
            SinkExists, self.max_sinks
        )

        if new_sink_name:
            source_view = random.choice(capabilities.get(ViewExists))
            dest_view = ViewExists(
                name=f"{new_sink_name}_view",
                inputs=[source_view],
                expensive_aggregates=source_view.expensive_aggregates,
            )

            return [
                CreateSink(
                    sink=SinkExists(
                        name=new_sink_name,
                        source_view=source_view,
                        dest_view=dest_view,
                    ),
                    capabilities=capabilities,
                ),
            ]
        else:
            return []


class CreateSink(Action):
    def __init__(
        self,
        sink: SinkExists,
        capabilities: Capabilities,
    ) -> None:
        assert (
            sink is not None
        ), "CreateSink Action can not be referenced directly, it is produced by CreateSinkParameterized factory"
        self.sink = sink
        super().__init__(capabilities)

    def run(self, c: Composition) -> None:
        # The sink-derived source has upsert semantics, so produce a "normal" ViewExists output
        # from the 'before' and the 'after'

        dest_view_sql = dedent(
            f"""
            > CREATE MATERIALIZED VIEW {self.sink.dest_view.name} AS
              SELECT SUM(count_all)::int AS count_all, SUM(count_distinct)::int AS count_distinct, SUM(min_value)::int AS min_value, SUM(max_value)::int AS max_value FROM (
                SELECT (after).count_all, (after).count_distinct, (after).min_value, (after).max_value FROM {self.sink.name}_source
                UNION ALL
                SELECT -(before).count_all, -(before).count_distinct, -(before).min_value, -(before).max_value FROM {self.sink.name}_source
              );
            """
            if self.sink.dest_view.expensive_aggregates
            else f"""
            > CREATE MATERIALIZED VIEW {self.sink.dest_view.name} AS
              SELECT SUM(count_all)::int AS count_all FROM (
                SELECT (after).count_all FROM {self.sink.name}_source
                UNION ALL
                SELECT -(before).count_all FROM {self.sink.name}_source
              );
            """
        )

        c.testdrive(
            dedent(
                f"""
                > CREATE CONNECTION IF NOT EXISTS {self.sink.name}_kafka_conn TO KAFKA (BROKER '${{testdrive.kafka-addr}}', PROGRESS TOPIC 'zippy-{self.sink.name}-${{testdrive.seed}}');
                > CREATE CONNECTION IF NOT EXISTS {self.sink.name}_csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${{testdrive.schema-registry-url}}');

                > CREATE SINK {self.sink.name} FROM {self.sink.source_view.name}
                  INTO KAFKA CONNECTION {self.sink.name}_kafka_conn (TOPIC 'sink-{self.sink.name}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {self.sink.name}_csr_conn
                  ENVELOPE DEBEZIUM;

                # Ingest the sink again in order to be able to validate its contents

                > CREATE SOURCE {self.sink.name}_source
                  FROM KAFKA CONNECTION {self.sink.name}_kafka_conn (TOPIC 'sink-{self.sink.name}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {self.sink.name}_csr_conn
                  ENVELOPE NONE
            """
            )
            + dest_view_sql
        )

    def provides(self) -> List[Capability]:
        return [self.sink, self.sink.dest_view]
