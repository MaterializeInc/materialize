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

from materialize.mzcompose.composition import Composition
from materialize.zippy.balancerd_capabilities import BalancerdIsRunning
from materialize.zippy.framework import (
    Action,
    ActionFactory,
    Capabilities,
    Capability,
    State,
)
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.replica_capabilities import source_capable_clusters
from materialize.zippy.sink_capabilities import SinkExists
from materialize.zippy.storaged_capabilities import StoragedRunning
from materialize.zippy.view_capabilities import ViewExists


class CreateSinkParameterized(ActionFactory):
    """Creates a sink over an existing view. Then creates a source over that sink and a view over that source."""

    @classmethod
    def requires(cls) -> list[set[type[Capability]]]:
        return [{BalancerdIsRunning, MzIsRunning, StoragedRunning, ViewExists}]

    def __init__(self, max_sinks: int = 10) -> None:
        self.max_sinks = max_sinks

    def new(self, capabilities: Capabilities) -> list[Action]:
        new_sink_name = capabilities.get_free_capability_name(
            SinkExists, self.max_sinks
        )

        if new_sink_name:
            source_view = random.choice(capabilities.get(ViewExists))
            cluster_name_out = random.choice(source_capable_clusters(capabilities))
            cluster_name_in = random.choice(source_capable_clusters(capabilities))

            dest_view = ViewExists(
                name=f"{new_sink_name}_view",
                inputs=[source_view],
                expensive_aggregates=source_view.expensive_aggregates,
            )

            return [
                self.make_action(
                    sink=SinkExists(
                        name=new_sink_name,
                        source_view=source_view,
                        dest_view=dest_view,
                        cluster_name_out=cluster_name_out,
                        cluster_name_in=cluster_name_in,
                    ),
                    capabilities=capabilities,
                ),
            ]
        else:
            return []

    def make_action(self, sink: SinkExists, capabilities: Capabilities) -> Action:
        return CreateSink(sink=sink, capabilities=capabilities)


class CreatePostgresSinkParameterized(CreateSinkParameterized):
    def make_action(self, sink: SinkExists, capabilities: Capabilities) -> Action:
        return CreatePostgresSink(sink=sink, capabilities=capabilities)


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

    def run(self, c: Composition, state: State) -> None:
        # The sink-derived source has upsert semantics, so produce a "normal" ViewExists output
        # from the 'before' and the 'after'

        refresh = random.choice(
            ["ON COMMIT", f"EVERY '{random.randint(1, 5)} seconds'"]
        )

        dest_view_sql = dedent(
            f"""
            > CREATE MATERIALIZED VIEW {self.sink.dest_view.name}
              WITH (REFRESH {refresh}) AS
              SELECT SUM(count_all)::int AS count_all, SUM(count_distinct)::int AS count_distinct, SUM(min_value)::int AS min_value, SUM(max_value)::int AS max_value FROM (
                SELECT (after).count_all, (after).count_distinct, (after).min_value, (after).max_value FROM {self.sink.name}_source_tbl
                UNION ALL
                SELECT -(before).count_all, -(before).count_distinct, -(before).min_value, -(before).max_value FROM {self.sink.name}_source_tbl
              );
            """
            if self.sink.dest_view.expensive_aggregates
            else f"""
            > CREATE MATERIALIZED VIEW {self.sink.dest_view.name} AS
              SELECT SUM(count_all)::int AS count_all FROM (
                SELECT (after).count_all FROM {self.sink.name}_source_tbl
                UNION ALL
                SELECT -(before).count_all FROM {self.sink.name}_source_tbl
              );
            """
        )

        c.testdrive(
            dedent(f"""
                > CREATE CONNECTION IF NOT EXISTS {self.sink.name}_kafka_conn TO KAFKA (BROKER '${{testdrive.kafka-addr}}', PROGRESS TOPIC 'zippy-{self.sink.name}-${{testdrive.seed}}', SECURITY PROTOCOL PLAINTEXT);
                > CREATE CONNECTION IF NOT EXISTS {self.sink.name}_csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${{testdrive.schema-registry-url}}');

                > CREATE SINK {self.sink.name}
                  IN CLUSTER {self.sink.cluster_name_out}
                  FROM {self.sink.source_view.name}
                  INTO KAFKA CONNECTION {self.sink.name}_kafka_conn (TOPIC 'sink-{self.sink.name}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {self.sink.name}_csr_conn
                  ENVELOPE DEBEZIUM;

                $ kafka-verify-topic sink=materialize.public.{self.sink.name} await-value-schema=true

                # Ingest the sink again in order to be able to validate its contents

                > CREATE SOURCE {self.sink.name}_source
                  IN CLUSTER {self.sink.cluster_name_in}
                  FROM KAFKA CONNECTION {self.sink.name}_kafka_conn (TOPIC 'sink-{self.sink.name}')

                > CREATE TABLE {self.sink.name}_source_tbl FROM SOURCE {self.sink.name}_source (REFERENCE "sink-{self.sink.name}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {self.sink.name}_csr_conn
                  ENVELOPE NONE
            """) + dest_view_sql,
            mz_service=state.mz_service,
        )

    def provides(self) -> list[Capability]:
        return [self.sink, self.sink.dest_view]


class CreatePostgresSink(CreateSink):
    def run(self, c: Composition, state: State) -> None:
        c.up("postgres")
        refresh = random.choice(
            ["ON COMMIT", f"EVERY '{random.randint(1, 5)} seconds'"]
        )
        c.testdrive(
            dedent(f"""
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            CREATE SCHEMA IF NOT EXISTS zippy_sinks;

            > CREATE SECRET IF NOT EXISTS zippy_pgsink_pass AS 'postgres'
            > CREATE CONNECTION IF NOT EXISTS zippy_pgsink_conn TO POSTGRES
              (HOST postgres, DATABASE postgres, USER postgres, PASSWORD SECRET zippy_pgsink_pass)
            > CREATE SINK {self.sink.name} IN CLUSTER {self.sink.cluster_name_out}
              FROM {self.sink.source_view.name}
              INTO POSTGRES CONNECTION zippy_pgsink_conn (SCHEMA 'zippy_sinks', TABLE '{self.sink.name}')
            > SELECT s.status, s.error, f.write_frontier > 0
              FROM mz_internal.mz_sink_statuses s JOIN mz_internal.mz_frontiers f ON f.object_id = s.id
              WHERE s.name = '{self.sink.name}'
            running <null> true

            $ postgres-execute connection=postgres://postgres:postgres@postgres
            CREATE PUBLICATION {self.sink.name}_publication FOR TABLE zippy_sinks.{self.sink.name};

            > CREATE SOURCE {self.sink.name}_source IN CLUSTER {self.sink.cluster_name_in}
              FROM POSTGRES CONNECTION zippy_pgsink_conn (PUBLICATION '{self.sink.name}_publication')
            > CREATE TABLE {self.sink.name}_source_tbl FROM SOURCE {self.sink.name}_source
              (REFERENCE zippy_sinks.{self.sink.name})
            > CREATE MATERIALIZED VIEW {self.sink.dest_view.name} WITH (REFRESH {refresh})
              AS SELECT * FROM {self.sink.name}_source_tbl
        """),
            mz_service=state.mz_service,
        )
