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
from materialize.zippy.kafka_capabilities import KafkaRunning, TopicExists
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.replica_capabilities import source_capable_clusters
from materialize.zippy.source_capabilities import SourceExists
from materialize.zippy.storaged_capabilities import StoragedRunning


class CreateSourceParameterized(ActionFactory):
    """Creates a source in Materialized."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {
            BalancerdIsRunning,
            MzIsRunning,
            StoragedRunning,
            KafkaRunning,
            TopicExists,
        }

    def __init__(self, max_sources: int = 10) -> None:
        self.max_sources = max_sources

    def new(self, capabilities: Capabilities) -> list[Action]:
        new_source_name = capabilities.get_free_capability_name(
            SourceExists, self.max_sources
        )

        if new_source_name:
            return [
                CreateSource(
                    capabilities=capabilities,
                    source=SourceExists(
                        name=new_source_name,
                        topic=random.choice(capabilities.get(TopicExists)),
                        cluster_name=random.choice(
                            source_capable_clusters(capabilities)
                        ),
                        uses_ssh_tunnel=random.choice([True, False]),
                    ),
                )
            ]
        else:
            return []


class AlterSourceConnectionParameterized(ActionFactory):
    """Alters a source in Materialized."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {MzIsRunning, StoragedRunning, KafkaRunning, TopicExists, SourceExists}

    def new(self, capabilities: Capabilities) -> list[Action]:
        existing_source_exists = capabilities.get(
            SourceExists,
        )

        return [
            AlterSourceConnection(
                capabilities=capabilities,
                source=source_exists,
            )
            for source_exists in existing_source_exists
        ]


class CreateSource(Action):
    def __init__(self, capabilities: Capabilities, source: SourceExists) -> None:
        self.source = source
        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
        envelope = str(self.source.topic.envelope).split(".")[1]
        kafka_connection_name = f"{self.source.name}_kafka_conn"
        c.testdrive(
            dedent(
                f"""
                > CREATE CONNECTION IF NOT EXISTS {self.source.name}_csr_conn
                  TO CONFLUENT SCHEMA REGISTRY (URL '${{testdrive.schema-registry-url}}');

                > CREATE CONNECTION IF NOT EXISTS {kafka_connection_name}
                  TO KAFKA (BROKER '${{testdrive.kafka-addr}}' {'USING SSH TUNNEL zippy_ssh' if self.source.uses_ssh_tunnel else ''}, SECURITY PROTOCOL PLAINTEXT);

                > CREATE SOURCE {self.source.name}
                  IN CLUSTER {self.source.cluster_name}
                  FROM KAFKA CONNECTION {kafka_connection_name}
                  (TOPIC 'testdrive-{self.source.topic.name}-${{testdrive.seed}}')

                > CREATE TABLE {self.source.get_name_for_query()} FROM SOURCE {self.source.name} (REFERENCE "testdrive-{self.source.topic.name}-${{testdrive.seed}}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {self.source.name}_csr_conn
                  ENVELOPE {envelope}
                """
            ),
            mz_service=state.mz_service,
        )

    def provides(self) -> list[Capability]:
        return [self.source]


class AlterSourceConnection(Action):
    def __init__(self, capabilities: Capabilities, source: SourceExists) -> None:
        self.source = source
        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
        # This flips the usage of the SSH tunnel.
        self.flip_usage_of_ssh_tunnel(
            c,
            new_use_ssh_status=not self.source.uses_ssh_tunnel,
            mz_service=state.mz_service,
        )
        self.source.uses_ssh_tunnel = not self.source.uses_ssh_tunnel

    def flip_usage_of_ssh_tunnel(
        self, c: Composition, new_use_ssh_status: bool, mz_service: str
    ) -> None:
        kafka_connection_name = f"{self.source.name}_kafka_conn"

        c.testdrive(
            dedent(
                f"""
                > ALTER CONNECTION {kafka_connection_name} SET (BROKER '${{testdrive.kafka-addr}}'
                  {'USING SSH TUNNEL zippy_ssh' if new_use_ssh_status else ''});
                """
            ),
            mz_service=mz_service,
        )

    def provides(self) -> list[Capability]:
        return []
