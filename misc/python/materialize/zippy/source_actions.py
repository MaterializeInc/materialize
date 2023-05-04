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
from materialize.zippy.kafka_capabilities import KafkaRunning, TopicExists
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.source_capabilities import SourceExists
from materialize.zippy.storaged_capabilities import StoragedRunning


class CreateSourceParameterized(ActionFactory):
    """Creates a source in Materialized."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, KafkaRunning, TopicExists}

    def __init__(self, max_sources: int = 10) -> None:
        self.max_sources = max_sources

    def new(self, capabilities: Capabilities) -> List[Action]:
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
                    ),
                )
            ]
        else:
            return []


class CreateSource(Action):
    def __init__(self, capabilities: Capabilities, source: SourceExists) -> None:
        self.source = source
        super().__init__(capabilities)

    def run(self, c: Composition) -> None:
        envelope = str(self.source.topic.envelope).split(".")[1]
        c.testdrive(
            dedent(
                f"""
                > CREATE CONNECTION IF NOT EXISTS {self.source.name}_csr_conn
                  TO CONFLUENT SCHEMA REGISTRY (URL '${{testdrive.schema-registry-url}}');

                > CREATE CONNECTION IF NOT EXISTS {self.source.name}_kafka_conn
                  TO KAFKA (BROKER '${{testdrive.kafka-addr}}');

                > CREATE SOURCE {self.source.name}
                  FROM KAFKA CONNECTION {self.source.name}_kafka_conn
                  (TOPIC 'testdrive-{self.source.topic.name}-${{testdrive.seed}}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {self.source.name}_csr_conn
                  ENVELOPE {envelope}
                """
            )
        )

    def provides(self) -> List[Capability]:
        return [self.source]
