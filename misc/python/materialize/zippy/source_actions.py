# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from typing import List, Set, Type

from materialize.mzcompose import Composition
from materialize.zippy.framework import Action, Capabilities, Capability
from materialize.zippy.kafka_capabilities import KafkaRunning, TopicExists
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.source_capabilities import SourceExists


class CreateSource(Action):
    """Creates a source in Materialized."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, KafkaRunning, TopicExists}

    def __init__(self, capabilities: Capabilities) -> None:
        source_name = "source" + str(random.randint(1, 10))
        this_source = SourceExists(name=source_name)

        existing_sources = [
            s for s in capabilities.get(SourceExists) if s.name == this_source.name
        ]

        if len(existing_sources) == 0:
            self.new_source = True

            self.source = this_source
            self.topic = random.choice(capabilities.get(TopicExists))
            self.source.topic = self.topic
        elif len(existing_sources) == 1:
            self.new_source = False

            self.source = existing_sources[0]
            assert self.source.topic is not None
            self.topic = self.source.topic
        else:
            assert False

    def run(self, c: Composition) -> None:
        if self.new_source:
            envelope = str(self.topic.envelope).split(".")[1]
            c.testdrive(
                f"""
> CREATE SOURCE {self.source.name}
  FROM KAFKA BROKER '${{testdrive.kafka-addr}}'
  TOPIC 'testdrive-{self.topic.name}-${{testdrive.seed}}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${{testdrive.schema-registry-url}}'
  ENVELOPE {envelope}
"""
            )

    def provides(self) -> List[Capability]:
        return [self.source] if self.new_source else []
