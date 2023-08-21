# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import string
import threading
from textwrap import dedent
from typing import Dict, List, Set, Type

import numpy as np

from materialize.mzcompose import Composition
from materialize.zippy.framework import Action, ActionFactory, Capabilities, Capability
from materialize.zippy.kafka_capabilities import Envelope, KafkaRunning, TopicExists
from materialize.zippy.mz_capabilities import MzIsRunning

SCHEMA = """
$ set keyschema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"key", "type":"long"}
        ]
    }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"long"},
            {"name":"pad", "type":"string"}
        ]
    }
"""


class KafkaStart(Action):
    """Start a Kafka instance."""

    def provides(self) -> List[Capability]:
        return [KafkaRunning()]

    def run(self, c: Composition) -> None:
        c.up("kafka")


class KafkaStop(Action):
    """Stop the Kafka instance."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {KafkaRunning}

    def withholds(self) -> Set[Type[Capability]]:
        return {KafkaRunning}

    def run(self, c: Composition) -> None:
        c.kill("kafka")


class CreateTopicParameterized(ActionFactory):
    """Creates a Kafka topic and decides on the envelope that will be used."""

    @classmethod
    def requires(cls) -> Set[Type[Capability]]:
        return {MzIsRunning, KafkaRunning}

    def __init__(
        self,
        max_topics: int = 10,
        envelopes_with_weights: Dict[Envelope, int] = {
            Envelope.NONE: 25,
            Envelope.UPSERT: 75,
        },
    ) -> None:
        self.max_topics = max_topics
        self.envelopes_with_weights = envelopes_with_weights

    def new(self, capabilities: Capabilities) -> List[Action]:
        new_topic_name = capabilities.get_free_capability_name(
            TopicExists, self.max_topics
        )

        if new_topic_name:
            return [
                CreateTopic(
                    capabilities=capabilities,
                    topic=TopicExists(
                        name=new_topic_name,
                        envelope=random.choices(
                            list(self.envelopes_with_weights.keys()),
                            weights=list(self.envelopes_with_weights.values()),
                        )[0],
                        partitions=random.randint(1, 10),
                    ),
                )
            ]
        else:
            return []


class CreateTopic(Action):
    def __init__(self, capabilities: Capabilities, topic: TopicExists) -> None:
        self.topic = topic
        super().__init__(capabilities)

    def provides(self) -> List[Capability]:
        return [self.topic]

    def run(self, c: Composition) -> None:
        c.testdrive(
            SCHEMA
            + dedent(
                f"""
                $ kafka-create-topic topic={self.topic.name} partitions={self.topic.partitions}
                $ kafka-ingest format=avro key-format=avro topic={self.topic.name} schema=${{schema}} key-schema=${{keyschema}} repeat=1
                {{"key": 0}} {{"f1": 0, "pad": ""}}
                """
            )
        )


class Ingest(Action):
    """Ingests data (inserts, updates or deletions) into a Kafka topic."""

    @classmethod
    def requires(cls) -> Set[Type[Capability]]:
        return {MzIsRunning, KafkaRunning, TopicExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.topic = random.choice(capabilities.get(TopicExists))
        self.delta = random.randint(1, 10000)
        # This gives 67% pads of up to 10 bytes, 25% of up to 100 bytes and outliers up to 256 bytes
        self.pad = min(np.random.zipf(1.6, 1)[0], 256) * random.choice(
            string.ascii_letters
        )
        super().__init__(capabilities)

    def __str__(self) -> str:
        return f"{Action.__str__(self)} {self.topic.name}"


class KafkaInsert(Ingest):
    """Inserts data into a Kafka topic."""

    def parallel(self) -> bool:
        return False

    def run(self, c: Composition) -> None:
        prev_max = self.topic.watermarks.max
        self.topic.watermarks.max = prev_max + self.delta
        assert self.topic.watermarks.max >= 0
        assert self.topic.watermarks.min >= 0

        testdrive_str = SCHEMA + dedent(
            f"""
            $ kafka-ingest format=avro key-format=avro topic={self.topic.name} schema=${{schema}} key-schema=${{keyschema}} start-iteration={prev_max + 1} repeat={self.delta}
            {{"key": ${{kafka-ingest.iteration}}}} {{"f1": ${{kafka-ingest.iteration}}, "pad" : "{self.pad}"}}
            """
        )

        if self.parallel():
            threading.Thread(target=c.testdrive, args=[testdrive_str]).start()
        else:
            c.testdrive(testdrive_str)


class KafkaInsertParallel(KafkaInsert):
    """Inserts data into a Kafka topic using background threads."""

    @classmethod
    def require_explicit_mention(self) -> bool:
        return True

    def parallel(self) -> bool:
        return True


class KafkaUpsertFromHead(Ingest):
    """Updates records from the head in-place by modifying their pad"""

    def run(self, c: Composition) -> None:
        if self.topic.envelope is Envelope.NONE:
            return

        head = self.topic.watermarks.max
        start = max(head - self.delta, self.topic.watermarks.min)
        actual_delta = head - start

        if actual_delta > 0:
            c.testdrive(
                SCHEMA
                + dedent(
                    f"""
                    $ kafka-ingest format=avro topic={self.topic.name} key-format=avro key-schema=${{keyschema}} schema=${{schema}} start-iteration={start} repeat={actual_delta}
                    {{"key": ${{kafka-ingest.iteration}}}} {{"f1": ${{kafka-ingest.iteration}}, "pad": "{self.pad}"}}
                    """
                )
            )


class KafkaDeleteFromHead(Ingest):
    """Deletes the largest values previously inserted."""

    def run(self, c: Composition) -> None:
        if self.topic.envelope is Envelope.NONE:
            return

        prev_max = self.topic.watermarks.max
        self.topic.watermarks.max = max(
            prev_max - self.delta, self.topic.watermarks.min
        )
        assert self.topic.watermarks.max >= 0
        assert self.topic.watermarks.min >= 0

        actual_delta = prev_max - self.topic.watermarks.max

        if actual_delta > 0:
            c.testdrive(
                SCHEMA
                + dedent(
                    f"""
                    $ kafka-ingest format=avro topic={self.topic.name} key-format=avro key-schema=${{keyschema}} schema=${{schema}} start-iteration={self.topic.watermarks.max + 1} repeat={actual_delta}
                    {{"key": ${{kafka-ingest.iteration}}}}
                    """
                )
            )


class KafkaUpsertFromTail(Ingest):
    """Updates records from the tail in-place by modifying their pad"""

    def run(self, c: Composition) -> None:
        if self.topic.envelope is Envelope.NONE:
            return

        tail = self.topic.watermarks.min
        end = min(tail + self.delta, self.topic.watermarks.max)
        actual_delta = end - tail

        if actual_delta > 0:
            c.testdrive(
                SCHEMA
                + dedent(
                    f"""
                    $ kafka-ingest format=avro topic={self.topic.name} key-format=avro key-schema=${{keyschema}} schema=${{schema}} start-iteration={tail} repeat={actual_delta}
                    {{"key": ${{kafka-ingest.iteration}}}} {{"f1": ${{kafka-ingest.iteration}}, "pad": "{self.pad}"}}
                    """
                )
            )


class KafkaDeleteFromTail(Ingest):
    """Deletes the smallest values previously inserted."""

    def run(self, c: Composition) -> None:
        if self.topic.envelope is Envelope.NONE:
            return

        prev_min = self.topic.watermarks.min
        self.topic.watermarks.min = min(
            prev_min + self.delta, self.topic.watermarks.max
        )
        assert self.topic.watermarks.max >= 0
        assert self.topic.watermarks.min >= 0
        actual_delta = self.topic.watermarks.min - prev_min

        if actual_delta > 0:
            c.testdrive(
                SCHEMA
                + dedent(
                    f"""
                   $ kafka-ingest format=avro topic={self.topic.name} key-format=avro key-schema=${{keyschema}} schema=${{schema}} start-iteration={prev_min} repeat={actual_delta}
                   {{"key": ${{kafka-ingest.iteration}}}}
                   """
                )
            )
