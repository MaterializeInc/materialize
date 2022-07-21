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
            {"name":"f1", "type":"long"}
        ]
    }
"""


class KafkaStart(Action):
    """Start a Kafka instance."""

    def provides(self) -> List[Capability]:
        return [KafkaRunning()]

    def run(self, c: Composition) -> None:
        c.start_and_wait_for_tcp(services=["kafka"])


class KafkaStop(Action):
    """Stop the Kafka instance."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {KafkaRunning}

    def removes(self) -> Set[Type[Capability]]:
        return {KafkaRunning}

    def run(self, c: Composition) -> None:
        c.kill("kafka")


class CreateTopic(Action):
    """Creates a Kafka topic and decides on the envelope that will be used."""

    @classmethod
    def requires(cls) -> Set[Type[Capability]]:
        return {MzIsRunning, KafkaRunning}

    def __init__(self, capabilities: Capabilities) -> None:
        this_topic = TopicExists(
            name="topic" + str(random.randint(1, 10)),
            envelope=random.choice([Envelope.NONE, Envelope.UPSERT]),
            partitions=random.randint(1, 10),
        )
        existing_topics = [
            t for t in capabilities.get(TopicExists) if t.name == this_topic.name
        ]

        if len(existing_topics) == 0:
            self.new_topic = True
            self.topic = this_topic
        elif len(existing_topics) == 1:
            self.new_topic = False
            self.topic = existing_topics[0]
        else:
            assert False

    def provides(self) -> List[Capability]:
        return [self.topic] if self.new_topic else []

    def run(self, c: Composition) -> None:
        if self.new_topic:
            c.testdrive(
                f"""
$ kafka-create-topic topic={self.topic.name} partitions={self.topic.partitions}

{SCHEMA}

$ kafka-ingest format=avro key-format=avro topic={self.topic.name} schema=${{schema}} key-schema=${{keyschema}} publish=true repeat=1
{{"key": 0}} {{"f1": 0}}
"""
            )


class Ingest(Action):
    """Ingests data (inserts, updates or deletions) into a Kafka topic."""

    @classmethod
    def requires(cls) -> Set[Type[Capability]]:
        return {MzIsRunning, KafkaRunning, TopicExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.topic = random.choice(capabilities.get(TopicExists))
        self.delta = random.randint(1, 100000)


class KafkaInsert(Ingest):
    """Inserts data into a Kafka topic."""

    def run(self, c: Composition) -> None:
        prev_max = self.topic.watermarks.max
        self.topic.watermarks.max = prev_max + self.delta
        assert self.topic.watermarks.max >= 0
        assert self.topic.watermarks.min >= 0
        c.testdrive(
            f"""
{SCHEMA}

$ kafka-ingest format=avro key-format=avro topic={self.topic.name} schema=${{schema}} key-schema=${{keyschema}} start-iteration={prev_max + 1} publish=true repeat={self.delta}
{{"key": ${{kafka-ingest.iteration}}}} {{"f1": ${{kafka-ingest.iteration}}}}
"""
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
                f"""
{SCHEMA}

$ kafka-ingest format=avro topic={self.topic.name} key-format=avro key-schema=${{keyschema}} schema=${{schema}} start-iteration={self.topic.watermarks.max + 1} publish=true repeat={actual_delta}
{{"key": ${{kafka-ingest.iteration}}}}
"""
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
                f"""
{SCHEMA}

$ kafka-ingest format=avro topic={self.topic.name} key-format=avro key-schema=${{keyschema}} schema=${{schema}} start-iteration={prev_min} publish=true repeat={actual_delta}
{{"key": ${{kafka-ingest.iteration}}}}
"""
            )
