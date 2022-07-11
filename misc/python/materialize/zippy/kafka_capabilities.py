# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum
from typing import Optional

from materialize.zippy.framework import Capability
from materialize.zippy.watermarks import Watermarks


class KafkaRunning(Capability):
    """Kafka is running in the environment."""

    pass


class Envelope(Enum):
    """Kafka envelope to be used for a particular topic or source.

    If the Envelope is NONE, no deletions take place on the topic, just insertions
    """

    NONE = 1
    UPSERT = 2


class TopicExists(Capability):
    """A Topic exists on the Kafka instance."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.envelope: Optional[Envelope] = None
        self.watermarks = Watermarks()
