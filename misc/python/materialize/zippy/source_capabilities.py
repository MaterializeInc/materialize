# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Optional

from materialize.zippy.framework import Capability
from materialize.zippy.kafka_capabilities import TopicExists
from materialize.zippy.watermarks import Watermarks


class SourceExists(Capability):
    """A Kafka source exists in Materialize."""

    def __init__(self, name: str, topic: Optional[TopicExists] = None) -> None:
        self.name = name
        self.topic = topic

    def get_watermarks(self) -> Watermarks:
        assert self.topic is not None
        return self.topic.watermarks
