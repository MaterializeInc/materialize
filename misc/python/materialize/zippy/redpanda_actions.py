# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

from materialize.mzcompose import Composition
from materialize.zippy.framework import Action, Capability
from materialize.zippy.kafka_capabilities import KafkaRunning


class RedpandaStart(Action):
    """Start a Kafka instance."""

    def provides(self) -> List[Capability]:
        return [KafkaRunning()]

    def run(self, c: Composition) -> None:
        c.up("redpanda")
