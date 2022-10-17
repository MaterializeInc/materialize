# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from typing import List, Optional, Set, Type

from materialize.mzcompose import Composition
from materialize.zippy.framework import Action, Capabilities, Capability
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.replica_capabilities import ReplicaExists, ReplicaSizeType


class DropDefaultReplica(Action):
    """Drops the default replica."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning}

    def run(self, c: Composition) -> None:
        c.testdrive("> DROP CLUSTER REPLICA default.r1;")


class CreateReplica(Action):
    """Creates a replica on the default cluster."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning}

    def __init__(self, capabilities: Capabilities) -> None:
        this_replica = ReplicaExists(name="replica" + str(random.randint(1, 3)))

        existing_replicas = [
            t for t in capabilities.get(ReplicaExists) if t.name == this_replica.name
        ]

        if len(existing_replicas) == 0:
            self.new_replica = True

            size = str(random.choice([1, 2, 4, 8]))
            size_type = random.choice([ReplicaSizeType.Nodes, ReplicaSizeType.Workers])

            if size_type is ReplicaSizeType.Nodes:
                this_replica.size = size + "-1"
            elif size_type is ReplicaSizeType.Workers:
                this_replica.size = size
            else:
                assert False

            if this_replica.size == "1-1":
                this_replica.size = "1"

            self.replica = this_replica
        elif len(existing_replicas) == 1:
            self.new_replica = False
            self.replica = existing_replicas[0]
        else:
            assert False

    def run(self, c: Composition) -> None:
        if self.new_replica:
            c.testdrive(
                f"> CREATE CLUSTER REPLICA default.{self.replica.name} SIZE '{self.replica.size}'"
            )

    def provides(self) -> List[Capability]:
        return [self.replica] if self.new_replica else []


class DropReplica(Action):
    """Drops a replica from the default cluster."""

    replica: Optional[ReplicaExists]

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, ReplicaExists}

    def __init__(self, capabilities: Capabilities) -> None:
        existing_replicas = capabilities.get(ReplicaExists)

        if len(existing_replicas) > 1:
            self.replica = random.choice(existing_replicas)
            capabilities.remove_capability_instance(self.replica)
        else:
            self.replica = None

    def run(self, c: Composition) -> None:
        if self.replica is not None:
            c.testdrive(f"> DROP CLUSTER REPLICA IF EXISTS default.{self.replica.name}")
