# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Optional

from materialize.checks.actions import Action
from materialize.checks.executors import Executor
from materialize.cloudtest.k8s.environmentd import EnvironmentdStatefulSet
from materialize.util import MzVersion


class ReplaceEnvironmentdStatefulSet(Action):
    """Change the image tag of the environmentd stateful set, re-create the definition and replace the existing one."""

    new_tag: Optional[str]

    def __init__(self, new_tag: Optional[str] = None) -> None:
        self.new_tag = new_tag

    def execute(self, e: Executor) -> None:
        mz = e.cloudtest_application()
        stateful_set = [
            resource
            for resource in mz.resources
            if type(resource) == EnvironmentdStatefulSet
        ]
        assert len(stateful_set) == 1
        stateful_set = stateful_set[0]

        stateful_set.tag = self.new_tag
        stateful_set.replace()
        e.current_mz_version = (
            MzVersion.parse_mz(self.new_tag)
            if self.new_tag
            else MzVersion.parse_cargo()
        )

    def join(self, e: Executor) -> None:
        # execute is blocking already
        pass
