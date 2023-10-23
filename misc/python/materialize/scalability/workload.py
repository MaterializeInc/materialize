# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.scalability.operation import Operation


class Workload:
    def operations(self) -> list[Operation]:
        raise NotImplementedError

    def name(self) -> str:
        return self.__class__.__name__


class WorkloadSelfTest(Workload):
    """Used to self-test the framework, so need to be excluded from regular benchmark runs."""

    pass
