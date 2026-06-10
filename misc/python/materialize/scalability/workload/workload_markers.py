# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.scalability.workload.workload import Workload


class WorkloadMarker(Workload):
    """Workload marker to group workloads."""

    def group_name(self) -> str:
        raise NotImplementedError


class DmlDqlWorkload(WorkloadMarker):
    """Workloads that only run DML & DQL statements."""

    def group_name(self) -> str:
        return "DML & DQL"


class DdlWorkload(WorkloadMarker):
    """Workloads that run DDL statements."""

    def group_name(self) -> str:
        return "DDL"


class ConnectionWorkload(WorkloadMarker):
    """Workloads that perform connection operations."""

    def group_name(self) -> str:
        return "Connection"


class SelfTestWorkload(WorkloadMarker):
    """Used to self-test the framework, not relevant for regular benchmark runs."""

    def group_name(self) -> str:
        return "Self-Test"
