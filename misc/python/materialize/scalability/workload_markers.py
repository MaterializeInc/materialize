# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.scalability.workload import Workload


class WorkloadMarker(Workload):
    """Workload marker to group workloads."""

    pass


class DmlDqlWorkload(WorkloadMarker):
    """Workloads that only run DML & DQL statements."""

    pass


class DdlWorkload(WorkloadMarker):
    """Workloads that run DDL statements."""

    pass


class ConnectionWorkload(WorkloadMarker):
    """Workloads that perform connection operations."""

    pass


class SelfTestWorkload(WorkloadMarker):
    """Used to self-test the framework, not relevant for regular benchmark runs."""

    pass
