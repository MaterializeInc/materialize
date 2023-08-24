# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.scalability.operation import Operation
from materialize.scalability.operations import (
    InsertDefaultValues,
    SelectCount,
    SelectOne,
    SelectStar,
    SelectUnionAll,
    Update,
)
from materialize.scalability.workload import Workload


class InsertWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [InsertDefaultValues()]


class SelectOneWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [SelectOne()]


class SelectStarWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [SelectStar()]


class SelectCountWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [SelectCount()]


class SelectUnionAllWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [SelectUnionAll()]


class InsertAndSelectCountWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [InsertDefaultValues(), SelectCount()]


class InsertAndSelectStarWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [InsertDefaultValues(), SelectStar()]


class UpdateWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [Update()]
