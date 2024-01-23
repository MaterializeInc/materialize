# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.scalability.operation import Operation, OperationChainWithDataExchange
from materialize.scalability.operation_data import OperationData
from materialize.scalability.operations import (
    Connect,
    Disconnect,
    InsertDefaultValues,
    SelectCount,
    SelectCountInMv,
    SelectLimit,
    SelectOne,
    SelectStar,
    SelectUnionAll,
    Update,
)
from materialize.scalability.workload import Workload, WorkloadWithContext


class InsertWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [InsertDefaultValues()]


class SelectOneWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [SelectOne()]


class SelectStarWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [SelectStar()]


class SelectLimitWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [SelectLimit()]


class SelectCountWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [SelectCount()]


class SelectUnionAllWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [SelectUnionAll()]


class InsertAndSelectCountInMvWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [InsertDefaultValues(), SelectCountInMv()]


class InsertAndSelectLimitWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [InsertDefaultValues(), SelectLimit()]


class UpdateWorkload(Workload):
    def operations(self) -> list["Operation"]:
        return [Update()]


class EstablishConnectionWorkload(WorkloadWithContext):
    def amend_data_before_execution(self, data: OperationData) -> None:
        data.push("endpoint", self.endpoint)
        data.push("schema", self.schema)
        data.remove("cursor")

    def operations(self) -> list["Operation"]:
        return [OperationChainWithDataExchange([Connect(), SelectOne(), Disconnect()])]
