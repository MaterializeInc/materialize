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
from materialize.scalability.workload import (
    WorkloadWithContext,
)
from materialize.scalability.workload_markers import (
    ConnectionWorkload,
    DmlDqlWorkload,
)


class InsertWorkload(DmlDqlWorkload):
    def operations(self) -> list["Operation"]:
        return [InsertDefaultValues()]


class SelectOneWorkload(DmlDqlWorkload):
    def operations(self) -> list["Operation"]:
        return [SelectOne()]


class SelectStarWorkload(DmlDqlWorkload):
    def operations(self) -> list["Operation"]:
        return [SelectStar()]


class SelectLimitWorkload(DmlDqlWorkload):
    def operations(self) -> list["Operation"]:
        return [SelectLimit()]


class SelectCountWorkload(DmlDqlWorkload):
    def operations(self) -> list["Operation"]:
        return [SelectCount()]


class SelectUnionAllWorkload(DmlDqlWorkload):
    def operations(self) -> list["Operation"]:
        return [SelectUnionAll()]


class InsertAndSelectCountInMvWorkload(DmlDqlWorkload):
    def operations(self) -> list["Operation"]:
        return [InsertDefaultValues(), SelectCountInMv()]


class InsertAndSelectLimitWorkload(DmlDqlWorkload):
    def operations(self) -> list["Operation"]:
        return [InsertDefaultValues(), SelectLimit()]


class UpdateWorkload(DmlDqlWorkload):
    def operations(self) -> list["Operation"]:
        return [Update()]


class EstablishConnectionWorkload(WorkloadWithContext, ConnectionWorkload):
    def amend_data_before_execution(self, data: OperationData) -> None:
        data.push("endpoint", self.endpoint)
        data.push("schema", self.schema)
        data.remove("cursor")

    def operations(self) -> list["Operation"]:
        return [OperationChainWithDataExchange([Connect(), SelectOne(), Disconnect()])]
