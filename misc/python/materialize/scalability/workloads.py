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
    AddColumnToTableX,
    Connect,
    CreateMvOnTableX,
    CreateTableX,
    Disconnect,
    DropColumnFromTableX,
    DropTableX,
    FillColumnInTableX,
    InsertDefaultValues,
    PopulateTableX,
    SelectCount,
    SelectCountInMv,
    SelectLimit,
    SelectOne,
    SelectStar,
    SelectStarFromMvOnTableX,
    SelectStarFromTableX,
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


class CreateAndDropTableWorkload(WorkloadWithContext):
    def amend_data_before_execution(self, data: OperationData) -> None:
        data.push("table_seed", data.get("worker_id"))

    def operations(self) -> list["Operation"]:
        return [
            OperationChainWithDataExchange(
                [CreateTableX(), PopulateTableX(), SelectStarFromTableX(), DropTableX()]
            )
        ]


class CreateAndDropTableWithMvWorkload(WorkloadWithContext):
    def amend_data_before_execution(self, data: OperationData) -> None:
        data.push("table_seed", data.get("worker_id"))

    def operations(self) -> list["Operation"]:
        return [
            OperationChainWithDataExchange(
                [
                    CreateTableX(),
                    PopulateTableX(),
                    CreateMvOnTableX(),
                    SelectStarFromMvOnTableX(),
                    DropTableX(),
                ]
            )
        ]


class AlterTableColumnsWorkload(WorkloadWithContext):
    def init_operations(self) -> list["Operation"]:
        return [CreateTableX(), PopulateTableX()]

    def amend_data_before_execution(self, data: OperationData) -> None:
        data.push("table_seed", "fixed")
        data.push("column_seed_a", f"{data.get('worker_id')}_a")
        data.push("column_seed_b", f"{data.get('worker_id')}_b")
        data.push("column_seed_c", f"{data.get('worker_id')}_c")

    def operations(self) -> list["Operation"]:
        return [
            OperationChainWithDataExchange(
                [
                    AddColumnToTableX(column_seed_key="column_seed_a"),
                    FillColumnInTableX(column_seed_key="column_seed_a"),
                    AddColumnToTableX(column_seed_key="column_seed_b"),
                    FillColumnInTableX(column_seed_key="column_seed_b"),
                    AddColumnToTableX(column_seed_key="column_seed_c"),
                    FillColumnInTableX(column_seed_key="column_seed_c"),
                    DropColumnFromTableX(column_seed_key="column_seed_a"),
                    DropColumnFromTableX(column_seed_key="column_seed_b"),
                    DropColumnFromTableX(column_seed_key="column_seed_c"),
                ]
            )
        ]
