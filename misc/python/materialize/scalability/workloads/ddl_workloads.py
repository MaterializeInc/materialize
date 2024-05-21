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
    CreateMvOnTableX,
    CreateTableX,
    DropTableX,
    PopulateTableX,
    SelectStarFromMvOnTableX,
    SelectStarFromTableX,
)
from materialize.scalability.workload import (
    WorkloadWithContext,
)
from materialize.scalability.workload_markers import (
    DdlWorkload,
)


class CreateAndDropTableWorkload(WorkloadWithContext, DdlWorkload):
    def amend_data_before_execution(self, data: OperationData) -> None:
        data.push("table_seed", data.get("worker_id"))

    def operations(self) -> list["Operation"]:
        return [
            OperationChainWithDataExchange(
                [CreateTableX(), PopulateTableX(), SelectStarFromTableX(), DropTableX()]
            )
        ]


class CreateAndDropTableWithMvWorkload(WorkloadWithContext, DdlWorkload):
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
