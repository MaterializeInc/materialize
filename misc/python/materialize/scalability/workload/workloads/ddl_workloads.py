# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.scalability.operation.operation_data import OperationData
from materialize.scalability.operation.operations.operations import (
    CreateMvOnTableX,
    CreateTableX,
    CreateViewXOnSeries,
    CreateViewXOnViewOnSeries,
    DropTableX,
    DropViewX,
    PopulateTableX,
    SelectStarFromMvOnTableX,
    SelectStarFromTableX,
)
from materialize.scalability.operation.scalability_operation import (
    Operation,
    OperationChainWithDataExchange,
)
from materialize.scalability.workload.workload import WorkloadWithContext
from materialize.scalability.workload.workload_markers import DdlWorkload


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


class CreateAndReplaceViewWorkload(WorkloadWithContext, DdlWorkload):
    """Creates n materialized views, a materialized view based on these, and then drops all MVs."""

    def amend_data_before_execution(self, data: OperationData) -> None:
        data.push("view_seed", data.get("worker_id"))

    def operations(self) -> list["Operation"]:
        return [
            OperationChainWithDataExchange(
                [
                    CreateViewXOnSeries(
                        materialized=False, additional_name_suffix="_1"
                    ),
                    CreateViewXOnSeries(
                        materialized=False, additional_name_suffix="_2"
                    ),
                    CreateViewXOnSeries(
                        materialized=False, additional_name_suffix="_3"
                    ),
                    CreateViewXOnSeries(
                        materialized=False, additional_name_suffix="_4"
                    ),
                    CreateViewXOnSeries(
                        materialized=False, additional_name_suffix="_5"
                    ),
                    CreateViewXOnViewOnSeries(
                        materialized=False,
                        additional_name_suffix="_merge",
                        suffixes_of_other_views_on_series=[
                            "_1",
                            "_2",
                            "_3",
                            "_4",
                            "_5",
                        ],
                    ),
                    DropViewX(materialized=False, additional_name_suffix="_merge"),
                    DropViewX(materialized=False, additional_name_suffix="_1"),
                    DropViewX(materialized=False, additional_name_suffix="_2"),
                    DropViewX(materialized=False, additional_name_suffix="_3"),
                    DropViewX(materialized=False, additional_name_suffix="_4"),
                    DropViewX(materialized=False, additional_name_suffix="_5"),
                ]
            )
        ]
