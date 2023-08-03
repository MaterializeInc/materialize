# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List, Optional

from materialize.output_consistency.execution.evaluation_strategy import (
    DataFlowRenderingEvaluation,
    EvaluationStrategy,
    EvaluationStrategyKey,
)
from materialize.output_consistency.execution.value_storage_layout import (
    ValueStorageLayout,
)
from materialize.output_consistency.input_data.test_input_data import (
    ConsistencyTestInputData,
)
from materialize.output_consistency.selection.selection import (
    DataRowSelection,
    TableColumnByNameSelection,
)


class PgEvaluation(EvaluationStrategy):
    def __init__(self) -> None:
        self.strategy_with_same_logic = DataFlowRenderingEvaluation()
        super().__init__(
            EvaluationStrategyKey.POSTGRES,
            "Postgres evaluation",
            "t_pg",
            "postgres_evaluation",
        )

    def generate_source_for_storage_layout(
        self,
        input_data: ConsistencyTestInputData,
        storage_layout: ValueStorageLayout,
        row_selection: DataRowSelection,
        table_column_selection: TableColumnByNameSelection,
        override_db_object_name: Optional[str] = None,
    ) -> List[str]:
        return self.strategy_with_same_logic.generate_source_for_storage_layout(
            input_data,
            storage_layout,
            row_selection,
            table_column_selection,
            override_db_object_name,
        )
