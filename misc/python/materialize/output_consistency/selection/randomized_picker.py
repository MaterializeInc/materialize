# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random

from materialize.output_consistency.common import probability
from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.data_type.data_type_with_values import (
    DataTypeWithValues,
)
from materialize.output_consistency.data_value.data_value import DataValue
from materialize.output_consistency.operation.operation import (
    DbOperationOrFunction,
    OperationRelevance,
)
from materialize.output_consistency.query.data_source import (
    DataSource,
)
from materialize.output_consistency.query.join import (
    JOIN_TARGET_WEIGHTS,
    JoinOperator,
    JoinTarget,
)


class RandomizedPicker:
    def __init__(self, config: ConsistencyTestConfiguration):
        self.config = config
        random.seed(self.config.random_seed)

    def random_boolean(self, probability_for_true: float = 0.5) -> bool:
        assert (
            0 <= probability_for_true <= 1
        ), f"Invalid probability: {probability_for_true}"

        weights = [probability_for_true, 1 - probability_for_true]
        return random.choices([True, False], k=1, weights=weights)[0]

    def random_number(self, min_value_incl: int, max_value_incl: int) -> int:
        return random.randint(min_value_incl, max_value_incl)

    def random_operation(
        self, operations: list[DbOperationOrFunction], weights: list[float]
    ) -> DbOperationOrFunction:
        return random.choices(operations, k=1, weights=weights)[0]

    def random_type_with_values(
        self, types_with_values: list[DataTypeWithValues]
    ) -> DataTypeWithValues:
        return random.choice(types_with_values)

    def random_row_indices(
        self, vertical_storage_row_count: int, max_number_of_rows_to_select: int
    ) -> set[int]:
        selected_rows = random.choices(
            range(0, vertical_storage_row_count), k=max_number_of_rows_to_select
        )
        return set(selected_rows)

    def random_value(self, values: list[DataValue]) -> DataValue:
        return random.choice(values)

    def random_data_source(self, sources: list[DataSource]) -> DataSource:
        assert len(sources) > 0, "No data sources available"

        if self.random_boolean(
            probability.COLUMN_SELECTION_ADDITIONAL_CHANCE_FOR_FIRST_TABLE
        ):
            # give the first data source a higher chance so that not all queries need a join
            return sources[0]

        return random.choice(sources)

    def convert_operation_relevance_to_number(
        self, relevance: OperationRelevance
    ) -> float:
        if relevance == OperationRelevance.EXTREME_HIGH:
            return 100
        if relevance == OperationRelevance.HIGH:
            return 0.8
        elif relevance == OperationRelevance.DEFAULT:
            return 0.5
        elif relevance == OperationRelevance.LOW:
            return 0.2
        else:
            raise RuntimeError(f"Unexpected value: {relevance}")

    def _random_bool(self, probability: float) -> bool:
        return random.random() < probability

    def random_join_operator(self) -> JoinOperator:
        return random.choice(list(JoinOperator))

    def random_join_target(self) -> JoinTarget:
        return random.choices(list(JoinTarget), k=1, weights=JOIN_TARGET_WEIGHTS)[0]
