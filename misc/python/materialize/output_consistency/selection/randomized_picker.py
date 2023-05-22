# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from typing import List

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


class RandomizedPicker:
    def __init__(self, config: ConsistencyTestConfiguration):
        self.config = config
        random.seed(self.config.random_seed)

    def random_number(self, min_value_incl: int, max_value_incl: int) -> int:
        return random.randint(min_value_incl, max_value_incl)

    def random_operation(
        self, operations: List[DbOperationOrFunction], weights: List[float]
    ) -> DbOperationOrFunction:
        return random.choices(operations, k=1, weights=weights)[0]

    def random_type_with_values(
        self, types_with_values: List[DataTypeWithValues]
    ) -> DataTypeWithValues:
        return random.choice(types_with_values)

    def random_value(self, values: List[DataValue]) -> DataValue:
        return random.choice(values)

    def convert_operation_relevance_to_number(
        self, relevance: OperationRelevance
    ) -> float:
        if relevance == OperationRelevance.HIGH:
            return 0.8
        elif relevance == OperationRelevance.NORMAL:
            return 0.5
        elif relevance == OperationRelevance.LOW:
            return 0.2
        else:
            raise RuntimeError(f"Unexpected value: {relevance}")

    def _random_bool(self, probability: float) -> bool:
        return random.random() < probability
