# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Dict, List, Optional

from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.data_type.data_type_with_values import (
    DataTypeWithValues,
)
from materialize.output_consistency.execution.value_storage_layout import (
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_with_args import (
    ExpressionWithArgs,
)
from materialize.output_consistency.input_data.test_input_data import (
    ConsistencyTestInputData,
)
from materialize.output_consistency.operation.operation import DbOperationOrFunction
from materialize.output_consistency.operation.operation_param import OperationParam
from materialize.output_consistency.selection.randomized_picker import RandomizedPicker


class ExpressionGenerator:
    """Generates expressions based on a random selection of operations"""

    def __init__(
        self,
        config: ConsistencyTestConfiguration,
        randomized_picker: RandomizedPicker,
        input_data: ConsistencyTestInputData,
    ):
        self.config = config
        self.randomized_picker = randomized_picker
        self.input_data = input_data
        self.selectable_operations: List[DbOperationOrFunction] = []
        self.operation_weights: List[float] = []
        self.operation_weights_no_aggregates: List[float] = []
        self.types_with_values_by_category: Dict[
            DataTypeCategory, List[DataTypeWithValues]
        ] = dict()
        self._initialize_operations()
        self._initialize_types()

    def _initialize_operations(self) -> None:
        for operation in self.input_data.all_operation_types:
            self.selectable_operations.append(operation)
            operation_weight = (
                self.randomized_picker.convert_operation_relevance_to_number(
                    operation.relevance
                )
            )
            self.operation_weights.append(operation_weight)
            self.operation_weights_no_aggregates.append(
                0 if operation.is_aggregation else operation_weight
            )

    def _initialize_types(self) -> None:
        for data_type_with_values in self.input_data.all_data_types_with_values:
            category = data_type_with_values.data_type.category
            types_with_values = self.types_with_values_by_category.get(category, [])
            types_with_values.append(data_type_with_values)
            self.types_with_values_by_category[category] = types_with_values

    def pick_random_operation(self, include_aggregates: bool) -> DbOperationOrFunction:
        weights = (
            self.operation_weights
            if include_aggregates
            else self.operation_weights_no_aggregates
        )

        return self.randomized_picker.random_operation(
            self.selectable_operations, weights
        )

    def generate_expression(
        self,
        operation: DbOperationOrFunction,
    ) -> Optional[Expression]:
        storage_layout = self._select_storage_layout(operation)
        try:
            args = self._generate_args_for_operation(operation, storage_layout)
        except NoSuitableExpressionFound:
            return None

        is_aggregate = operation.is_aggregation or self._contains_aggregate_arg(args)
        is_expect_error = operation.is_expected_to_cause_db_error(args)
        expression = ExpressionWithArgs(operation, args, is_aggregate, is_expect_error)

        return expression

    def _select_storage_layout(
        self, operation: DbOperationOrFunction
    ) -> ValueStorageLayout:
        if not operation.is_aggregation:
            # Non-aggregate expressions can unfortunately only use horizontal layout because the processing order does
            # not seem to be consistent between data-flow rendering and constant folding such that error messages will
            # differ
            return ValueStorageLayout.HORIZONTAL

        # strongly prefer vertical storage for aggregations but allow some variance

        if self.randomized_picker.random_number(0, 9) == 0:
            # Use horizontal layout in 10 % different of the cases
            return ValueStorageLayout.HORIZONTAL

        return ValueStorageLayout.VERTICAL

    def _contains_aggregate_arg(self, args: List[Expression]) -> bool:
        for arg in args:
            if arg.is_aggregate:
                return True

        return False

    def _generate_args_for_operation(
        self,
        operation: DbOperationOrFunction,
        storage_layout: ValueStorageLayout,
        try_number: int = 1,
    ) -> List[Expression]:
        number_of_args = self.randomized_picker.random_number(
            operation.min_param_count, operation.max_param_count
        )

        if number_of_args == 0:
            return []

        args = []

        for arg_index in range(0, number_of_args):
            param = operation.params[arg_index]
            arg = self.generate_arg_for_operation_param(param, storage_layout)
            args.append(arg)

        if (
            self.config.avoid_expressions_expecting_db_error
            and try_number <= 50
            and operation.is_expected_to_cause_db_error(args)
        ):
            return self._generate_args_for_operation(
                operation, storage_layout, try_number=try_number + 1
            )

        return args

    def generate_arg_for_operation_param(
        self, param: OperationParam, storage_layout: ValueStorageLayout
    ) -> Expression:
        # only consider the data type category, do not check incompatibilities and other validations at this point
        suitable_types_with_values = self._get_data_type_values_of_category(
            param.type_category
        )

        if len(suitable_types_with_values) == 0:
            raise NoSuitableExpressionFound()

        type_with_values = self.randomized_picker.random_type_with_values(
            suitable_types_with_values
        )

        if storage_layout == ValueStorageLayout.VERTICAL:
            return type_with_values.create_vertical_storage_column_expression()
        else:
            if len(type_with_values.raw_values) == 0:
                raise NoSuitableExpressionFound()

            return self.randomized_picker.random_value(type_with_values.raw_values)

    def _get_data_type_values_of_category(
        self, category: DataTypeCategory
    ) -> List[DataTypeWithValues]:
        if category == DataTypeCategory.ANY:
            return self.input_data.all_data_types_with_values

        if category == DataTypeCategory.DYNAMIC:
            raise RuntimeError(
                f"Type {DataTypeCategory.DYNAMIC} not allowed for parameters"
            )

        return self.types_with_values_by_category[category]


class NoSuitableExpressionFound(Exception):
    def __init__(self) -> None:
        super().__init__()
