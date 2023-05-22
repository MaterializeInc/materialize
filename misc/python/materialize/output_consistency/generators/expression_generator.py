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
from materialize.output_consistency.execution.test_summary import ConsistencyTestLogger
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_with_args import (
    ExpressionWithArgs,
)
from materialize.output_consistency.input_data.operations.all_operations_provider import (
    ALL_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.values.all_values_provider import (
    ALL_DATA_TYPES_WITH_VALUES,
)
from materialize.output_consistency.known_inconsistencies.known_deviation_filter import (
    KnownOutputInconsistenciesFilter,
)
from materialize.output_consistency.operation.operation import DbOperationOrFunction
from materialize.output_consistency.operation.operation_param import OperationParam
from materialize.output_consistency.selection.randomized_picker import RandomizedPicker


class ExpressionGenerator:
    """Generates expressions based on a random selection of operations"""

    def __init__(
        self,
        config: ConsistencyTestConfiguration,
        known_inconsistencies_filter: KnownOutputInconsistenciesFilter,
    ):
        self.config = config
        self.known_inconsistencies_filter = known_inconsistencies_filter
        self.randomized_picker = RandomizedPicker(self.config)
        self.selectable_operations: List[DbOperationOrFunction] = []
        self.operation_weights: List[float] = []
        self.types_with_values_by_category: Dict[
            DataTypeCategory, List[DataTypeWithValues]
        ] = dict()
        self._initialize_operations()
        self._initialize_types()

    def _initialize_operations(self) -> None:
        for operation in ALL_OPERATION_TYPES:
            if operation.aggregation:
                # aggregation functions are not supported yet
                continue

            self.selectable_operations.append(operation)
            self.operation_weights.append(
                self.randomized_picker.convert_operation_relevance_to_number(
                    operation.relevance
                )
            )

    def _initialize_types(self) -> None:
        for data_type_with_values in ALL_DATA_TYPES_WITH_VALUES:
            category = data_type_with_values.data_type.category
            types_with_values = self.types_with_values_by_category.get(category, [])
            types_with_values.append(data_type_with_values)
            self.types_with_values_by_category[category] = types_with_values

    def pick_random_operation(self) -> DbOperationOrFunction:
        return self.randomized_picker.random_operation(
            self.selectable_operations, self.operation_weights
        )

    def generate_expression(
        self,
        operation: DbOperationOrFunction,
        logger: ConsistencyTestLogger,
        try_number: int = 1,
    ) -> Optional[Expression]:
        try:
            args = self.generate_args_for_operation(operation)
        except NoSuitableExpressionFound:
            return None

        is_expect_error = operation.is_expected_to_cause_db_error(args)
        expression = ExpressionWithArgs(operation, args, is_expect_error)

        if self.known_inconsistencies_filter.matches(expression):
            if try_number <= 5:
                if self.config.verbose_output:
                    logger.add_global_warning(
                        f"Skipping expression with known inconsistency: {expression}"
                    )

                return self.generate_expression(
                    operation, logger, try_number=try_number + 1
                )
            else:
                logger.add_global_warning(
                    f"Aborting expression generation for {operation},"
                    f" all tries resulted in known inconsistencies (e.g., {expression})"
                )
                return None

        return expression

    def generate_args_for_operation(
        self, operation: DbOperationOrFunction, try_number: int = 1
    ) -> List[Expression]:
        number_of_args = self.randomized_picker.random_number(
            operation.min_param_count, operation.max_param_count
        )

        if number_of_args == 0:
            return []

        args = []

        for arg_index in range(0, number_of_args):
            param = operation.params[arg_index]
            arg = self.generate_arg_for_operation_param(param)
            args.append(arg)

        if (
            self.config.avoid_expressions_expecting_db_error
            and try_number <= 5
            and operation.is_expected_to_cause_db_error(args)
        ):
            return self.generate_args_for_operation(
                operation, try_number=try_number + 1
            )

        return args

    def generate_arg_for_operation_param(
        self,
        param: OperationParam,
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

        if len(type_with_values.raw_values) == 0:
            raise NoSuitableExpressionFound()

        return self.randomized_picker.random_value(type_with_values.raw_values)

    def _get_data_type_values_of_category(
        self, category: DataTypeCategory
    ) -> List[DataTypeWithValues]:
        if category == DataTypeCategory.ANY:
            return ALL_DATA_TYPES_WITH_VALUES

        if category == DataTypeCategory.DYNAMIC:
            raise RuntimeError(
                f"Type {DataTypeCategory.DYNAMIC} not allowed for parameters"
            )

        return self.types_with_values_by_category[category]


class NoSuitableExpressionFound(Exception):
    def __init__(self) -> None:
        super().__init__()
