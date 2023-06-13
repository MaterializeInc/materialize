# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Callable, Dict, List, Optional

from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.data_type.data_type_with_values import (
    DataTypeWithValues,
)
from materialize.output_consistency.enum.enum_operation_param import (
    EnumConstantOperationParam,
)
from materialize.output_consistency.execution.value_storage_layout import (
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression import (
    Expression,
    LeafExpression,
)
from materialize.output_consistency.expression.expression_with_args import (
    ExpressionWithArgs,
)
from materialize.output_consistency.input_data.test_input_data import (
    ConsistencyTestInputData,
)
from materialize.output_consistency.operation.operation import DbOperationOrFunction
from materialize.output_consistency.operation.operation_param import OperationParam
from materialize.output_consistency.selection.randomized_picker import RandomizedPicker

NESTING_LEVEL_ROOT = 0
NESTING_LEVEL_OUTERMOST_ARG = 1
FIRST_ARG_INDEX = 0


class ArgContext:
    def __init__(self) -> None:
        self.args: List[Expression] = []
        self.contains_aggregation = False

    def append(self, arg: Expression) -> None:
        self.args.append(arg)

        if arg.is_aggregate:
            self.contains_aggregation = True

    def has_no_args(self) -> bool:
        return len(self.args) == 0

    def requires_aggregation(self) -> bool:
        return self.contains_aggregation


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
        self.operations_by_return_type_category: Dict[
            DataTypeCategory, List[DbOperationOrFunction]
        ] = dict()
        self.types_with_values_by_category: Dict[
            DataTypeCategory, List[DataTypeWithValues]
        ] = dict()
        self._initialize_operations()
        self._initialize_types()

    def _initialize_operations(self) -> None:
        self.operation_weights = self._get_operation_weights(
            self.input_data.all_operation_types
        )

        for index, operation in enumerate(self.input_data.all_operation_types):
            self.selectable_operations.append(operation)
            self.operation_weights_no_aggregates.append(
                0 if operation.is_aggregation else self.operation_weights[index]
            )

            category = operation.return_type_spec.type_category
            operations_with_return_category = (
                self.operations_by_return_type_category.get(category, [])
            )
            operations_with_return_category.append(operation)
            self.operations_by_return_type_category[
                category
            ] = operations_with_return_category

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
        storage_layout: Optional[ValueStorageLayout] = None,
        nesting_level: int = NESTING_LEVEL_ROOT,
    ) -> Optional[ExpressionWithArgs]:
        if storage_layout is None:
            storage_layout = self._select_storage_layout(operation)

        try:
            args = self._generate_args_for_operation(
                operation, storage_layout, nesting_level + 1
            )
        except NoSuitableExpressionFound as ex:
            if self.config.verbose_output:
                print(f"No suitable expression found: {ex.message}")
            return None

        is_aggregate = operation.is_aggregation or self._contains_aggregate_arg(args)
        is_expect_error = operation.is_expected_to_cause_db_error(args)
        expression = ExpressionWithArgs(operation, args, is_aggregate, is_expect_error)

        return expression

    def _select_storage_layout(
        self, operation: DbOperationOrFunction
    ) -> ValueStorageLayout:
        if not operation.is_aggregation:
            # Prefer the horizontal row format for non-aggregate expressions. (It makes it less likely that a query
            # results in (an unexpected) error. Furthermore, in case of an error, error messages of non-aggregate
            # expressions can only be compared in HORIZONTAL layout (because the row processing order of an
            # evaluation strategy is not defined).)
            if self.randomized_picker.random_boolean(0.9):
                return ValueStorageLayout.HORIZONTAL
            else:
                return ValueStorageLayout.VERTICAL

        # strongly prefer vertical storage for aggregations but allow some variance

        if self.randomized_picker.random_boolean(0.1):
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
        nesting_level: int,
        try_number: int = 1,
    ) -> List[Expression]:
        number_of_args = self.randomized_picker.random_number(
            operation.min_param_count, operation.max_param_count
        )

        if number_of_args == 0:
            return []

        arg_context = ArgContext()

        for arg_index in range(FIRST_ARG_INDEX, number_of_args):
            param = operation.params[arg_index]
            # nesting_level was already incremented before invoking this function
            arg = self._generate_arg_for_param(
                operation,
                param,
                storage_layout,
                arg_context,
                nesting_level,
            )
            arg_context.append(arg)

        if (
            self.config.avoid_expressions_expecting_db_error
            and try_number <= 50
            and operation.is_expected_to_cause_db_error(arg_context.args)
        ):
            # retry
            return self._generate_args_for_operation(
                operation,
                storage_layout,
                nesting_level=nesting_level,
                try_number=try_number + 1,
            )

        return arg_context.args

    def _generate_arg_for_param(
        self,
        operation: DbOperationOrFunction,
        param: OperationParam,
        storage_layout: ValueStorageLayout,
        arg_context: ArgContext,
        nesting_level: int,
    ) -> Expression:
        if isinstance(param, EnumConstantOperationParam):
            return self._pick_enum_constant(param)

        create_complex_arg = (
            arg_context.requires_aggregation()
            or self.randomized_picker.random_boolean(0.2)
        )

        if create_complex_arg:
            return self._generate_complex_arg_for_param(
                param,
                storage_layout,
                arg_context,
                operation.is_aggregation,
                nesting_level,
            )
        else:
            return self._generate_simple_arg_for_param(
                param, arg_context, storage_layout
            )

    def _pick_enum_constant(self, param: EnumConstantOperationParam) -> Expression:
        enum_constant_index = self.randomized_picker.random_number(
            0, len(param.values) - 1
        )
        return param.get_enum_constant(enum_constant_index)

    def _generate_simple_arg_for_param(
        self,
        param: OperationParam,
        arg_context: ArgContext,
        storage_layout: ValueStorageLayout,
    ) -> LeafExpression:
        # only consider the data type category, do not check incompatibilities and other validations at this point
        suitable_types_with_values = self._get_data_type_values_of_category(
            param, arg_context
        )

        if len(suitable_types_with_values) == 0:
            raise NoSuitableExpressionFound("No suitable type")

        type_with_values = self.randomized_picker.random_type_with_values(
            suitable_types_with_values
        )

        if storage_layout == ValueStorageLayout.VERTICAL:
            return type_with_values.create_vertical_storage_column()
        elif storage_layout == ValueStorageLayout.HORIZONTAL:
            if len(type_with_values.raw_values) == 0:
                raise NoSuitableExpressionFound("No value in type")

            return self.randomized_picker.random_value(type_with_values.raw_values)
        else:
            raise RuntimeError(f"Unsupported storage layout: {storage_layout}")

    def _generate_complex_arg_for_param(
        self,
        param: OperationParam,
        storage_layout: ValueStorageLayout,
        arg_context: ArgContext,
        is_aggregation_operation: bool,
        nesting_level: int,
        try_number: int = 1,
    ) -> ExpressionWithArgs:
        must_use_aggregation = arg_context.requires_aggregation()

        # currently allow an aggregation function as argument if all applies:
        # * the operation is not an aggregation (nested aggregations are impossible)
        # * it is first param (all consecutive params with require aggregation)
        # * we are not already nested (to avoid nested aggregations spread across several levels)
        allow_aggregation = must_use_aggregation or (
            not is_aggregation_operation
            and arg_context.has_no_args()
            and nesting_level == NESTING_LEVEL_OUTERMOST_ARG
        )

        suitable_operations = self._get_operations_of_category(
            param, arg_context, must_use_aggregation, allow_aggregation
        )

        if len(suitable_operations) == 0:
            raise NoSuitableExpressionFound(
                f"No suitable operation for {param}"
                f" (layout={storage_layout},"
                f" allow_aggregation={allow_aggregation},"
                f" must_use_aggregation={must_use_aggregation})"
            )

        weights = self._get_operation_weights(suitable_operations)
        operation = self.randomized_picker.random_operation(
            suitable_operations, weights
        )

        nested_expression = self.generate_expression(
            operation, storage_layout, nesting_level
        )

        if nested_expression is None:
            raise NoSuitableExpressionFound(
                f"No nested expression for {param} in {storage_layout}"
            )

        data_type = nested_expression.try_resolve_exact_data_type()

        is_unsupported = data_type is not None and not param.supports_type(
            data_type, arg_context.args
        )
        is_unsupported = (
            is_unsupported
            or not param.might_support_as_input_assuming_category_matches(
                nested_expression.operation.return_type_spec
            )
        )

        if is_unsupported:
            if try_number < 5:
                return self._generate_complex_arg_for_param(
                    param,
                    storage_layout,
                    arg_context,
                    is_aggregation_operation,
                    nesting_level,
                    try_number=try_number + 1,
                )
            else:
                raise NoSuitableExpressionFound("No supported data type")

        return nested_expression

    def _get_data_type_values_of_category(
        self, param: OperationParam, arg_context: ArgContext
    ) -> List[DataTypeWithValues]:
        category = param.resolve_type_category(arg_context.args)
        if category == DataTypeCategory.ANY:
            return self.input_data.all_data_types_with_values

        self._assert_valid_type_category_for_param(param, category)

        preselected_types_with_values = self.types_with_values_by_category.get(
            category, []
        )
        suitable_types_with_values = []

        for type_with_values in preselected_types_with_values:
            if param.supports_type(type_with_values.data_type, arg_context.args):
                suitable_types_with_values.append(type_with_values)

        return suitable_types_with_values

    def _assert_valid_type_category_for_param(
        self, param: OperationParam, category: DataTypeCategory
    ) -> None:
        assert category not in {
            DataTypeCategory.DYNAMIC,
        }, f"Type category {category} not allowed for parameters (param={param})"

    def _get_operations_of_category(
        self,
        param: OperationParam,
        arg_context: ArgContext,
        must_use_aggregation: bool,
        allow_aggregation: bool,
    ) -> List[DbOperationOrFunction]:
        category = param.resolve_type_category(arg_context.args)
        suitable_operations = self._get_all_operations_of_category(param, category)
        if must_use_aggregation:
            return self._get_only_aggregate_operations(suitable_operations)
        elif not allow_aggregation:
            return self._get_without_aggregate_operations(suitable_operations)
        else:
            return suitable_operations

    def _get_all_operations_of_category(
        self, param: OperationParam, category: DataTypeCategory
    ) -> List[DbOperationOrFunction]:
        if category == DataTypeCategory.ANY:
            return self.input_data.all_operation_types

        self._assert_valid_type_category_for_param(param, category)

        return self.operations_by_return_type_category.get(category, [])

    def _get_without_aggregate_operations(
        self, operations: List[DbOperationOrFunction]
    ) -> List[DbOperationOrFunction]:
        return self._get_operations_with_filter(
            operations, lambda op: not op.is_aggregation
        )

    def _get_only_aggregate_operations(
        self, operations: List[DbOperationOrFunction]
    ) -> List[DbOperationOrFunction]:
        return self._get_operations_with_filter(
            operations, lambda op: op.is_aggregation
        )

    def _get_operations_with_filter(
        self,
        operations: List[DbOperationOrFunction],
        op_filter: Callable[[DbOperationOrFunction], bool],
    ) -> List[DbOperationOrFunction]:
        matching_operations = []
        for operation in operations:
            if op_filter(operation):
                matching_operations.append(operation)

        return matching_operations

    def _get_operation_weights(
        self, operations: List[DbOperationOrFunction]
    ) -> List[float]:
        weights = []

        for operation in operations:
            weight = self.randomized_picker.convert_operation_relevance_to_number(
                operation.relevance
            )
            weights.append(weight)

        return weights


class NoSuitableExpressionFound(Exception):
    def __init__(self, message: str):
        super().__init__()
        self.message = message
