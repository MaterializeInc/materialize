# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.output_consistency.data_type.data_provider import DATA_TYPES
from materialize.output_consistency.data_type.data_type import RawValue
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expressions.expression import Expression
from materialize.output_consistency.expressions.expression_with_args import (
    ExpressionWithNArgs,
)
from materialize.output_consistency.operations.operation import DbOperationOrFunction
from materialize.output_consistency.operations.operation_provider import OPERATION_TYPES


class ExpressionGenerator:
    def generate_expressions(self) -> list[Expression]:
        expressions: list[Expression] = []

        for data_type in DATA_TYPES:

            if len(data_type.raw_values) == 0:
                continue

            for operation in OPERATION_TYPES:
                if operation.aggregation:
                    # currently not supported
                    continue

                # TODO: data_type of all data types combined with data_type of all data types
                combinations = self.generate_combinations(
                    data_type.raw_values,
                    offset_value=data_type.raw_values[0],
                    length=operation.max_param_count,
                    with_self=True,
                    with_earlier=not operation.commutative,
                )

                for combination in combinations:
                    if not self.satisfies_data_types(operation, combination):
                        continue

                    expected_db_error = self.is_expected_to_cause_error(
                        operation, combination
                    )

                    if expected_db_error:
                        # exclude for now, handle separately later
                        continue

                    expression = ExpressionWithNArgs(
                        operation, args=combination, is_expect_error=expected_db_error
                    )
                    expressions.append(expression)

        return expressions

    def generate_combinations(
        self,
        values: list[RawValue],
        offset_value: RawValue,
        length: int,
        with_self: bool = True,
        with_earlier: bool = True,
    ) -> list[list[Expression]]:
        if length == 0:
            return []

        value_combinations: list[list[Expression]] = []
        start_reached = False

        for current_value in values:
            if current_value == offset_value:
                start_reached = True

            if not start_reached and not with_earlier:
                continue

            if current_value == offset_value and not with_self:
                continue

            if length == 1:
                value_combinations.append([current_value])
            else:
                new_combinations = self.generate_combinations(
                    values, current_value, length - 1, with_self, with_earlier
                )

                for new_combination in new_combinations:
                    entry: list[Expression] = [current_value]
                    entry.extend(new_combination)
                    value_combinations.append(entry)

        return value_combinations

    # checks if the data type is appropriate for the operation
    def satisfies_data_types(
        self, operation: DbOperationOrFunction, args: list[Expression]
    ) -> bool:
        operation.validate_args_count_in_range(len(args))

        for param_index in range(operation.max_param_count):
            param = operation.params[param_index]
            arg = args[param_index]

            if param.type_category == DataTypeCategory.ANY:
                # param ANY accepts arguments of all types
                continue
            if param.type_category == DataTypeCategory.DYNAMIC:
                raise RuntimeError(
                    f"Type {DataTypeCategory.DYNAMIC} not allowed for parameters"
                )

            arg_type_category = arg.resolve_data_type_category()

            if arg_type_category == DataTypeCategory.ANY:
                raise RuntimeError(
                    f"Type {DataTypeCategory.ANY} not allowed for arguments"
                )
            if arg_type_category == DataTypeCategory.DYNAMIC:
                raise RuntimeError(
                    f"Type {DataTypeCategory.DYNAMIC} must be resolved based on the expression"
                )

            if param.type_category != arg_type_category:
                # Type mismatch
                return False

        return True

    # checks incompatibilities (e.g., division by zero) and potential error scenarios (e.g., addition of two max data_type)
    def is_expected_to_cause_error(
        self, operation: DbOperationOrFunction, args: list[Expression]
    ) -> bool:
        operation.validate_args_count_in_range(len(args))

        for validator in operation.args_validators:
            if validator.is_expected_to_cause_error(args):
                return True

        for arg_index, arg in enumerate(args):
            param = operation.params[arg_index]

            if not param.supports_arg(arg):
                return True

        return False
