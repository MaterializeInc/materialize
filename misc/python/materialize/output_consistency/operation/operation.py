# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum

from materialize.mz_version import MzVersion
from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.operation_args_validator import (
    OperationArgsValidator,
)
from materialize.output_consistency.operation.operation_param import OperationParam
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec

EXPRESSION_PLACEHOLDER = "$"


class OperationRelevance(Enum):
    # for testing
    EXTREME_HIGH = 1
    HIGH = 2
    DEFAULT = 3
    LOW = 4


class DbOperationOrFunction:
    """Base class of `DbOperation` and `DbFunction`"""

    def __init__(
        self,
        params: list[OperationParam],
        min_param_count: int,
        max_param_count: int,
        return_type_spec: ReturnTypeSpec,
        args_validators: set[OperationArgsValidator] | None = None,
        is_aggregation: bool = False,
        is_table_function: bool = False,
        relevance: OperationRelevance = OperationRelevance.DEFAULT,
        comment: str | None = None,
        is_enabled: bool = True,
        is_pg_compatible: bool = True,
        tags: set[str] | None = None,
        since_mz_version: MzVersion | None = None,
    ):
        """
        :param is_enabled: an operation should only be disabled if its execution causes problems;
                            if it just fails, it should be ignored
        """
        if args_validators is None:
            args_validators = set()

        self.params = params
        self.min_param_count = min_param_count
        self.max_param_count = max_param_count
        self.return_type_spec = return_type_spec
        self.args_validators: set[OperationArgsValidator] = args_validators
        self.is_aggregation = is_aggregation
        self.is_table_function = is_table_function
        self.relevance = relevance
        self.comment = comment
        self.is_enabled = is_enabled
        self.is_pg_compatible = is_pg_compatible
        self.tags = tags
        self.since_mz_version = since_mz_version
        self.added_characteristics: set[ExpressionCharacteristics] = set()

    def to_pattern(self, args_count: int) -> str:
        raise NotImplementedError

    def validate_args_count_in_range(self, args_count: int) -> None:
        if args_count < self.min_param_count:
            raise RuntimeError(
                f"To few arguments (got {args_count}, expected at least {self.min_param_count})"
            )
        if args_count > self.max_param_count:
            raise RuntimeError(
                f"To many arguments (got {args_count}, expected at most {self.max_param_count})"
            )

    def derive_characteristics(
        self, args: list[Expression]
    ) -> set[ExpressionCharacteristics]:
        return self.added_characteristics

    def __str__(self) -> str:
        raise NotImplementedError

    def operation_type_name(self) -> str:
        raise NotImplementedError

    def to_description(self, param_count: int) -> str:
        assert self.min_param_count <= param_count <= self.max_param_count
        return f"{self.operation_type_name()} '{self._to_pattern_with_named_params(param_count)}'"

    def _to_pattern_with_named_params(self, param_count: int) -> str:
        pattern = self.to_pattern(param_count)

        for i in range(param_count):
            pattern = pattern.replace("$", self.params[i].__class__.__name__, 1)

        return pattern

    def is_tagged(self, tag: str) -> bool:
        if self.tags is None:
            return False

        return tag in self.tags

    def try_resolve_exact_data_type(self, args: list[Expression]) -> DataType | None:
        return None

    def is_expected_to_cause_db_error(self, args: list[Expression]) -> bool:
        """checks incompatibilities (e.g., division by zero) and potential error scenarios (e.g., addition of two max
        data_type)
        """

        self.validate_args_count_in_range(len(args))

        for validator in self.args_validators:
            if validator.is_expected_to_cause_error(args):
                return True

        for param, arg in zip(self.params, args):
            if not param.supports_expression(arg):
                return True

        return False

    def count_variants(self) -> int:
        return self.max_param_count - self.min_param_count + 1


class DbOperation(DbOperationOrFunction):
    """A database operation (e.g., `a + b`)"""

    def __init__(
        self,
        pattern: str,
        params: list[OperationParam],
        return_type_spec: ReturnTypeSpec,
        args_validators: set[OperationArgsValidator] | None = None,
        relevance: OperationRelevance = OperationRelevance.DEFAULT,
        comment: str | None = None,
        is_enabled: bool = True,
        is_pg_compatible: bool = True,
        tags: set[str] | None = None,
        since_mz_version: MzVersion | None = None,
    ):
        param_count = len(params)
        super().__init__(
            params,
            min_param_count=param_count,
            max_param_count=param_count,
            return_type_spec=return_type_spec,
            args_validators=args_validators,
            is_aggregation=False,
            relevance=relevance,
            comment=comment,
            is_enabled=is_enabled,
            is_pg_compatible=is_pg_compatible,
            tags=tags,
            since_mz_version=since_mz_version,
        )
        self.pattern = pattern

        if param_count != self.pattern.count(EXPRESSION_PLACEHOLDER):
            raise RuntimeError(
                f"Operation has pattern {self.pattern} but has only {param_count} parameters"
            )

    def to_pattern(self, args_count: int) -> str:
        self.validate_args_count_in_range(args_count)
        # wrap in parentheses
        return f"({self.pattern})"

    def __str__(self) -> str:
        comment = f" (comment: {self.comment})" if self.comment is not None else ""
        return f"DbOperation: {self.pattern}{comment}"

    def operation_type_name(self) -> str:
        return "operation"


class DbFunction(DbOperationOrFunction):
    """A database function (e.g., `SUM(x)`)"""

    def __init__(
        self,
        function_name: str,
        params: list[OperationParam],
        return_type_spec: ReturnTypeSpec,
        args_validators: set[OperationArgsValidator] | None = None,
        is_aggregation: bool = False,
        is_table_function: bool = False,
        relevance: OperationRelevance = OperationRelevance.DEFAULT,
        comment: str | None = None,
        is_enabled: bool = True,
        is_pg_compatible: bool = True,
        tags: set[str] | None = None,
        since_mz_version: MzVersion | None = None,
    ):
        self.validate_params(params)

        super().__init__(
            params,
            min_param_count=self.get_min_param_count(params),
            max_param_count=len(params),
            return_type_spec=return_type_spec,
            args_validators=args_validators,
            is_aggregation=is_aggregation,
            is_table_function=is_table_function,
            relevance=relevance,
            comment=comment,
            is_enabled=is_enabled,
            is_pg_compatible=is_pg_compatible,
            tags=tags,
            since_mz_version=since_mz_version,
        )
        self.function_name_in_lower_case = function_name.lower()

    def validate_params(self, params: list[OperationParam]) -> None:
        optional_param_seen = False

        for param in params:
            if optional_param_seen and not param.optional:
                raise RuntimeError("Optional parameters must be at the end")

            if param.optional:
                optional_param_seen = True

    def get_min_param_count(self, params: list[OperationParam]) -> int:
        for index, param in enumerate(params):
            if param.optional:
                return index

        return len(params)

    def to_pattern(self, args_count: int) -> str:
        self.validate_args_count_in_range(args_count)
        args_pattern = ", ".join(["$"] * args_count)
        return f"{self.function_name_in_lower_case}({args_pattern})"

    def __str__(self) -> str:
        comment = f" (comment: {self.comment})" if self.comment is not None else ""
        return f"DbFunction: {self.function_name_in_lower_case}{comment}"

    def operation_type_name(self) -> str:
        return "function"


class DbFunctionWithCustomPattern(DbFunction):
    def __init__(
        self,
        function_name: str,
        pattern_per_param_count: dict[int, str],
        params: list[OperationParam],
        return_type_spec: ReturnTypeSpec,
        args_validators: set[OperationArgsValidator] | None = None,
        is_aggregation: bool = False,
        is_table_function: bool = False,
        relevance: OperationRelevance = OperationRelevance.DEFAULT,
        comment: str | None = None,
        is_enabled: bool = True,
        tags: set[str] | None = None,
    ):
        super().__init__(
            function_name,
            params,
            return_type_spec,
            args_validators=args_validators,
            is_aggregation=is_aggregation,
            is_table_function=is_table_function,
            relevance=relevance,
            comment=comment,
            is_enabled=is_enabled,
            tags=tags,
        )
        self.pattern_per_param_count = pattern_per_param_count
        self.min_param_count = min(pattern_per_param_count.keys())
        self.max_param_count = max(pattern_per_param_count.keys())

    def to_pattern(self, args_count: int) -> str:
        self.validate_args_count_in_range(args_count)

        if args_count not in self.pattern_per_param_count:
            raise RuntimeError(
                f"No pattern specified for {self.function_name_in_lower_case} with {args_count} params"
            )

        return self.pattern_per_param_count[args_count]

    def count_variants(self) -> int:
        return len(self.pattern_per_param_count)


def match_function_by_name(
    op: DbOperationOrFunction, function_name_in_lower_case: str
) -> bool:
    return (
        isinstance(op, DbFunction)
        and op.function_name_in_lower_case == function_name_in_lower_case
    )
