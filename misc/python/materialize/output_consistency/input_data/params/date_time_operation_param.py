# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Optional, Set

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.return_specs.date_time_return_spec import (
    DateTimeReturnTypeSpec,
)
from materialize.output_consistency.input_data.types.date_time_types_provider import (
    DATE_TYPE_IDENTIFIER,
    INTERVAL_TYPE_IDENTIFIER,
    TIME_TYPE_IDENTIFIER,
    TIMESTAMP_TYPE_IDENTIFIER,
    TIMESTAMPTZ_TYPE_IDENTIFIER,
    DateTimeDataType,
)
from materialize.output_consistency.operation.operation_param import OperationParam
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec


class DateTimeOperationParam(OperationParam):
    def __init__(
        self,
        optional: bool = False,
        support_date: bool = True,
        support_time: bool = True,
        support_timestamp: bool = True,
        support_timestamp_tz: bool = True,
        incompatibilities: Optional[Set[ExpressionCharacteristics]] = None,
    ):
        super().__init__(
            DataTypeCategory.DATE_TIME,
            optional,
            incompatibilities,
            incompatibility_combinations=None,
        )
        self.supported_type_identifiers = []

        if support_date:
            self.supported_type_identifiers.append(DATE_TYPE_IDENTIFIER)
        if support_time:
            self.supported_type_identifiers.append(TIME_TYPE_IDENTIFIER)
        if support_timestamp:
            self.supported_type_identifiers.append(TIMESTAMP_TYPE_IDENTIFIER)
        if support_timestamp_tz:
            self.supported_type_identifiers.append(TIMESTAMPTZ_TYPE_IDENTIFIER)

    def supports_type(
        self, data_type: DataType, previous_args: List[Expression]
    ) -> bool:
        return (
            isinstance(data_type, DateTimeDataType)
            and data_type.identifier in self.supported_type_identifiers
        )

    def might_support_as_input_assuming_category_matches(
        self, return_type_spec: ReturnTypeSpec
    ) -> bool:
        # In doubt return True

        if isinstance(return_type_spec, DateTimeReturnTypeSpec):
            return return_type_spec.type_identifier in self.supported_type_identifiers

        return True


class TimeIntervalOperationParam(OperationParam):
    def __init__(
        self,
        optional: bool = False,
        incompatibilities: Optional[Set[ExpressionCharacteristics]] = None,
    ):
        super().__init__(
            DataTypeCategory.DATE_TIME,
            optional,
            incompatibilities,
            incompatibility_combinations=None,
        )

    def supports_type(
        self, data_type: DataType, previous_args: List[Expression]
    ) -> bool:
        return (
            isinstance(data_type, DateTimeDataType)
            and data_type.identifier == INTERVAL_TYPE_IDENTIFIER
        )
