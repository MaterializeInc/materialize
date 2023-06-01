# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Optional, Set

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.types.date_time_types_provider import (
    DateTimeDataType,
)
from materialize.output_consistency.operation.operation_param import OperationParam


class DateTimeOperationParam(OperationParam):
    def __init__(
        self,
        type_identifier: str,
        optional: bool = False,
        incompatibilities: Optional[Set[ExpressionCharacteristics]] = None,
    ):
        super().__init__(
            DataTypeCategory.DATE_TIME,
            optional,
            incompatibilities,
            incompatibility_combinations=None,
        )
        self.type_identifier = type_identifier

    def supports_type(self, data_type: DataType) -> bool:
        return (
            isinstance(data_type, DateTimeDataType)
            and data_type.identifier == self.type_identifier
        )
