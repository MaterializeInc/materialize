# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.expression.constant_expression import (
    ConstantExpression,
)
from materialize.output_consistency.input_data.types.boolean_type_provider import (
    BOOLEAN_DATA_TYPE,
)

TRUE_EXPRESSION = ConstantExpression("TRUE", BOOLEAN_DATA_TYPE)
FALSE_EXPRESSION = ConstantExpression("FALSE", BOOLEAN_DATA_TYPE)


def create_null_expression(data_type: DataType) -> ConstantExpression:
    return ConstantExpression("NULL", data_type)
