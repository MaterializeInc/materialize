# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.return_specs.jsonb_return_spec import (
    JsonbReturnTypeSpec,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec

JSONB_TYPE_IDENTIFIER = "JSONB"


class JsonbDataType(DataType):
    def __init__(
        self,
    ):
        super().__init__(
            JSONB_TYPE_IDENTIFIER,
            "JSONB",
            DataTypeCategory.JSONB,
        )

    def resolve_return_type_spec(
        self, characteristics: set[ExpressionCharacteristics]
    ) -> ReturnTypeSpec:
        return JsonbReturnTypeSpec()


JSONB_DATA_TYPE = JsonbDataType()
