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
from materialize.output_consistency.input_data.return_specs.record_return_spec import (
    RecordReturnTypeSpec,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec


# Note that this type has no values. It cannot be stored in a table.
class RecordDataType(DataType):
    def __init__(
        self,
        internal_identifier: str,
    ):
        super().__init__(
            internal_identifier,
            "RECORD",
            DataTypeCategory.RECORD,
            is_pg_compatible=True,
        )

    def resolve_return_type_spec(
        self, characteristics: set[ExpressionCharacteristics]
    ) -> ReturnTypeSpec:
        return RecordReturnTypeSpec()


RECORD_TYPE_IDENTIFIER = "RECORD"

RECORD_DATA_TYPE = RecordDataType(
    RECORD_TYPE_IDENTIFIER,
)
