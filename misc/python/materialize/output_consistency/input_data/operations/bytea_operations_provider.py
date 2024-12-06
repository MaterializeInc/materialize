# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.params.bytea_operation_param import (
    ByteaOperationParam,
)
from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    STRING_ENCODING_PARAM,
    STRING_FORMAT_PARAM,
)
from materialize.output_consistency.input_data.params.number_operation_param import (
    NumericOperationParam,
)
from materialize.output_consistency.input_data.params.string_operation_param import (
    StringOperationParam,
)
from materialize.output_consistency.input_data.return_specs.boolean_return_spec import (
    BooleanReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.bytea_return_spec import (
    ByteaReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.number_return_spec import (
    NumericReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.string_return_spec import (
    StringReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperationOrFunction,
)

BYTEA_OPERATION_TYPES: list[DbOperationOrFunction] = []

BYTEA_OPERATION_TYPES.append(
    DbFunction(
        "convert_from",
        [ByteaOperationParam(), STRING_ENCODING_PARAM],
        StringReturnTypeSpec(),
    )
)

BYTEA_OPERATION_TYPES.append(
    DbFunction(
        "bit_length",
        [ByteaOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)

BYTEA_OPERATION_TYPES.append(
    DbFunction(
        "get_byte",
        [ByteaOperationParam(), NumericOperationParam(only_int_type=True)],
        NumericReturnTypeSpec(only_integer=True),
    )
)

BYTEA_OPERATION_TYPES.append(
    DbFunction(
        "decode",
        [StringOperationParam(), STRING_FORMAT_PARAM],
        ByteaReturnTypeSpec(),
    )
)

encode_function = DbFunction(
    "encode",
    [ByteaOperationParam(), STRING_FORMAT_PARAM],
    StringReturnTypeSpec(),
)
# encode may introduce new lines and backslashes
encode_function.added_characteristics.add(
    ExpressionCharacteristics.STRING_WITH_SPECIAL_SPACE_CHARS
)
encode_function.added_characteristics.add(
    ExpressionCharacteristics.STRING_WITH_BACKSLASH_CHAR
)
BYTEA_OPERATION_TYPES.append(encode_function)

BYTEA_OPERATION_TYPES.append(
    DbFunction(
        "constant_time_eq",
        [ByteaOperationParam(), ByteaOperationParam()],
        BooleanReturnTypeSpec(),
        is_pg_compatible=False,
    )
)

BYTEA_OPERATION_TYPES.append(
    DbFunction(
        "length",
        [ByteaOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)

BYTEA_OPERATION_TYPES.append(
    DbFunction(
        "octet_length",
        [ByteaOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)
