# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mz_version import MzVersion
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.params.bytea_operation_param import (
    ByteaOperationParam,
)
from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    TEXT_ENCODING_PARAM,
    TEXT_FORMAT_PARAM,
)
from materialize.output_consistency.input_data.params.number_operation_param import (
    NumericOperationParam,
)
from materialize.output_consistency.input_data.params.text_operation_param import (
    TextOperationParam,
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
from materialize.output_consistency.input_data.return_specs.text_return_spec import (
    TextReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperationOrFunction,
)

BYTEA_OPERATION_TYPES: list[DbOperationOrFunction] = []

BYTEA_OPERATION_TYPES.append(
    DbFunction(
        "convert_from",
        [ByteaOperationParam(), TEXT_ENCODING_PARAM],
        TextReturnTypeSpec(),
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
        [TextOperationParam(), TEXT_FORMAT_PARAM],
        ByteaReturnTypeSpec(),
    )
)

encode_function = DbFunction(
    "encode",
    [ByteaOperationParam(), TEXT_FORMAT_PARAM],
    TextReturnTypeSpec(),
)
# encode may introduce new lines and backslashes
encode_function.added_characteristics.add(
    ExpressionCharacteristics.TEXT_WITH_SPECIAL_SPACE_CHARS
)
encode_function.added_characteristics.add(
    ExpressionCharacteristics.TEXT_WITH_BACKSLASH_CHAR
)
BYTEA_OPERATION_TYPES.append(encode_function)

BYTEA_OPERATION_TYPES.append(
    DbFunction(
        "constant_time_eq",
        [ByteaOperationParam(), ByteaOperationParam()],
        BooleanReturnTypeSpec(),
        is_pg_compatible=False,
        since_mz_version=MzVersion.parse_mz("v0.77.0"),
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
