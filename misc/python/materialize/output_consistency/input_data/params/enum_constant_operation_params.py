# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.output_consistency.enum.enum_operation_param import (
    EnumConstantOperationParam,
)
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.types.all_types_provider import (
    DATA_TYPES,
)

TIME_ZONE_PARAM = EnumConstantOperationParam(
    ["UTC", "CET", "+8", "America/New_York"], add_quotes=True
)

DATE_TIME_COMPONENT_PARAM = EnumConstantOperationParam(
    [
        "microseconds",
        "milliseconds",
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "quarter",
        "year",
        "decade",
        "century",
        "millennium",
    ],
    add_quotes=True,
)

TYPE_FORMAT_PARAM = EnumConstantOperationParam(
    ["Dy, Mon DD YYYY HH24:MI:SS +0000", "Welcome to Mon, YYYY", 'Dth "of" Mon'],
    add_quotes=True,
    add_invalid_value=False,
)

ISO8601_TIMESTAMP_PARAM = EnumConstantOperationParam(
    ["0000-01-01T00:00:00.000Z", "2015-09-18T23:56:04.123Z"],
    add_quotes=True,
)

REGEX_PARAM = EnumConstantOperationParam(
    [".*", "A+", "[ab]"], add_quotes=True, invalid_value="ab("
)

REGEX_FLAG_PARAM = EnumConstantOperationParam(["i"], add_quotes=True, optional=True)

TEXT_TRIM_SPEC_PARAM = EnumConstantOperationParam(
    ["BOTH ", "LEADING", "TRAILING"],
    add_quotes=False,
)

REPETITIONS_PARAM = EnumConstantOperationParam(
    ["0", "1", "2", "10", "100", "-2"], add_quotes=False
)
REPETITIONS_PARAM.characteristics_per_index[REPETITIONS_PARAM.values.index("-2")].add(
    ExpressionCharacteristics.NEGATIVE
)

PRECISION_PARAM = EnumConstantOperationParam(
    ["0", "1", "2", "3", "6", "9"], add_quotes=False
)

TEXT_ENCODING_PARAM = EnumConstantOperationParam(["utf8"], add_quotes=True)

TEXT_FORMAT_PARAM = EnumConstantOperationParam(
    ["base64", "escape", "hex"], add_quotes=True
)

JSON_FIELD_INDEX_PARAM = EnumConstantOperationParam(
    ["0", "1", "2", "8"], add_quotes=False, add_invalid_value=False
)

JSON_FIELD_NAME_PARAM = EnumConstantOperationParam(
    ["a", "b", "c", "z"], add_quotes=True, add_invalid_value=False
)

JSON_PATH_PARAM = EnumConstantOperationParam(
    ["{1}", "{1,1}", "{b,b2}"], add_quotes=True, add_invalid_value=False
)

HASH_ALGORITHM_PARAM = EnumConstantOperationParam(
    ["md5", "sha1", "sha224", "sha256", "sha384", "sha512"], add_quotes=True
)


def all_data_types_enum_constant_operation_param(
    must_be_pg_compatible: bool,
) -> EnumConstantOperationParam:
    all_type_names = [
        data_type.type_name
        for data_type in DATA_TYPES
        if data_type.is_pg_compatible or not must_be_pg_compatible
    ]
    return EnumConstantOperationParam(
        all_type_names, add_quotes=False, add_invalid_value=False
    )
