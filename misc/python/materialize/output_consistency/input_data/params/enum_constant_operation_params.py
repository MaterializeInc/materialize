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

TAG_DATA_TYPE_ENUM = "data_type"

TIME_ZONE_PARAM = EnumConstantOperationParam(
    ["UTC", "CET", "+8", "America/New_York"], add_quotes=True
)

TIME_COMPONENT_PARAM = EnumConstantOperationParam(
    [
        "microseconds",
        "milliseconds",
        "second",
        "minute",
        "hour",
    ],
    add_quotes=True,
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

LIKE_PARAM = EnumConstantOperationParam(
    ["a%", "a%b", "%c", "a__", "%", "%A%", "a", "a.c", "a?c", ""],
    add_quotes=True,
    add_invalid_value=False,
)

REGEX_PARAM = EnumConstantOperationParam(
    [".*", "A+", "[ab]"], add_quotes=True, invalid_value="ab("
)

REGEX_PARAM_WITH_GROUP = EnumConstantOperationParam(
    ["(.)", "(.*)", "(A+)", "([ab])"], add_quotes=True, invalid_value="ab("
)

REGEX_FLAG_OPTIONAL_PARAM = EnumConstantOperationParam(
    ["i", "g", "n"], add_quotes=True, optional=True
)

STRING_TRIM_SPEC_PARAM = EnumConstantOperationParam(
    ["BOTH ", "LEADING", "TRAILING"],
    add_quotes=False,
)

REPETITIONS_PARAM = EnumConstantOperationParam(
    ["0", "1", "2", "10", "100", "-2"], add_quotes=False
)
REPETITIONS_PARAM.characteristics_per_value["-2"].add(
    ExpressionCharacteristics.NEGATIVE
)

PRECISION_PARAM = EnumConstantOperationParam(
    ["0", "1", "2", "3", "6", "9"], add_quotes=False
)

STRING_ENCODING_PARAM = EnumConstantOperationParam(["utf8"], add_quotes=True)

STRING_FORMAT_PARAM = EnumConstantOperationParam(
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

MAP_FIELD_NAME_PARAM = EnumConstantOperationParam(
    ["", "n", "a", "A", "b"], add_quotes=True, add_invalid_value=True
)

UUID_VALUE_PARAM = EnumConstantOperationParam(
    ["0000bc999c0b4ef8bb6d6bb9bd380a11", "1111Bc99-9c0b-4ef8-bB6d-6bb9bd380A11"],
    add_quotes=True,
    add_invalid_value=True,
)

COLLECTION_INDEX_PARAM = EnumConstantOperationParam(
    ["0", "1", "2", "8"], add_quotes=False, add_invalid_value=False
)

COLLECTION_INDEX_OPTIONAL_PARAM = EnumConstantOperationParam(
    ["0", "1", "2", "8"], add_quotes=False, add_invalid_value=False, optional=True
)

ARRAY_DIMENSION_PARAM = EnumConstantOperationParam(
    ["0", "1", "8"], add_quotes=False, add_invalid_value=False
)

RECORD_FIELD_PARAM = EnumConstantOperationParam(
    # do not use "*" because it will mess up the column mapping between query and result
    ["f1", "f2", "f3", "f4", "key", "value"],
    add_quotes=False,
    add_invalid_value=True,
)
