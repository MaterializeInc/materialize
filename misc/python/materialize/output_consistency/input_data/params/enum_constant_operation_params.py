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

TIME_ZONE_PARAM = EnumConstantOperationParam(
    {"UTC", "CET", "+8", "America/New_York"}, add_quotes=True
)

DATE_TIME_COMPONENT_PARAM = EnumConstantOperationParam(
    {
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
    },
    add_quotes=True,
)

TYPE_FORMAT_PARAM = EnumConstantOperationParam(
    {"Dy, Mon DD YYYY HH24:MI:SS +0000", "Welcome to Mon, YYYY", "Dth of Mon"},
    add_quotes=True,
)

REGEX_PARAM = EnumConstantOperationParam({".*", "A+", "[ab]"}, add_quotes=True)

REGEX_FLAG_PARAM = EnumConstantOperationParam({"i"}, add_quotes=True, optional=True)

TEXT_TRIM_SPEC_PARAM = EnumConstantOperationParam(
    {"BOTH ", "LEADING", "TRAILING"},
    add_quotes=False,
)

REPETITIONS_PARAM = EnumConstantOperationParam(
    {"0", "1", "2", "10", "100", "-2"}, add_quotes=False
)
