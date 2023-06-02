# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List

from materialize.output_consistency.input_data.params.date_time_operation_param import (
    DateTimeOperationParam,
)
from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    DATE_TIME_COMPONENT_PARAM,
    TIME_ZONE_PARAM,
    TYPE_FORMAT_PARAM,
)
from materialize.output_consistency.input_data.params.number_operation_param import (
    NumericOperationParam,
)
from materialize.output_consistency.input_data.return_specs.date_time_return_spec import (
    DateTimeReturnTypeSpec,
)
from materialize.output_consistency.input_data.types.date_time_types_provider import (
    INTERVAL_TYPE_IDENTIFIER,
    TIMESTAMP_TYPE_IDENTIFIER,
    TIMESTAMPTZ_TYPE_IDENTIFIER,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    DbOperationOrFunction,
    OperationRelevance,
)

DATE_TIME_OPERATION_TYPES: List[DbOperationOrFunction] = []

DATE_TIME_OPERATION_TYPES.append(
    DbFunction(
        "date_bin",
        [
            DateTimeOperationParam(INTERVAL_TYPE_IDENTIFIER),
            DateTimeOperationParam(TIMESTAMP_TYPE_IDENTIFIER),
            DateTimeOperationParam(TIMESTAMP_TYPE_IDENTIFIER),
        ],
        DateTimeReturnTypeSpec(TIMESTAMP_TYPE_IDENTIFIER),
        relevance=OperationRelevance.LOW,
    )
)

DATE_TIME_OPERATION_TYPES.append(
    DbFunction(
        "date_trunc",
        [DATE_TIME_COMPONENT_PARAM, DateTimeOperationParam(TIMESTAMP_TYPE_IDENTIFIER)],
        DateTimeReturnTypeSpec(TIMESTAMP_TYPE_IDENTIFIER),
    )
)

# TODO split off interval, support all non-interval types, return that type
DATE_TIME_OPERATION_TYPES.append(
    DbFunction(
        "date_trunc",
        [DATE_TIME_COMPONENT_PARAM, DateTimeOperationParam(INTERVAL_TYPE_IDENTIFIER)],
        DateTimeReturnTypeSpec(INTERVAL_TYPE_IDENTIFIER),
    )
)

# TIMESTAMP_TYPE_IDENTIFIER as input, TIMESTAMPTZ_TYPE_IDENTIFIER as output
DATE_TIME_OPERATION_TYPES.append(
    DbOperation(
        "EXTRACT($ FROM $)",
        [DATE_TIME_COMPONENT_PARAM, DateTimeOperationParam(TIMESTAMP_TYPE_IDENTIFIER)],
        DateTimeReturnTypeSpec(TIMESTAMP_TYPE_IDENTIFIER),
    )
)

DATE_TIME_OPERATION_TYPES.append(
    DbFunction(
        "date_part",
        [DATE_TIME_COMPONENT_PARAM, DateTimeOperationParam(TIMESTAMP_TYPE_IDENTIFIER)],
        DateTimeReturnTypeSpec(TIMESTAMP_TYPE_IDENTIFIER),
    )
)

# TIMESTAMP_TYPE_IDENTIFIER as input, TIMESTAMPTZ_TYPE_IDENTIFIER as output
DATE_TIME_OPERATION_TYPES.append(
    DbOperation(
        "$ AT TIME ZONE $::TEXT",
        [DateTimeOperationParam(TIMESTAMP_TYPE_IDENTIFIER), TIME_ZONE_PARAM],
        DateTimeReturnTypeSpec(TIMESTAMPTZ_TYPE_IDENTIFIER),
    )
)

# TIMESTAMPTZ_TYPE_IDENTIFIER as input, TIMESTAMP_TYPE_IDENTIFIER as output
DATE_TIME_OPERATION_TYPES.append(
    DbOperation(
        "$ AT TIME ZONE $::TEXT",
        [DateTimeOperationParam(TIMESTAMPTZ_TYPE_IDENTIFIER), TIME_ZONE_PARAM],
        DateTimeReturnTypeSpec(TIMESTAMP_TYPE_IDENTIFIER),
    )
)

# TIMESTAMP_TYPE_IDENTIFIER as input, TIMESTAMPTZ_TYPE_IDENTIFIER as output
DATE_TIME_OPERATION_TYPES.append(
    DbFunction(
        "timezone",
        [TIME_ZONE_PARAM, DateTimeOperationParam(TIMESTAMP_TYPE_IDENTIFIER)],
        DateTimeReturnTypeSpec(TIMESTAMPTZ_TYPE_IDENTIFIER),
    )
)

# TIMESTAMPTZ_TYPE_IDENTIFIER as input, TIMESTAMP_TYPE_IDENTIFIER as output
DATE_TIME_OPERATION_TYPES.append(
    DbFunction(
        "timezone",
        [TIME_ZONE_PARAM, DateTimeOperationParam(TIMESTAMPTZ_TYPE_IDENTIFIER)],
        DateTimeReturnTypeSpec(TIMESTAMP_TYPE_IDENTIFIER),
    )
)

DATE_TIME_OPERATION_TYPES.append(
    DbFunction(
        "to_timestamp",
        [NumericOperationParam()],
        DateTimeReturnTypeSpec(TIMESTAMPTZ_TYPE_IDENTIFIER),
    )
)


DATE_TIME_OPERATION_TYPES.append(
    DbFunction(
        "to_char",
        [DateTimeOperationParam(TIMESTAMP_TYPE_IDENTIFIER), TYPE_FORMAT_PARAM],
        # TODO: wrong, requires text type
        DateTimeReturnTypeSpec(TIMESTAMP_TYPE_IDENTIFIER),
    )
)

DATE_TIME_OPERATION_TYPES.append(
    DbFunction(
        "justify_days",
        [DateTimeOperationParam(INTERVAL_TYPE_IDENTIFIER)],
        DateTimeReturnTypeSpec(INTERVAL_TYPE_IDENTIFIER),
    )
)

DATE_TIME_OPERATION_TYPES.append(
    DbFunction(
        "justify_hours",
        [DateTimeOperationParam(INTERVAL_TYPE_IDENTIFIER)],
        DateTimeReturnTypeSpec(INTERVAL_TYPE_IDENTIFIER),
    )
)

DATE_TIME_OPERATION_TYPES.append(
    DbFunction(
        "justify_interval",
        [DateTimeOperationParam(INTERVAL_TYPE_IDENTIFIER)],
        DateTimeReturnTypeSpec(INTERVAL_TYPE_IDENTIFIER),
    )
)
