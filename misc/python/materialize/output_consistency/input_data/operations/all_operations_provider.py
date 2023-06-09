# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import itertools
from typing import List

from materialize.output_consistency.input_data.operations.aggregate_operations_provider import (
    AGGREGATE_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.boolean_operations_provider import (
    BOOLEAN_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.date_time_operations_provider import (
    DATE_TIME_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.generic_operations_provider import (
    GENERIC_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.number_operations_provider import (
    NUMERIC_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.trigonometric_operations_provider import (
    TRIGONOMETRIC_OPERATION_TYPES,
)
from materialize.output_consistency.operation.operation import DbOperationOrFunction

ALL_OPERATION_TYPES: List[DbOperationOrFunction] = list(
    itertools.chain(
        GENERIC_OPERATION_TYPES,
        AGGREGATE_OPERATION_TYPES,
        BOOLEAN_OPERATION_TYPES,
        NUMERIC_OPERATION_TYPES,
        TRIGONOMETRIC_OPERATION_TYPES,
        DATE_TIME_OPERATION_TYPES,
    )
)
