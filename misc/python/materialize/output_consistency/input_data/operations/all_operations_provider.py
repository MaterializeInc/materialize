# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import itertools

from materialize.output_consistency.input_data.operations.aggregate_operations_provider import (
    AGGREGATE_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.array_operations_provider import (
    ARRAY_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.boolean_operations_provider import (
    BOOLEAN_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.bytea_operations_provider import (
    BYTEA_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.crypto_operations_provider import (
    CRYPTO_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.date_time_operations_provider import (
    DATE_TIME_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.equality_operations_provider import (
    EQUALITY_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.generic_operations_provider import (
    GENERIC_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.jsonb_operations_provider import (
    JSONB_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.list_operations_provider import (
    LIST_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.map_operations_provider import (
    MAP_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.number_operations_provider import (
    NUMERIC_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.pg_operations_provider import (
    PG_OPERATIONS,
)
from materialize.output_consistency.input_data.operations.range_operations_provider import (
    RANGE_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.record_operations_provider import (
    RECORD_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.set_operations_provider import (
    SET_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.string_operations_provider import (
    STRING_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.table_operations_provider import (
    TABLE_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.trigonometric_operations_provider import (
    TRIGONOMETRIC_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.operations.uuid_operations_provider import (
    UUID_OPERATION_TYPES,
)
from materialize.output_consistency.operation.operation import DbOperationOrFunction

ALL_OPERATION_TYPES: list[DbOperationOrFunction] = list(
    itertools.chain(
        GENERIC_OPERATION_TYPES,
        EQUALITY_OPERATION_TYPES,
        SET_OPERATION_TYPES,
        AGGREGATE_OPERATION_TYPES,
        BOOLEAN_OPERATION_TYPES,
        NUMERIC_OPERATION_TYPES,
        TRIGONOMETRIC_OPERATION_TYPES,
        DATE_TIME_OPERATION_TYPES,
        STRING_OPERATION_TYPES,
        BYTEA_OPERATION_TYPES,
        CRYPTO_OPERATION_TYPES,
        JSONB_OPERATION_TYPES,
        MAP_OPERATION_TYPES,
        LIST_OPERATION_TYPES,
        ARRAY_OPERATION_TYPES,
        RANGE_OPERATION_TYPES,
        UUID_OPERATION_TYPES,
        RECORD_OPERATION_TYPES,
        TABLE_OPERATION_TYPES,
        PG_OPERATIONS,
    )
)
