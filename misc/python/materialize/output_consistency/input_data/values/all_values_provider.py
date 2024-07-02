# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import itertools

from materialize.output_consistency.data_type.data_type_with_values import (
    DataTypeWithValues,
)
from materialize.output_consistency.input_data.values.array_values_provider import (
    VALUES_PER_ARRAY_DATA_TYPE,
)
from materialize.output_consistency.input_data.values.boolean_values_provider import (
    BOOLEAN_DATA_TYPE_WITH_VALUES,
)
from materialize.output_consistency.input_data.values.bytea_values_provider import (
    BYTEA_DATA_TYPE_WITH_VALUES,
)
from materialize.output_consistency.input_data.values.date_time_values_provider import (
    DATE_TIME_DATA_TYPES_WITH_VALUES,
)
from materialize.output_consistency.input_data.values.jsonb_values_provider import (
    JSONB_DATA_TYPE_WITH_VALUES,
)
from materialize.output_consistency.input_data.values.list_values_provider import (
    VALUES_PER_LIST_DATA_TYPE,
)
from materialize.output_consistency.input_data.values.map_values_provider import (
    VALUES_PER_MAP_DATA_TYPE,
)
from materialize.output_consistency.input_data.values.number_values_provider import (
    VALUES_PER_NUMERIC_DATA_TYPE,
)
from materialize.output_consistency.input_data.values.range_values_provider import (
    VALUES_PER_RANGE_DATA_TYPE,
)
from materialize.output_consistency.input_data.values.string_values_provider import (
    VALUES_PER_STRING_DATA_TYPE,
)
from materialize.output_consistency.input_data.values.uuid_values_provider import (
    UUID_DATA_TYPE_WITH_VALUES,
)

ALL_DATA_TYPES_WITH_VALUES: list[DataTypeWithValues] = list(
    itertools.chain(
        list(VALUES_PER_NUMERIC_DATA_TYPE.values()),
        [BOOLEAN_DATA_TYPE_WITH_VALUES],
        DATE_TIME_DATA_TYPES_WITH_VALUES,
        [BYTEA_DATA_TYPE_WITH_VALUES],
        list(VALUES_PER_STRING_DATA_TYPE.values()),
        [JSONB_DATA_TYPE_WITH_VALUES],
        list(VALUES_PER_MAP_DATA_TYPE.values()),
        list(VALUES_PER_LIST_DATA_TYPE.values()),
        list(VALUES_PER_ARRAY_DATA_TYPE.values()),
        list(VALUES_PER_RANGE_DATA_TYPE.values()),
        [UUID_DATA_TYPE_WITH_VALUES],
    )
)
