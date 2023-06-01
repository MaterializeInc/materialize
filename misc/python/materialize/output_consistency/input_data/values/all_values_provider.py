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

from materialize.output_consistency.data_type.data_type_with_values import (
    DataTypeWithValues,
)
from materialize.output_consistency.input_data.values.boolean_values_provider import (
    BOOLEAN_DATA_TYPE_WITH_VALUES,
)
from materialize.output_consistency.input_data.values.date_time_values_provider import (
    DATE_TIME_DATA_TYPES_WITH_VALUES,
)
from materialize.output_consistency.input_data.values.number_values_provider import (
    VALUES_PER_NUMERIC_DATA_TYPE,
)

ALL_DATA_TYPES_WITH_VALUES: List[DataTypeWithValues] = list(
    itertools.chain(
        list(VALUES_PER_NUMERIC_DATA_TYPE.values()),
        [BOOLEAN_DATA_TYPE_WITH_VALUES],
        DATE_TIME_DATA_TYPES_WITH_VALUES,
    )
)
