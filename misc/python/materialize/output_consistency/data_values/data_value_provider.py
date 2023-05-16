# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.output_consistency.data_values.data_type_with_values import (
    DataTypeWithValues,
)
from materialize.output_consistency.data_values.number_data_value_provider import (
    VALUES_PER_NUMERIC_DATA_TYPE,
)

DATA_TYPES_WITH_VALUES: list[DataTypeWithValues] = []
DATA_TYPES_WITH_VALUES.extend(list(VALUES_PER_NUMERIC_DATA_TYPE.values()))
