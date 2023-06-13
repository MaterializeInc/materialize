# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type_with_values import (
    DataTypeWithValues,
)
from materialize.output_consistency.input_data.types.boolean_type_provider import (
    BOOLEAN_DATA_TYPE,
)

BOOLEAN_DATA_TYPE_WITH_VALUES = DataTypeWithValues(BOOLEAN_DATA_TYPE)

BOOLEAN_DATA_TYPE_WITH_VALUES.add_raw_value("TRUE", "TRUE", set())
BOOLEAN_DATA_TYPE_WITH_VALUES.add_raw_value("FALSE", "FALSE", set())
