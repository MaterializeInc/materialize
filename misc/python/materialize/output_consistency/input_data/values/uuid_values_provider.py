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
from materialize.output_consistency.input_data.types.uuid_type_provider import (
    UUID_DATA_TYPE,
)

UUID_DATA_TYPE_WITH_VALUES = DataTypeWithValues(UUID_DATA_TYPE)

UUID_DATA_TYPE_WITH_VALUES.add_raw_value(
    "'aaaabc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "VAL_1", set()
)
UUID_DATA_TYPE_WITH_VALUES.add_raw_value(
    "'bbbbbc999c0b4ef8bb6d6bb9bd380a11'", "VAL_2", set()
)
UUID_DATA_TYPE_WITH_VALUES.add_raw_value(
    "'ccccBc99-9c0b-4ef8-bB6d-6bb9bd380A11'", "VAL_3", set()
)
