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
from materialize.output_consistency.input_data.types.text_type_provider import (
    TEXT_DATA_TYPE,
)

TEXT_DATA_TYPE_WITH_VALUES = DataTypeWithValues(TEXT_DATA_TYPE)

TEXT_DATA_TYPE_WITH_VALUES.add_raw_value("''", "EMPTY", set())
TEXT_DATA_TYPE_WITH_VALUES.add_raw_value("'a'", "VAL_1", set())
TEXT_DATA_TYPE_WITH_VALUES.add_raw_value("'abc'", "VAL_2", set())
TEXT_DATA_TYPE_WITH_VALUES.add_raw_value("'xAAx'", "VAL_3", set())
TEXT_DATA_TYPE_WITH_VALUES.add_raw_value("' mAA m\n\t '", "VAL_W_SPACES", set())
