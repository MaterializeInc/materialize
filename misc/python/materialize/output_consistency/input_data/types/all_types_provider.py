# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.input_data.types.boolean_type_provider import (
    BOOLEAN_DATA_TYPE,
)
from materialize.output_consistency.input_data.types.number_types_provider import (
    NUMERIC_DATA_TYPES,
)

DATA_TYPES: List[DataType] = []
DATA_TYPES.extend(NUMERIC_DATA_TYPES)
DATA_TYPES.append(BOOLEAN_DATA_TYPE)
