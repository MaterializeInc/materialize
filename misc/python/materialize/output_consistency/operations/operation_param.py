# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Optional

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.data_type.value_characteristics import (
    ValueCharacteristics,
)


class OperationParam:
    def __init__(
        self,
        type_category: DataTypeCategory,
        incompatibilities: Optional[set[ValueCharacteristics]] = None,
    ):
        if incompatibilities is None:
            incompatibilities = set()

        self.type_category = type_category
        self.incompatibilities: set[ValueCharacteristics] = incompatibilities


class NumericOperationParam(OperationParam):
    def __init__(
        self,
        incompatibilities: Optional[set[ValueCharacteristics]] = None,
    ):
        super().__init__(DataTypeCategory.NUMERIC, incompatibilities)
