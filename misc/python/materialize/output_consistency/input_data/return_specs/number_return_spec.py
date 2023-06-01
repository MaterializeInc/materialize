# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec


class NumericReturnTypeSpec(ReturnTypeSpec):
    def __init__(
        self,
        only_integer: bool = False,
    ):
        super().__init__(DataTypeCategory.NUMERIC)
        self.only_integer = only_integer
