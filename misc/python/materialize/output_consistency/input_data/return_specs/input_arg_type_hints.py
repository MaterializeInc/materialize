# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.output_consistency.data_type.data_type_category import DataTypeCategory


class InputArgTypeHints:
    def __init__(self):
        self.type_category_of_requested_args: dict[int, DataTypeCategory] = dict()

    def is_empty(self) -> bool:
        return len(self.type_category_of_requested_args) == 0
