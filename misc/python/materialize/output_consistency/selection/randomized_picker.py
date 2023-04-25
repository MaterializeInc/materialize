# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random

from materialize.output_consistency.expressions.expression import Expression


class RandomizedPicker:
    def __init__(self, random_seed: int = 0):
        self.random_seed = random_seed

    def select(
        self, expressions: list[Expression], num_elements: int = 10000
    ) -> list[Expression]:
        random.seed(self.random_seed)
        return random.choices(expressions, k=num_elements)
