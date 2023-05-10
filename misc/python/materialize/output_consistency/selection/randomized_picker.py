# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random

from materialize.output_consistency.configuration.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.expressions.expression import Expression


class RandomizedPicker:
    def __init__(self, config: ConsistencyTestConfiguration):
        self.config = config

    def get_seed(self) -> int:
        return self.config.random_seed

    def select(
        self, expressions: list[Expression], num_elements: int = 10000
    ) -> list[Expression]:
        random.seed(self.get_seed())
        return random.choices(expressions, k=num_elements)
