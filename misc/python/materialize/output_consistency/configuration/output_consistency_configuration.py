# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


class OutputConsistencyConfiguration:
    def __init__(self, queries_per_tx: int, max_cols_per_query: int, random_seed: int):
        self.queries_per_tx = queries_per_tx
        self.max_cols_per_query = max_cols_per_query
        self.random_seed = random_seed


DEFAULT_CONFIG = OutputConsistencyConfiguration(
    queries_per_tx=20, random_seed=0, max_cols_per_query=8
)
