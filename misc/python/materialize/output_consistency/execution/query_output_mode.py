# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from enum import Enum


class QueryOutputMode(Enum):
    SELECT = "select"
    EXPLAIN = "explain"
    EXPLAIN_PHYSICAL = "explain_physical"

    def __str__(self):
        return str(self.value).lower()


QUERY_OUTPUT_MODE_CHOICES = list(QueryOutputMode)


def query_output_mode_to_sql(mode: QueryOutputMode) -> str:
    if mode == QueryOutputMode.SELECT:
        return ""
    elif mode == QueryOutputMode.EXPLAIN:
        return "EXPLAIN"
    elif mode == QueryOutputMode.EXPLAIN_PHYSICAL:
        return "EXPLAIN PHYSICAL PLAN AS TEXT FOR"
    else:
        raise ValueError(f"Unsupported mode: {mode}")
