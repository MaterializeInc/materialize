# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum


class EvaluationScenario(Enum):
    OUTPUT_CONSISTENCY = 1
    """Data-flow rendering vs. constant folding"""
    POSTGRES_CONSISTENCY = 2
    """Materialize vs. Postgres"""
    VERSION_CONSISTENCY = 3
    """Two different versions of mz"""
    FEATURE_FLAG_CONSISTENCY = 4
    """Different feature flag configuration in mz"""

    def __str__(self) -> str:
        return self.name
