# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from enum import Enum


class Complexity(Enum):
    Read = "read"
    DML = "dml"
    DDL = "ddl"
    DDLOnly = "ddl-only"

    @classmethod
    def _missing_(cls, value):
        if value == "random":
            return cls(random.choice([elem.value for elem in cls]))


class Scenario(Enum):
    Regression = "regression"
    Cancel = "cancel"
    Kill = "kill"
    Rename = "rename"
    BackupRestore = "backup-restore"
    ZeroDowntimeDeploy = "0dt-deploy"

    @classmethod
    def _missing_(cls, value):
        if value == "random":
            return cls(random.choice([elem.value for elem in cls]))


ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS = {
    # Uses a lot of memory, hard to predict how much
    "memory_limiter_interval": "0",
    # TODO: Remove when https://github.com/MaterializeInc/database-issues/issues/9660 is fixed
    "log_filter": "warn",
    # TODO: Remove when https://github.com/MaterializeInc/database-issues/issues/9656 is fixed
    "persist_stats_filter_enabled": "false",
}
