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
    TogglePersistTxn = "toggle-persist-txn"

    @classmethod
    def _missing_(cls, value):
        if value == "random":
            return cls(random.choice([elem.value for elem in cls]))
