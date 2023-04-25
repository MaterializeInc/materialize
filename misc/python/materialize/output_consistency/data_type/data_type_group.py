# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from enum import Enum


class DataTypeGroup(Enum):
    # only allowed for input parameters
    ANY = (1,)
    # only allowed for expression output, must be resolved from the first arg
    DYNAMIC = (2,)
    NUMERIC = (3,)
    TEXT = (4,)
