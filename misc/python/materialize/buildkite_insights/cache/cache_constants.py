# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum


class FetchMode(Enum):
    AUTO = "auto"
    """Fetch fresh data if existing data does not exist or is older than x hours"""
    ALWAYS = "always"
    """Always fetch fresh data"""
    AVOID = "avoid"
    """Fetch fresh data if data does not exist (regardless whether it is outdated or not)"""
    NEVER = "never"
    """Never fetch fresh data"""

    def __str__(self):
        return str(self.value).lower()


FETCH_MODE_CHOICES = list(FetchMode)
