#!/usr/bin/env python3
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
    AUTO = 1
    """Fetch fresh data if existing data does not exist or is outdated"""
    ALWAYS = 2
    """Always fetch fresh data"""
    NEVER = 3
    """Never fetch fresh data"""


FETCH_MODE_CHOICES = [entry.name.lower() for entry in list(FetchMode)]
