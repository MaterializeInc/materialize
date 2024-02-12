# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from attr import dataclass


@dataclass
class IgnoreVerdict:
    ignore: bool


class YesIgnore(IgnoreVerdict):
    reason: str

    def __init__(self, reason: str, ignore: bool = True):
        super().__init__(ignore)
        self.reason = reason


@dataclass
class NoIgnore(IgnoreVerdict):
    ignore: bool = False
