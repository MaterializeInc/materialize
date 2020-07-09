# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Optional

class VersionInfo:
    major: int
    minor: int
    patch: int
    prerelease: Optional[str]
    build: Optional[str]
    @staticmethod
    def parse(version: str) -> VersionInfo: ...
    def __eq__(self, other: object) -> bool: ...
    def __lt__(self, other: object) -> bool: ...
    def __gt__(self, other: object) -> bool: ...
    def __le__(self, other: object) -> bool: ...
    def __ge__(self, other: object) -> bool: ...
