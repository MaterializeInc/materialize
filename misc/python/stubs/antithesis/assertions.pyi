# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from collections.abc import Mapping
from typing import Any

def always(
    condition: bool, message: str, details: Mapping[str, Any] | None = None
) -> None: ...
def always_or_unreachable(
    condition: bool, message: str, details: Mapping[str, Any] | None = None
) -> None: ...
def sometimes(
    condition: bool, message: str, details: Mapping[str, Any] | None = None
) -> None: ...
def reachable(message: str, details: Mapping[str, Any] | None = None) -> None: ...
def unreachable(message: str, details: Mapping[str, Any] | None = None) -> None: ...
def always_greater_than_or_equal_to(
    left: Any, right: Any, message: str, details: Mapping[str, Any] | None = None
) -> None: ...
def always_less_than_or_equal_to(
    left: Any, right: Any, message: str, details: Mapping[str, Any] | None = None
) -> None: ...
