# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from collections.abc import Callable
from typing import Any

class PrettyTable:
    field_names: list[str]
    def __init__(self, field_names: list[str] = ..., align: str = ...): ...
    def add_row(self, rows: list[Any]) -> None: ...
    def add_rows(self, rows: list[list[Any]]) -> None: ...
    def get_string(self, sortkey: Callable) -> str: ...
