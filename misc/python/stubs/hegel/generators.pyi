# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from collections.abc import Sequence
from typing import Generic, TypeVar

T = TypeVar("T")

class Generator(Generic[T]): ...

def integers(
    min_value: int | None = None, max_value: int | None = None
) -> Generator[int]: ...
def booleans() -> Generator[bool]: ...
def weighted_booleans(p: float) -> Generator[bool]: ...
def sampled_from(elements: Sequence[T]) -> Generator[T]: ...
