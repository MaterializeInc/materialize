# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The subset of hegeltest 0.5.0 that the Antithesis driver uses. The package
# is optional (misc/python/materialize/antithesis/requirements.txt), so type
# checking cannot rely on its own annotations.

import enum
from collections.abc import Callable
from typing import Any, TypeVar

from hegel import generators as generators
from hegel.generators import Generator

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])

class HegelError(Exception): ...
class StopTest(BaseException): ...
class AssumeFailed(BaseException): ...

class Phase(enum.Enum):
    explicit = ...
    reuse = ...
    generate = ...
    target = ...
    shrink = ...
    explain = ...

class TestCase:
    def draw(self, generator: Generator[T], name: str | None = None) -> T: ...
    def draw_silent(self, generator: Generator[T]) -> T: ...
    def assume(self, condition: object) -> None: ...
    def note(self, message: str) -> None: ...

class StateMachine:
    steps: int
    min_concurrency: int
    max_concurrency: int

def rule(
    fn: Callable[..., Any] | None = None,
    *,
    groups: Any = None,
    weight: float = 1.0,
    name: str | None = None,
) -> Callable[[F], F]: ...
def invariant(
    fn: Callable[..., Any] | None = None,
    *,
    always_run: bool = False,
    pause_faults: bool = False,
    name: str | None = None,
) -> Callable[[F], F]: ...
def main(
    fn: Callable[[TestCase], Any] | None = None,
    /,
    *,
    settings: Any = None,
    **overrides: Any,
) -> Callable[[Callable[[TestCase], Any]], Callable[[list[str] | None], None]]: ...
def run_state_machine(tc: TestCase, machine: Any, **overrides: Any) -> None: ...
