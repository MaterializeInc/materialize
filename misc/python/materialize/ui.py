# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Utilities for interacting with humans
"""

import os
import shlex
import sys
import time
from typing import Any, Callable, Generator, Iterable, Optional


class Verbosity:
    """How noisy logs should be"""

    quiet: bool = False

    @classmethod
    def init_from_env(cls, explicit: Optional[bool]) -> None:
        """Set to quiet based on MZ_QUIET being set to almost any value

        The only values that this gets set to false for are the empty string, 0, or no
        """
        env = os.getenv("MZ_QUIET")
        if env is not None:
            cls.quiet = env not in ("", "0", "no")
        if explicit is not None:
            cls.quiet = explicit


def speaker(prefix: str, for_progress: bool = False) -> Callable[..., None]:
    """Create a function that will log with a prefix to stderr

    Obeys `Verbosity.quiet`_

    Example::

        >>> say = speaker("mz")
        >>> say("hello")
        mz> hello
    """

    def say(msg: str, *fmt: str) -> None:
        if not Verbosity.quiet:
            print("{} {}".format(prefix, msg.format(*fmt)), file=sys.stderr)

    return say


def progress(
    msg: str = "", prefix: Optional[str] = None, *, finish: bool = False
) -> None:
    """Print a progress message to stderr, using the same prefix format as speaker
    """
    if prefix is not None:
        msg = f"{prefix}> {msg}"
    end = "" if not finish else "\n"
    print(msg, file=sys.stderr, flush=True, end=end)


def timeout_loop(timeout: int, tick: int = 1) -> Generator[float, None, None]:
    """Loop until timeout, optionally sleeping until tick

    Always iterates at least once

    Args:
        timeout: maximum. number of seconds to wait
        tick: how long to ensure passes between loop iterations. Default: 1
    """
    end = time.monotonic() + timeout
    while True:
        before = time.monotonic()
        yield end - before
        after = time.monotonic()

        if after >= end:
            return

        if after - before < tick:
            if tick > 0:
                time.sleep(tick - (after - before))


def shell_quote(args: Iterable[Any]) -> str:
    """Return shell-escaped string of all the parameters

    ::

        >>> shell_quote(["one", "two three"])
        "one 'two three'"
    """
    return " ".join(shlex.quote(str(arg)) for arg in args)
