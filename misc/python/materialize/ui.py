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

import sys
import time
from typing import Callable, Generator


def speaker(prefix: str, for_progress: bool = False) -> Callable[[str, ...], None]:
    """Create a function that will log with a prefix to stderr

    Example::

        >>> say = speaker("mz")
        >>> say("hello")
        mz> hello
    """

    def say(msg: str, *fmt: str) -> None:
        print("{}> {}".format(msg.format(*fmt)), file=sys.stderr)

    return say


def progress(msg: str = "", prefix: Optional[str] = None, *, finish=False):
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
