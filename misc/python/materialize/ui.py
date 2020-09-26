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

import datetime
import os
import shlex
import sys
import time
from typing import Any, Callable, Generator, Iterable, Optional, NamedTuple, Union

from materialize import docker


class Verbosity:
    """How noisy logs should be"""

    quiet: bool = False

    @classmethod
    def init_from_env(cls, explicit: Optional[bool]) -> None:
        """Set to quiet based on MZ_QUIET being set to almost any value

        The only values that this gets set to false for are the empty string, 0, or no
        """
        cls.quiet = env_is_truthy("MZ_QUIET")
        if explicit is not None:
            cls.quiet = explicit


def speaker(prefix: str, for_progress: bool = False) -> Callable[..., None]:
    """Create a function that will log with a prefix to stderr

    Obeys `Verbosity.quiet`_

    Example::

        >>> say = speaker("mz")
        >>> say("hello")  # doctest: +SKIP
        mz> hello
    """

    def say(msg: str) -> None:
        if not Verbosity.quiet:
            print("{} {}".format(prefix, msg), file=sys.stderr)

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


def log_in_automation(msg: str) -> None:
    """Log to a file, if we're running in automation"""
    if env_is_truthy("MZ_IN_AUTOMATION"):
        with open("/tmp/mzconduct.log", "a") as fh:
            now = datetime.datetime.now().isoformat()
            print(f"[{now}] {msg}", file=fh)


def shell_quote(args: Iterable[Any]) -> str:
    """Return shell-escaped string of all the parameters

    ::

        >>> shell_quote(["one", "two three"])
        "one 'two three'"
    """
    return " ".join(shlex.quote(str(arg)) for arg in args)


def env_is_truthy(env_var: str) -> bool:
    """Return true if `env_var` is set and is not one of: 0, '', no"""
    env = os.getenv(env_var)
    if env is not None:
        return env not in ("", "0", "no")
    return False


def warn_docker_resource_limits() -> None:
    """Check docker for recommended resource limits
    """
    warn = speaker("WARN:")

    limits = docker.resource_limits()
    if limits.mem_total < docker.RECOMMENDED_MIN_MEM:
        actual = SiNumber.of(limits.mem_total)
        desired = SiNumber.of(docker.RECOMMENDED_MIN_MEM)
        warn(
            f"Docker only has {actual} memory, "
            f"less than {desired} can cause demos to fail in unexpected ways.\n"
            "    See https://materialize.io/docs/third-party/docker/\n"
        )
    if limits.ncpus < docker.RECOMMENDED_MIN_CPUS:
        warn(
            f"Docker only has access to {limits.ncpus}, "
            f"fewer than {docker.RECOMMENDED_MIN_CPUS} can cause demos to fail in unexpected ways.\n"
            "    See https://materialize.io/docs/third-party/docker/\n"
        )


class SiNumber(NamedTuple):
    """A human-readable number, constructed by the .of method

    ::

        >>> str(SiNumber.of(16795869184, "B"))
        '15.6 GiB'
    """

    val: float
    suffix: str
    kind: str

    def __str__(self) -> str:
        return f"{self.val:.1f} {self.suffix}{self.kind}"

    @staticmethod
    def of(val: Union[int, float], suffix: str = "B") -> "SiNumber":
        """Construct a human readable number by continuously dividing until it fits in an SI prefix

        Args:
            val: the number to convert
            suffix: What kind of number it is (e.g. "B" for bytes)
        """

        suffixes = ["", "Ki", "Mi", "Gi", "Ti"]
        index = 0
        while val > 1024:
            val /= 1024
            index += 1
        return SiNumber(val, suffixes[index], suffix)
