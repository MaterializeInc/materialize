# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Utilities for spawning processes.

The functions in this module are a convenient high-level interface to the
operations provided by the standard [`subprocess`][subprocess] module.

[subprocess]: https://docs.python.org/3/library/subprocess.html
"""

from pathlib import Path
from typing import Sequence, Union, Optional, IO, overload
from typing_extensions import Literal
import subprocess

CalledProcessError = subprocess.CalledProcessError


def runv(
    args: Sequence[Union[Path, str]],
    cwd: Optional[Path] = None,
    stdin: Optional[IO[bytes]] = None,
    stdout: Optional[IO[bytes]] = None,
) -> None:
    """Verbosely run a subprocess.

    A description of the subprocess will be written to stdout before the
    subprocess is executed.

    Args:
        args: A list of strings or paths describing the program to run and
            the arguments to pass to it.
        cwd: An optional directory to change into before executing the process.
        stdin: An optional IO handle to use as the process's stdin stream.
        stdout: An optional IO handle to use as the process's stdout stream.

    Raises:
        OSError: The process cannot be executed, e.g. because the specified
            program does not exist.
        CalledProcessError: The process exited with a non-zero exit status.
    """
    print("$", " ".join(str(arg) for arg in args))
    subprocess.check_call(args, cwd=cwd, stdin=stdin, stdout=stdout)


@overload
def capture(
    args: Sequence[Union[Path, str]],
    cwd: Optional[Path] = ...,
    unicode: Literal[False] = ...,
) -> bytes:
    ...


@overload
def capture(
    args: Sequence[Union[Path, str]],
    cwd: Optional[Path] = ...,
    *,
    unicode: Literal[True],
) -> str:
    ...


def capture(
    args: Sequence[Union[Path, str]],
    cwd: Optional[Path] = None,
    unicode: bool = False,
    stderr_too: bool = False,
) -> Union[str, bytes]:
    """Capture the output of a subprocess.

    Args:
        args: A list of strings or paths describing the program to run and
            the arguments to pass to it.
        cwd: An optional directory to change into before executing the process.
        unicode: Whether to return output as a unicode string or as bytes.
        stderr_too: Whether to capture stderr in the returned value

    Returns:
        output: The verbatim output of the process as a string or bytes object,
            depending on the value of the `unicode` argument. Note that trailing
            whitespace is preserved.

    Raises:
        OSError: The process cannot be executed, e.g. because the specified
            program does not exist.
        CalledProcessError: The process exited with a non-zero exit status.

    .. tip:: Many programs produce output with a trailing newline.
        You may want to call `strip()` on the output to remove any trailing
        whitespace.
    """
    stderr = subprocess.STDOUT if stderr_too else None
    return subprocess.check_output(
        args, cwd=cwd, universal_newlines=unicode, stderr=stderr
    )
