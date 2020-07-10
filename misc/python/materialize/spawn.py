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
from typing import Dict, Sequence, Union, Optional, IO, overload
from typing_extensions import Literal
import subprocess

from materialize import ui

CalledProcessError = subprocess.CalledProcessError


def runv(
    args: Sequence[Union[Path, str]],
    cwd: Optional[Path] = None,
    stdin: Union[None, int, IO[bytes]] = None,
    stdout: Union[None, int, IO[bytes]] = None,
    capture_output: bool = False,
    env: Optional[Dict[str, str]] = None,
) -> subprocess.CompletedProcess:
    """Verbosely run a subprocess.

    A description of the subprocess will be written to stdout before the
    subprocess is executed.

    Args:
        args: A list of strings or paths describing the program to run and
            the arguments to pass to it.
        cwd: An optional directory to change into before executing the process.
        stdin: An optional IO handle to use as the process's stdin stream.
        stdout: An optional IO handle to use as the process's stdout stream.
        capture_output: Whether to prevent the process from streaming output
            the parent process's stdin/stdout handles. If true, the output
            will be captured and made available as the `stdout` and `stderr`
            fields on the returned `subprocess.CompletedProcess`. Note that
            setting this parameter to true will override the behavior of the
            `stdout` parameter.
        env: If present, overwrite the environment with this dict

    Raises:
        OSError: The process cannot be executed, e.g. because the specified
            program does not exist.
        CalledProcessError: The process exited with a non-zero exit status.
    """
    print("$", ui.shell_quote(args))
    stderr = None
    if capture_output:
        stdout = subprocess.PIPE
        stderr = subprocess.PIPE
    return subprocess.run(
        args, cwd=cwd, stdin=stdin, stdout=stdout, stderr=stderr, check=True, env=env,
    )


@overload
def capture(
    args: Sequence[Union[Path, str]],
    cwd: Optional[Path] = ...,
    stdin: Union[None, int, IO[bytes]] = None,
    unicode: Literal[False] = ...,
    stderr_too: bool = False,
) -> bytes:
    ...


@overload
def capture(
    args: Sequence[Union[Path, str]],
    cwd: Optional[Path] = ...,
    stdin: Union[None, int, IO[bytes]] = None,
    *,
    unicode: Literal[True],
    stderr_too: bool = False,
) -> str:
    ...


def capture(
    args: Sequence[Union[Path, str]],
    cwd: Optional[Path] = None,
    stdin: Union[None, int, IO[bytes]] = None,
    unicode: bool = False,
    stderr_too: bool = False,
) -> Union[str, bytes]:
    """Capture the output of a subprocess.

    Args:
        args: A list of strings or paths describing the program to run and
            the arguments to pass to it.
        cwd: An optional directory to change into before executing the process.
        stdin: Optional input stream for the process.
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
    return subprocess.check_output(  # type: ignore
        args, cwd=cwd, stdin=stdin, universal_newlines=unicode, stderr=stderr
    )
