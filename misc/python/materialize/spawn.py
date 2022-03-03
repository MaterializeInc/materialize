# Copyright Materialize, Inc. and contributors. All rights reserved.
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

import subprocess
import sys
from pathlib import Path
from typing import IO, Dict, Optional, Sequence, Union

from materialize import ui

CalledProcessError = subprocess.CalledProcessError

# NOTE(benesch): Please think twice before adding additional parameters to this
# method! It is meant to serve 95% of callers with a small ands understandable
# set of parameters. If your needs are niche, consider calling `subprocess.run`
# directly rather than adding a one-off parameter here.
def runv(
    args: Sequence[Union[Path, str]],
    *,
    cwd: Optional[Path] = None,
    env: Optional[Dict[str, str]] = None,
    stdin: Union[None, int, IO[bytes], bytes] = None,
    stdout: Union[None, int, IO[bytes]] = None,
    stderr: Union[None, int, IO[bytes]] = None,
) -> subprocess.CompletedProcess:
    """Verbosely run a subprocess.

    A description of the subprocess will be written to stdout before the
    subprocess is executed.

    Args:
        args: A list of strings or paths describing the program to run and
            the arguments to pass to it.
        cwd: An optional directory to change into before executing the process.
        env: A replacement environment with which to launch the process. If
            unspecified, the current process's environment is used. Replacement
            occurs wholesale, so use a construction like
            `env=dict(os.environ, KEY=VAL, ...)` to instead amend the existing
            environment.
        stdin: An optional IO handle or byte string to use as the process's
            stdin stream.
        stdout: An optional IO handle to use as the process's stdout stream.
        stderr: An optional IO handle to use as the process's stderr stream.

    Raises:
        OSError: The process cannot be executed, e.g. because the specified
            program does not exist.
        CalledProcessError: The process exited with a non-zero exit status.
    """
    print("$", ui.shell_quote(args), file=sys.stderr)

    input = None
    if isinstance(stdin, bytes):
        input = stdin
        stdin = None

    return subprocess.run(
        args,
        cwd=cwd,
        env=env,
        input=input,
        stdin=stdin,
        stdout=stdout,
        stderr=stderr,
        check=True,
    )


def capture(
    args: Sequence[Union[Path, str]],
    *,
    cwd: Optional[Path] = None,
    env: Optional[Dict[str, str]] = None,
    stdin: Union[None, int, IO[bytes], str] = None,
    stderr: Union[None, int, IO[bytes]] = None,
) -> str:
    """Capture the output of a subprocess.

    Args:
        args: A list of strings or paths describing the program to run and
            the arguments to pass to it.
        cwd: An optional directory to change into before executing the process.
        env: A replacement environment with which to launch the process. If
            unspecified, the current process's environment is used. Replacement
            occurs wholesale, so use a construction like
            `env=dict(os.environ, KEY=VAL, ...)` to instead amend the existing
            environment.
        stdin: An optional IO handle, byte string or string to use as the process's
            stdin stream.
        stderr: An optional IO handle to use as the process's stderr stream.

    Returns:
        output: The verbatim output of the process as a string. Note that
            trailing whitespace is preserved.

    Raises:
        OSError: The process cannot be executed, e.g. because the specified
            program does not exist.
        CalledProcessError: The process exited with a non-zero exit status.

    .. tip:: Many programs produce output with a trailing newline.
        You may want to call `strip()` on the output to remove any trailing
        whitespace.
    """
    input = None
    if isinstance(stdin, str):
        input = stdin
        stdin = None

    return subprocess.check_output(
        args, cwd=cwd, env=env, input=input, stdin=stdin, stderr=stderr, text=True
    )
