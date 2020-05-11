# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Exceptions materialize tools may throw, and tools for handling them
"""

import sys
from contextlib import contextmanager
from typing import Callable, Any, Iterable


class MzError(Exception):
    """All errors are MzErrors"""


class MzConfigurationError(MzError):
    """An error that occurred because user-provided code was in error """


class MzRuntimeError(MzError):
    """Execution of a task failed"""


class UnknownItem(MzConfigurationError):
    """A user specified something that we don't recognize"""

    def __init__(self, kind: str, item: Any, acceptable: Iterable[Any]) -> None:
        self.kind = kind
        self.item = item
        self.acceptable = acceptable

    def __str__(self) -> str:
        val = f"Unknown {self.kind}: '{self.item}'"
        if self.acceptable:
            val += ". Expected one of: " + ", ".join([str(a) for a in self.acceptable])
        return val


class BadSpec(MzConfigurationError):
    """User provided a bad specification"""


class Failed(MzRuntimeError):
    """The workflow failed"""


@contextmanager
def error_handler(reporter: Callable[[Any], None]) -> Any:
    """A context manager for your main"""
    try:
        yield
    except MzConfigurationError as e:
        reporter(f"ERROR: {e}")
        sys.exit(1)
    except MzRuntimeError as e:
        reporter(f"Failed! {e}")
        sys.exit(1)
