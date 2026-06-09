# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from enum import Enum

import psycopg
import requests


class Complexity(Enum):
    Read = "read"
    DML = "dml"
    DDL = "ddl"
    DDLOnly = "ddl-only"

    @classmethod
    def _missing_(cls, value):
        if value == "random":
            return cls(random.choice([elem.value for elem in cls]))


class Scenario(Enum):
    Regression = "regression"
    Cancel = "cancel"
    Kill = "kill"
    Rename = "rename"
    BackupRestore = "backup-restore"
    ZeroDowntimeDeploy = "0dt-deploy"
    RepeatRow = "repeat-row"

    @classmethod
    def _missing_(cls, value):
        if value == "random":
            return cls(random.choice([elem.value for elem in cls]))


# Message substrings produced by connection drops, DNS partitions, broker
# transport failures, and Mz-restart admission control.  Used by
# `is_fault_shaped()` to decide whether a Scenario.Kill / ZeroDowntimeDeploy
# swallow site should tolerate `exc` or re-raise it.  Anything not in this
# list is treated as a real correctness signal worth surfacing.
_FAULT_SHAPED_MSG_PATTERNS: tuple[str, ...] = (
    # psycopg connection-drop wordings
    "server closed the connection unexpectedly",
    "the connection is lost",
    "EOF detected",
    "Cursor closed",
    # libc / socket
    "connection refused",
    "connection reset",
    "broken pipe",
    "could not connect to server",
    "Failed to resolve hostname",
    "failed to lookup address information",
    "Temporary failure in name resolution",
    "Multiple connection attempts failed",
    "connection timeout",
    # Materialize / persist admission control: S3-backed persist surfaces
    # HTTP 429s via reqwest error chains from src/persist/src/location.rs.
    "TooManyRequests",
    # Postgres source visiting its own restart window
    "terminating connection due to administrator command",
    # Kafka transport during broker fault windows
    "BrokerTransportFailure",
    "Meta data fetch error",
    # HTTP / WS
    "Remote end closed connection without response",
    "Connection aborted",
    "Connection broken: IncompleteRead",
    "Connection to remote host was lost",
    "socket is already closed",
    "WS connect",
)


def is_fault_shaped(exc: BaseException) -> bool:
    """True if `exc` looks like a fault-injection / kill-test artifact
    rather than a SUT correctness bug.

    Used by `Scenario.Kill` / `Scenario.ZeroDowntimeDeploy` tolerance
    sites in `action.py` and `executor.py`: matching exceptions get
    swallowed (the kill-thread can plausibly produce them); everything
    else re-raises so real bugs surface.

    The bare `except:` shape that this function replaces previously
    swallowed *every* exception under those scenarios, including
    AssertionError / KeyError / TypeError from framework bugs and
    actual SUT misbehavior.  Narrow it to the shapes a kill / fault
    actually produces.
    """
    # Operational connection-error types — always tolerated regardless
    # of the surface message (drivers don't promise a stable wording).
    if isinstance(
        exc,
        psycopg.OperationalError
        | psycopg.InterfaceError
        | requests.exceptions.ConnectionError,
    ):
        return True
    msg = getattr(exc, "msg", None) or str(exc)
    return any(p in msg for p in _FAULT_SHAPED_MSG_PATTERNS)


ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS = {
    # Uses a lot of memory, hard to predict how much
    "memory_limiter_interval": "0",
    # See https://materializeinc.slack.com/archives/CTESPM7FU/p1758195280629909, should reenable when it performs better
    "enable_compute_logical_backpressure": "false",
    # Allows the `Scenario.RepeatRow` scenario to call `repeat_row`. Having
    # it on outside that scenario is harmless: no Parallel Workload codegen
    # emits `repeat_row` unless the scenario is active.
    "enable_repeat_row": "true",
}
