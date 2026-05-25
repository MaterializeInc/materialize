# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Shared loader + classification helpers for the workload-replay driver.

A captured Materialize workload YAML (the format that
`test/workload-replay/mzcompose.py` consumes) is baked into the workload
image at build time.  The Antithesis driver replays a random sample of
its `queries` list against a live `materialized` under fault injection.

The bundled workload is `captured/console_cluster_tabs.yml`: a real one-
hour slice of Materialize web-console traffic (introspection + metadata
queries), deduplicated by SQL.  Every query targets the built-in
`mz_catalog_server` cluster and the built-in `materialize` database, so
no user clusters / sources / sinks need to exist for replay to work —
the workload-replay group's topology is therefore minimal (universal
services only).

This module owns the YAML load (cached at first call so concurrent
driver invocations share the parse) plus the substring lists for SQL
errors we consider "expected under Antithesis" vs "real product
violation".  Pgwire connect/retry helpers come from the shared
`helper_pg` module.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any

import yaml

# The bake script copies this driver template into
# /opt/antithesis/test/v1/workload-replay/, so the captured workload
# lands as a sibling of the running script under
# /opt/antithesis/test/v1/workload-replay/captured/.  The path is
# derived from __file__ rather than hard-coded so the same module
# works inside the Antithesis runtime directory and inside the staging
# directory used at image-build time.
CAPTURED_DIR = Path(__file__).resolve().parent / "captured"
DEFAULT_WORKLOAD = "console_cluster_tabs.yml"

_LOAD_LOCK = threading.Lock()
_CACHE: dict[str, dict[str, Any]] = {}


def load_workload(name: str = DEFAULT_WORKLOAD) -> dict[str, Any]:
    """Parse the bundled workload YAML.  Cached across calls in one process.

    Concurrent invocations within a single workload container would
    otherwise each re-parse a multi-megabyte file; the lock serializes
    the first miss.
    """
    with _LOAD_LOCK:
        cached = _CACHE.get(name)
        if cached is not None:
            return cached
        path = CAPTURED_DIR / name
        with path.open() as f:
            data = yaml.safe_load(f)
        _CACHE[name] = data
        return data


# Statement types from the workload-replay YAML schema that we never
# replay standalone: they only make sense inside a multi-statement
# transaction or refer to objects we don't recreate.  Mirrors the skip
# list in `materialize.workload_replay.replay.continuous_queries`.
SKIP_STATEMENT_TYPES = frozenset(
    {
        "start_transaction",
        "set_transaction",
        "commit",
        "rollback",
        "fetch",
        "create_connection",
        "create_webhook",
        "create_source",
        "create_subsource",
        "create_sink",
        "create_table_from_source",
    }
)


def replayable_queries(workload: dict[str, Any]) -> list[dict[str, Any]]:
    """Filter the workload's queries to ones that can be replayed in isolation."""
    return [
        q
        for q in workload.get("queries", [])
        if q.get("statement_type") not in SKIP_STATEMENT_TYPES
    ]


# Per-query error substrings produced by the SUT on legitimate replay
# attempts.  These don't indicate fault injection (`looks_like_fault`
# in `helper_fault_tolerance` covers that) and they don't indicate a
# property violation either — they reflect the captured workload not
# matching the current SUT's catalog exactly (e.g. the workload was
# captured against a build with a column the current build renamed,
# or a randomized parameter cast can't satisfy the column type).
#
# Demote these to `sometimes()` rather than `always(False)`: they're
# noise relative to what Antithesis is actually trying to catch
# (panics, internal errors, query hangs).
EXPECTED_REPLAY_ERROR_PATTERNS: tuple[str, ...] = (
    # The workload captured queries against a catalog snapshot; any
    # introspection query that references a column the current build
    # has renamed / removed surfaces as one of these.  Not a bug —
    # the workload bundle is older than HEAD.
    "unknown catalog item",
    'column "',
    "no such function",
    "function ",
    # `param` values come from the capture and may not cast cleanly to
    # the (possibly evolving) column type.  Same root cause as the
    # workload-replay framework's own
    # `if "invalid input syntax for type" in error: continue` filter.
    "invalid input syntax for type",
    "cannot cast",
    "type mismatch",
    # Captured `SUBSCRIBE` queries with a captured isolation level may
    # be cancelled by the per-query timeout we apply (see
    # `SUBSCRIBE_BUDGET_S` in the driver).  psycopg surfaces this as
    # `QueryCanceled`; we treat it as a replay-side timeout, not a SUT
    # bug.
    "canceling statement due to statement timeout",
    "canceling statement due to user request",
    # The catalog `quickstart` cluster from the capture may not exist
    # in the workload-replay topology; queries that explicitly
    # `SET cluster = 'quickstart'` fail before the actual SELECT runs.
    "unknown cluster",
    # Some captured queries reference user objects that don't exist
    # because we deliberately skip object creation (the workload only
    # exercises catalog reads).  Same demotion as above.
    'schema "',
)


def is_expected_replay_error(msg: str) -> bool:
    """True if the SUT error matches a captured-vs-current-build mismatch."""
    lo = msg.lower()
    return any(pat.lower() in lo for pat in EXPECTED_REPLAY_ERROR_PATTERNS)


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)
