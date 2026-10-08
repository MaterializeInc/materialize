# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""SQL access to the system under test, with error classification.

Under fault injection most statements can fail for reasons that are not bugs.
`classify` decides which, from the SQLSTATE and a masked message template, so
drivers neither bail on expected errors nor hide real ones behind substring
matches. An internal error (XX000) is a violation unless its template is on one
of the reviewed lists, `CATALOG_RACE_TEMPLATES` or `DESIGNED_INTERNAL_TEMPLATES`;
other SQLSTATEs outside the rejected and indeterminate sets are violations
unless their template is in `REJECTED_TEMPLATES`.
"""

from __future__ import annotations

import enum
import re
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import psycopg

from materialize.antithesis.endpoints import INTERNAL_SQL_PORT, SQL_PORT


class Outcome(enum.Enum):
    INDETERMINATE = "indeterminate"
    """The statement may or may not have taken effect (connection lost, timeout)."""
    REJECTED = "rejected"
    """The statement definitely did not take effect, for a reason a correct
    system can give under concurrency or faults."""
    VIOLATION = "violation"
    """No correct system returns this. Report it."""


# SQLSTATE classes and codes a correct system returns under faults or
# concurrent DDL, where the statement did not take effect.
_REJECTED_CODES = {
    "40001",  # serialization_failure
    "40P01",  # deadlock_detected
    "42704",  # undefined_object: concurrent drop of a dependency
    "3F000",  # invalid_schema_name: concurrent drop
    "3D000",  # invalid_catalog_name: concurrent drop
    "55000",  # object_not_in_prerequisite_state
    "53300",  # too_many_connections
    "25006",  # read_only_sql_transaction: read-only generation
}

# SQLSTATEs after which the outcome is unknown: the server went away.
_INDETERMINATE_CODES = {
    # query_canceled: a write cancelled by statement_timeout may already be
    # in group commit.
    "57014",
    "57P01",  # admin_shutdown
    "57P02",  # crash_shutdown
    "57P03",  # cannot_connect_now
    # idle_in_transaction_session_timeout: the server ended the session.
    "25P03",
    "08000",
    "08003",
    "08006",
    "08001",
    "08004",
}


class CatalogRace(enum.Enum):
    MISSING = "missing"
    """A named object, or one it depends on, was dropped concurrently."""
    EXISTS = "exists"
    """The name was taken by a concurrent create."""
    DEPENDED_UPON = "depended_upon"
    """A non-cascading drop found dependents, possibly created concurrently."""


# Losing a race with concurrent DDL. Materialize reports these as XX000:
# `AdapterError::code` maps catalog errors and most `PlanError`s to
# INTERNAL_ERROR, and it never emits 42P01, 42P07, or 42710 as errors. The
# statement did not take effect. Each entry is a regex over the masked
# template, derived from the `Display` of `mz_sql::catalog::CatalogError` and
# `PlanError::DependentObjectsStillExist`. Configuration errors that share a
# prefix (`unknown cluster replica size`) are deliberately not matched.
CATALOG_RACE_TEMPLATES: list[tuple[re.Pattern[str], CatalogRace]] = [
    (re.compile(r"^unknown catalog item '\?'"), CatalogRace.MISSING),
    # The by-id twin of `unknown catalog item`: a statement that names an item
    # as `[<id> AS <name> ...]` after the item was dropped. `PlanError::InvalidId`
    # from `NameResolver::resolve_item_name_id` when the catalog has no item
    # with that id.
    (re.compile(r"^invalid id \?$"), CatalogRace.MISSING),
    (
        re.compile(r"^unknown (cluster|cluster replica|schema|database|role) '\?'"),
        CatalogRace.MISSING,
    ),
    (
        re.compile(r"^(catalog item|cluster|schema|database|role) '\?' already exists"),
        CatalogRace.EXISTS,
    ),
    (
        re.compile(r"^cannot create multiple replicas named '\?' on cluster '\?'"),
        CatalogRace.EXISTS,
    ),
    (
        re.compile(r"^cannot drop .*: still depended upon by "),
        CatalogRace.DEPENDED_UPON,
    ),
    (
        re.compile(r"^cannot drop .* because other objects depend on it"),
        CatalogRace.DEPENDED_UPON,
    ),
]

# Errors with a specific SQLSTATE that a correct system returns under
# concurrent DDL, keyed by SQLSTATE. The statement did not take effect.
REJECTED_TEMPLATES: dict[str, list[tuple[re.Pattern[str], CatalogRace | None]]] = {
    # A prepared statement whose result type changed under DDL, as in
    # Postgres. psycopg prepares a statement after it runs a few times.
    "0A000": [(re.compile(r"^cached plan must not change result type"), None)],
    # `AdapterError::CollectionUnreadable`: a read hold with an empty since,
    # because an input was dropped while the query was sequenced.
    "P0002": [
        (
            re.compile(r"^collection '\?' is not readable at any timestamp"),
            CatalogRace.MISSING,
        )
    ],
    # A graceful `ALTER CLUSTER` holds the cluster's replica set. Releases
    # before v26.46 also reject replication factor changes during it, which the
    # upgrade scenario reaches while it runs the older release.
    "55006": [
        (
            re.compile(
                r"^cannot (change replication factor|change the cluster schedule"
                r"|convert cluster to unmanaged) while a reconfiguration is in progress"
            ),
            None,
        )
    ],
}

# Internal errors that the system raises on purpose. Each entry is a regex over
# the masked template (see `_mask`). Keep this list short and reviewed: every
# entry hides a class of XX000 from every property.
DESIGNED_INTERNAL_TEMPLATES: list[re.Pattern[str]] = [
    # A pending peek cancelled because a dependency was dropped. The same race
    # on another path returns 42704.
    re.compile(r"^query could not complete because .* was dropped"),
    # A peek or subscribe pinned to a replica (`cluster_replica`) that crashed
    # or was dropped mid-read (`ERROR_TARGET_REPLICA_FAILED`).
    re.compile(r"^target replica failed or was dropped"),
]


def _mask(message: str) -> str:
    """Replace identifiers, numbers, and quoted text so messages group by template."""
    message = re.sub(r'"[^"]*"', '"?"', message)
    message = re.sub(r"'[^']*'", "'?'", message)
    message = re.sub(r"\b[us]\d+\b", "?", message)
    return re.sub(r"\b\d+\b", "?", message)


@dataclass(frozen=True)
class Classified:
    outcome: Outcome
    sqlstate: str | None
    template: str
    race: CatalogRace | None = None
    """Set when the error is a lost race with concurrent DDL."""


def catalog_race(template: str) -> CatalogRace | None:
    for pattern, race in CATALOG_RACE_TEMPLATES:
        if pattern.search(template):
            return race
    return None


def classify(error: BaseException) -> Classified:
    if isinstance(error, psycopg.Error):
        sqlstate = error.sqlstate
        message = str(error).strip().splitlines()[0] if str(error).strip() else ""
        template = _mask(message)
        if sqlstate is None:
            # No server response: the connection broke mid-statement.
            return Classified(Outcome.INDETERMINATE, None, template)
        if sqlstate in _INDETERMINATE_CODES or sqlstate.startswith("08"):
            return Classified(Outcome.INDETERMINATE, sqlstate, template)
        if sqlstate in _REJECTED_CODES:
            race = CatalogRace.MISSING if sqlstate == "42704" else None
            return Classified(Outcome.REJECTED, sqlstate, template, race)
        if sqlstate == "XX000":
            race = catalog_race(template)
            if race is not None:
                return Classified(Outcome.REJECTED, sqlstate, template, race)
            if any(p.search(template) for p in DESIGNED_INTERNAL_TEMPLATES):
                return Classified(Outcome.REJECTED, sqlstate, template)
            return Classified(Outcome.VIOLATION, sqlstate, template)
        for pattern, race in REJECTED_TEMPLATES.get(sqlstate, []):
            if pattern.search(template):
                return Classified(Outcome.REJECTED, sqlstate, template, race)
        return Classified(Outcome.VIOLATION, sqlstate, template)
    if isinstance(error, (OSError, TimeoutError)):
        return Classified(Outcome.INDETERMINATE, None, type(error).__name__)
    return Classified(Outcome.VIOLATION, None, f"{type(error).__name__}: {error}")


# `AdapterError::ImpossibleTimestampConstraints`, SQLSTATE 22000.
_AS_OF_SINCE_TEMPLATE = re.compile(r"^could not find a valid timestamp")


def classify_as_of_read(error: BaseException) -> Classified:
    """`classify` for a read issued `AS OF` an explicit timestamp.

    Such a read is rejected when compaction advanced a since past the chosen
    timestamp between choosing it and running the read. Without an explicit
    `AS OF` the coordinator picks the timestamp itself, so the same error is a
    violation, which is why `classify` does not accept it.
    """
    c = classify(error)
    if c.sqlstate == "22000" and _AS_OF_SINCE_TEMPLATE.search(c.template):
        return Classified(Outcome.REJECTED, c.sqlstate, c.template)
    return c


def connect(
    host: str,
    *,
    internal: bool = False,
    connect_timeout: int = 10,
    statement_timeout_ms: int | None = 60_000,
    autocommit: bool = True,
    options: dict[str, str] | None = None,
) -> psycopg.Connection:
    """Open a connection to environmentd.

    `internal` connects as `mz_system` on the internal SQL port, which is
    needed for `ALTER SYSTEM` and some introspection. Such sessions default to
    the `mz_catalog_server` cluster: `mz_system`'s own default cluster runs
    with no replicas in this harness, so reads there could never be served.
    """
    settings = dict(options or {})
    if internal:
        settings.setdefault("cluster", "mz_catalog_server")
    if statement_timeout_ms is not None:
        settings.setdefault("statement_timeout", f"{statement_timeout_ms}ms")
    opts = " ".join(f"-c {k}={v}" for k, v in settings.items())
    return psycopg.connect(
        host=host,
        port=INTERNAL_SQL_PORT if internal else SQL_PORT,
        user="mz_system" if internal else "materialize",
        dbname="materialize",
        connect_timeout=connect_timeout,
        autocommit=autocommit,
        options=opts or None,
    )


@contextmanager
def connection(host: str, **kwargs: object) -> Iterator[psycopg.Connection]:
    conn = connect(host, **kwargs)  # type: ignore[arg-type]
    try:
        yield conn
    finally:
        try:
            conn.close()
        except psycopg.Error:
            pass


def connect_with_retry(
    host: str, deadline_seconds: float, interval: float = 2.0, **kwargs: object
) -> psycopg.Connection:
    """Retry `connect` until it succeeds or the deadline passes."""
    deadline = time.monotonic() + deadline_seconds
    while True:
        try:
            return connect(host, **kwargs)  # type: ignore[arg-type]
        except (psycopg.Error, OSError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(interval)
