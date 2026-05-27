# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Canonical fault-injection-shape pattern list for Antithesis drivers.

Antithesis injects faults (kill / pause / partition / drop / dup) at
the container layer.  Each fault overlaps some sensitive moment in the
SUT or a driver's interaction with it, surfacing as one of a fairly
small family of error strings — most of which are produced by upstream
crates (tokio-postgres, tiberius, librdkafka, hyper, reqwest, glibc
resolver) rather than by Materialize itself, so they're stable across
SUT revisions and worth tracking centrally.

This module owns the **union** of those fault-shape strings.  Every
Antithesis driver that needs to demote "looks like a fault, not a bug"
from `always(False)` to `sometimes(False)` should call
`looks_like_fault(msg)` rather than maintaining its own list, otherwise
the list drifts per-driver (e.g. `helper_testdrive.py` historically had
`"Failed to resolve hostname"` but not `"Name or service not known"`,
so an mysql-cdc fault hit `EAI_NONAME` and escaped tolerance even
though the parallel-workload driver had absorbed the same shape).

What belongs here:
  * Network errors that fire when Antithesis kills/pauses/partitions a
    container the driver is mid-conversation with (TCP-level resets,
    closed sockets, refused connections).
  * DNS-resolver errors that fire when the workload container's resolver
    can't find the target (EAI_AGAIN / EAI_NONAME).
  * Driver-specific disconnect strings produced by upstream crates we
    use (tokio-postgres "connection closed", tiberius "No more packets
    in the wire", librdkafka "OperationTimedOut", reqwest "Failed to
    execute http request").
  * Materialize-specific shapes produced by environmentd / clusterd
    during their own restart windows ("is (re)initializing",
    "TooManyRequests" from admission control,
    "terminating connection due to administrator command" from
    server-initiated drops).

What does NOT belong here:
  * Concurrent-driver race shapes ("already exists", "was concurrently
    dropped", "unknown role").  Those are not fault injection — they're
    the cost of running multiple driver invocations against a shared
    catalog.  The parallel-workload driver keeps its own
    `_SETUP_RACE_PATTERNS` list because that surface is unique to
    setup-phase concurrency.
  * Property-violation indicators (SinceViolation, `as_of not beyond`,
    etc.).  Those are the *opposite* of fault tolerance — drivers want
    to fire `always(False)` when they see one.  They stay in the
    driver that owns the property.

When adding a new pattern: include a one-line comment naming the
producer crate and the in-the-wild context (which directive / which
sequencer), so a future reader can verify the pattern is still relevant
when the upstream wording shifts.
"""

from __future__ import annotations

# Canonical fault-injection patterns.  Case-folded substring matched
# against the combined stdout+stderr blob (testdrive) or the bare
# exception message (parallel-workload-style drivers).  Order doesn't
# matter; patterns are evaluated as a flat `any()`.
#
# Patterns are stored in their natural casing for readability; the
# matcher lowercases both sides.
FAULT_PATTERNS: tuple[str, ...] = (
    # ---- Network / TCP-level disconnects ----------------------------
    # Generic connection failures that fire when Antithesis kills or
    # pauses the target container mid-conversation.
    "connection refused",
    "connection reset",
    "no route to host",
    "broken pipe",
    "could not connect to server",
    "connection timeout",
    "Multiple connection attempts failed",
    "EOF detected",  # psycopg's wording for peer-closed during query
    "the connection is lost",
    "server closed the connection",  # tokio-postgres + psycopg
    "Unable to connect to MySQL server",  # testdrive's $ mysql-connect
    # tokio-postgres bare "connection closed" wrapped by testdrive as
    # `error: executing query failed: connection closed` or
    # `error: preparing query failed: connection closed`.  The upstream
    # crates only produce this string when the TCP stream drops, never
    # for a successful round-trip — safe to match broadly.
    "connection closed",
    # reqwest's wording when materialized's HTTP response (e.g. the
    # `--check-catalog-consistency` GET /api/catalog/dump) is cut off.
    "connection closed before message completed",
    # duckdb's httpfs extension when its S3 client can't reach the
    # endpoint (minio or polaris paused/killed/clogged).  Surfaces via
    # testdrive's `$ duckdb-query` as `IO Error: Could not establish
    # connection error for HTTP GET to '...'`.  Distinct from
    # "could not connect to server" (psycopg/libpq) — this one is the
    # duckdb side's wording for the same TCP-level failure.
    "Could not establish connection",
    # reqwest's outer wording when the response body deserialization fails
    # because the connection was torn mid-body.  Observed via the iceberg
    # driver's testdrive duckdb-query: the duckdb iceberg catalog client
    # reaches into the REST API, polaris's TCP gets dropped under fault
    # injection, and the full error chain surfaces as
    # `error: catalog inconsistency: catalog state / Caused by: deserialize
    # catalog / error decoding response body / error reading a body from
    # connection / end of file before message length reached`.  Matching
    # the reqwest-level wording absorbs the whole family at once — the
    # deeper hyper-level "end of file before message length reached" line
    # is also present, but this string fires earlier in the chain and is
    # the most stable across reqwest version bumps.
    "error decoding response body",
    # tiberius (Rust TDS driver) when sql-server's connection dies
    # mid-statement.  Wrapped by testdrive's `$ sql-server-execute`
    # directive as `error: executing SQL Server query: ...`.
    "No more packets in the wire",
    # librdkafka admin-client wording when the broker is paused long
    # enough that an admin op (e.g. CREATE SINK validation's metadata
    # fetch) exceeds its timeout.  Distinct from BrokerTransportFailure
    # below — this is connection-up-but-not-responding, not
    # connection-failed.
    "OperationTimedOut",
    # librdkafka producer/consumer wording when the broker is
    # unreachable at all (Antithesis killed kafka).
    "BrokerTransportFailure",
    # reqwest transport-failure wording when an HTTP target is gone —
    # currently surfaces via polaris's REST handler when its upstream
    # postgres-metadata is paused (wrapped by materialized as
    # `failed to list namespaces: Unexpected => Failed to execute http
    # request, source: error sending request for url (http://polaris...`)
    # but the pattern is generic to any reqwest call to a paused target.
    "Failed to execute http request",
    # ---- DNS resolver ----------------------------------------------
    # glibc surfaces resolver failures via socket.gaierror with one of
    # these messages; both Materialize-side and workload-side DNS
    # lookups hit them under network-partition fault windows.
    "Temporary failure in name resolution",  # EAI_AGAIN — partitioned
    "Name or service not known",  # EAI_NONAME — container gone
    # Materialize's own catch-all wording for the same condition (its
    # CREATE CONNECTION validation path uses a different resolver
    # plumbing that surfaces this string before glibc's gaierror).
    "Failed to resolve hostname",
    # Materialize's getaddrinfo() failure wording, surfaced through
    # testdrive as
    # `error: executing query failed: db error: ERROR: failed to lookup
    # address information: Name or service not known`.  We match the
    # prefix as well as the EAI_NONAME suffix above so either substring
    # absorbs the failure.
    "failed to lookup address information",
    # ---- Server-initiated drops & admission control ----------------
    # Postgres / Materialize wording when the server side terminates an
    # in-flight backend (e.g. environmentd kill, pg_terminate_backend
    # via polaris during its own restart).
    "terminating connection due to administrator command",
    # Materialize / persist admission control under load — surfaces via
    # S3-backed persist as a `StatusCode::TooManyRequests` (HTTP 429)
    # bubbled through reqwest error chains from `src/persist/src/location.rs`.
    "TooManyRequests",
    # HikariCP-style (polaris's connection pool) exhaustion when the
    # JDBC upstream stays paused longer than the pool's wait budget.
    "Acquisition timeout while waiting for new connection",
    # Polaris's JDBC PostgreSQL driver (org.postgresql) raises a PSQLException
    # with this exact wording when its socket to postgres-metadata is torn
    # mid-write — verbatim from `PgStream.flush()` in the upstream driver.
    # Surfaces through Materialize's CREATE CONNECTION ... TO ICEBERG
    # validation path as
    # `failed to list namespaces: DataInvalid, context: { type:
    #  RuntimeException, code: 500 } => Failed to retrieve polaris entity
    #  due to Failed due to 'An I/O error occurred while sending to the
    #  backend.' (error code 0, sql-state '08006')`.
    # Distinct from `"terminating connection due to administrator command"`
    # (server-side pg_terminate_backend) — that fires when postgres-metadata
    # itself cleanly tears the session; this one fires when the TCP path is
    # cut while the JDBC driver is mid-send.
    "An I/O error occurred while sending to the backend",
    # SQLSTATE 08006 = `connection_failure` (Postgres standard). Catch-all
    # for any 08006 surface — broadens coverage beyond the specific JDBC
    # wording above so future Polaris driver versions / different inner
    # transports don't re-escape tolerance under the same fault profile.
    "sql-state '08006'",
    # ---- Polaris 401 under slowed/torn OAuth handshakes -------------
    # When Antithesis injects a `Slowed` network partition between the
    # workload container and polaris, the duckdb iceberg extension's
    # OAuth POST occasionally races with the followup `/v1/config`
    # request: the token response arrives late or truncated, duckdb
    # sends an invalid/empty Bearer to `/v1/config?warehouse=…`, and
    # polaris responds 401.  Surface form (verbatim from the duckdb
    # iceberg extension):
    #   error: executing DuckDB query: ATTACH 'default_catalog' AS polaris …:
    #   Invalid Configuration Error: Request to
    #   'http://polaris:8181/api/catalog/v1/config?warehouse=default_catalog'
    #   returned a non-200 status code (Unauthorized_401), with reason: Unauthorized
    # Observed correlated 1:1 with an active `partition`/`Slowed` fault
    # in the failure window; not observed when polaris is reachable.
    # Pattern matches both the duckdb extension wording and any future
    # 401 surface that includes "Unauthorized_401" or the bare reason
    # string.  Intentionally narrow ("Unauthorized_401" with the
    # underscore form duckdb prints) rather than the bare word
    # "Unauthorized" to avoid swallowing genuine permission errors from
    # other surfaces.
    "Unauthorized_401",
    "non-200 status code (Unauthorized",
)


def looks_like_fault(msg: str) -> bool:
    """True if `msg` contains any canonical fault-injection pattern.

    Case-folded substring match.  Used by every driver that needs to
    classify "fault-injection noise" vs "real bug" — see this module's
    docstring for the categorisation.

    Match is intentionally generous (substring, case-folded): false-
    positive transients sacrifice some property-violation signal but
    avoid false-positive `always(False)` calls, which are far costlier
    in triage than a missed catch.
    """
    lo = msg.lower()
    return any(pat.lower() in lo for pat in FAULT_PATTERNS)
