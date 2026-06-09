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
    # ---- Compute replica fault during a replica-targeted read --------
    # When a query is pinned to a specific replica (SET cluster_replica)
    # and Antithesis kills/pauses that replica's clusterd mid-read, the
    # adapter surfaces this.  Hit by the cross-replica-consistency
    # driver, which reads each replica in turn; the fault that kills
    # the replica we're pinned to is expected, not a divergence.
    "target replica failed or was dropped",
    # Polaris's IllegalStateException wording when its embedded
    # postgres-backed metadata DB is mid-restart after a fault and its
    # service-layer reads the schema version before bootstrap finishes.
    # Surfaces through materialized's `CREATE CONNECTION TO ICEBERG
    # CATALOG (...)` validation as
    # `failed to list namespaces: DataInvalid, context: { type:
    # IllegalStateException, code: 500 } => Failed to retrieve schema
    # version`.  Distinct from `Failed to execute http request` (which
    # fires when Polaris's TCP socket is unreachable); this is "Polaris
    # is up but its DB schema isn't yet ready".
    "Failed to retrieve schema version",
    # ---- Materialize timestamp / since transients --------------------
    # Materialize rejects a query whose chosen `AS OF` is below the
    # storage input's since.  Outside fault injection the planner picks
    # AS OF >= since by construction; under fault windows, however,
    # materialized restarts (or a replica failover) can let persist
    # compaction advance since past a timestamp a long-lived driver
    # already witnessed and would otherwise reuse on reconnect.  Today
    # the `singleton_driver_subscribe_correctness` consumer thread is
    # the in-tree caller most likely to hit this — it saves the latest
    # PROGRESS timestamp and uses it as AS OF on every reconnect; if a
    # fault window's downtime exceeds the durable since-pin lifetime
    # the reconnect query is rejected with this exact wording.
    "could not find a valid timestamp for the query",
    # ---- statement_timeout under fault load -------------------------
    # Materialize/Postgres wording when a query exceeds its session's
    # `statement_timeout`.  Drivers that pin a per-query timeout (e.g.
    # `parallel_driver_cross_replica_consistency` and
    # `parallel_driver_differential_query` use 10s; `testdrive`'s
    # default pgwire timeout for `INSERT … generate_series` etc.) hit
    # this whenever Antithesis's fault windows slow the SUT enough
    # that the per-query budget elapses.  That's expected under
    # fault injection, not a property violation — without this entry
    # the cross-replica/differential drivers misfire `always(False)`
    # at heavier fault densities (observed once 4h runs were long
    # enough to land enough faults on a single read).  Same wording
    # surfaces from a testdrive subprocess in its stdout when the
    # underlying query gets canceled and helper_testdrive subsequently
    # kills the process at its 600s wall budget.
    "canceling statement due to statement timeout",
    # `helper_testdrive.run()` kills the testdrive subprocess at its
    # wall-clock budget (default 600s; platform-checks' `initialize`
    # phase uses 300s) and appends this marker to stderr so the caller
    # can distinguish a wall-budget kill from a clean non-zero exit.
    # Under Antithesis fault windows the per-phase budget is routinely
    # exceeded simply because the SUT is being killed/paused for
    # 20-40s at a stretch; treating that as a property violation
    # produces noise.  Without this, platform-check's "runs to
    # completion" assertion and the testdrive driver's "non-transient
    # error" assertion both misfire `always(False)` when a check's
    # `initialize` phase or the random .td script gets cut off at the
    # wall budget without the underlying query error reaching stdout
    # (e.g. the SUT was paused with the testdrive client blocked
    # mid-`>` directive, no error returned yet).
    "[helper_testdrive] timed out after",
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
