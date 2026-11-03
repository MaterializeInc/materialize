# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Consistency probes that run against a single relation."""

import enum
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from materialize.mzbisect.catalog import ObjectInfo, ReplicaInfo
from materialize.mzbisect.db import ConnParams, Db, error_message, literal

# Substrings (lowercased) of server error messages that indicate a
# consistency violation rather than an ordinary query failure.
CORRUPTION_MARKERS = (
    "non-positive accumulation",
    "non-positive multiplicity",
    "negative accumulation",
    "negative multiplicity",
    "net-zero records",
    "invalid data in source",
)

# Substrings of errors that mean the fixed AS OF timestamp fell outside some
# input's valid read window and the probe should be retried with a fresh
# timestamp.
STALE_TIMESTAMP_MARKERS = (
    "not valid for all inputs",
    "has been compacted",
    "could not find a valid timestamp",
)


class Status(enum.Enum):
    OK = "ok"
    INFO = "info"
    CORRUPT = "CORRUPT"
    FAILED = "failed"


@dataclass(frozen=True)
class ProbeResult:
    probe: str
    status: Status
    detail: str


@dataclass(frozen=True)
class FingerprintTarget:
    """One place to read a relation from, at a fixed AS OF."""

    cluster_name: str
    # None reads from whichever replica responds first, which is fine for the
    # scratch cluster (it has exactly one replica).
    replica_name: str | None


def is_corruption_error(message: str) -> bool:
    lowered = message.lower()
    return any(marker in lowered for marker in CORRUPTION_MARKERS)


def _is_stale_timestamp_error(message: str) -> bool:
    lowered = message.lower()
    return any(marker in lowered for marker in STALE_TIMESTAMP_MARKERS)


def _result_for_error(probe: str, e: BaseException) -> ProbeResult:
    message = error_message(e)
    if is_corruption_error(message):
        return ProbeResult(probe, Status.CORRUPT, f"query errored: {message}")
    return ProbeResult(probe, Status.FAILED, f"probe failed: {message}")


def scan_probe(db: Db, obj: ObjectInfo) -> ProbeResult:
    """Read the whole relation once.

    Surfaces errors persisted alongside the relation's data as well as errors
    thrown while hydrating it from persist.
    """
    try:
        row = db.query_one(f"SELECT count(*) AS c FROM {obj.qualified} AS t")
        return ProbeResult("scan", Status.OK, f"count(*) = {row['c']}")
    except Exception as e:
        return _result_for_error("scan", e)


def potato_probe(db: Db, obj: ObjectInfo, negative: bool, sample: int) -> ProbeResult:
    """Group by the full row and look for suspicious multiplicities.

    With negative=True, reports rows with non-positive multiplicities, which
    are always corruption. With negative=False, reports duplicate rows, which
    are corruption only where uniqueness is expected, so callers should treat
    that variant as informational.
    """
    name = "potato-negative" if negative else "potato-duplicates"
    predicate = "count(*) < 1" if negative else "count(*) > 1"
    try:
        rows = db.query(f"""
            SELECT r::text AS bad_row, multiplicity
            FROM (
                SELECT row(t.*) AS r, count(*) AS multiplicity
                FROM {obj.qualified} AS t
                GROUP BY 1
                HAVING {predicate}
            ) AS potato
            LIMIT {sample}
            """)
    except Exception as e:
        return _result_for_error(name, e)

    if not rows:
        return ProbeResult(name, Status.OK, "no offending rows")

    shown = "; ".join(
        f"multiplicity {r['multiplicity']}: {_truncate(r['bad_row'])}" for r in rows
    )
    if negative:
        detail = f"rows with non-positive multiplicity (sample): {shown}"
        return ProbeResult(name, Status.CORRUPT, detail)
    else:
        detail = (
            f"duplicate rows exist (corruption only if uniqueness expected): {shown}"
        )
        return ProbeResult(name, Status.INFO, detail)


def _truncate(s: str, limit: int = 120) -> str:
    return s if len(s) <= limit else s[: limit - 3] + "..."


def _fingerprint_query(obj: ObjectInfo, ts: str) -> str:
    # seahash yields uint8. The numeric cast matters: summing an unsigned
    # type over rows with negative multiplicities trips the reduce's
    # "invalid negative unsigned aggregation" error path, while a numeric sum
    # flows through and lands in the checksum, which is exactly what we want
    # to observe.
    return f"""
        SELECT count(*)::text AS row_count,
               coalesce(sum(seahash(row(t.*)::text)::numeric), 0)::text AS checksum
        FROM {obj.qualified} AS t
        AS OF {ts}
    """


_QUERY_TS_RE = re.compile(r"query timestamp: *(\d+)")


def _target_timestamp(
    params: ConnParams,
    obj: ObjectInfo,
    cluster_name: str,
    timeout_secs: int,
) -> int:
    """The minimal timestamp at which cluster_name can read obj.

    mz_now() is not a valid choice for the shared AS OF: arrangement
    compaction rounds frontiers up to second boundaries, so an index's since
    can sit ahead of the oracle's read timestamp. Instead ask each cluster
    what timestamp it would pick, and let the caller take the max.
    """
    db = Db(params)
    try:
        db.execute(f"SET statement_timeout = '{timeout_secs}s'")
        db.execute(f"SET cluster = {literal(cluster_name)}")
        rows = db.query(f"EXPLAIN TIMESTAMP FOR SELECT count(*) FROM {obj.qualified}")
        text = str(next(iter(rows[0].values())))
        m = _QUERY_TS_RE.search(text)
        if m is None:
            raise RuntimeError(
                f"could not parse EXPLAIN TIMESTAMP output: {text[:200]!r}"
            )
        return int(m.group(1))
    finally:
        db.close()


def _fingerprint_one(
    params: ConnParams,
    obj: ObjectInfo,
    target: FingerprintTarget,
    ts: str,
    timeout_secs: int,
) -> tuple[str, str]:
    db = Db(params)
    try:
        db.execute(f"SET statement_timeout = '{timeout_secs}s'")
        db.execute(f"SET cluster = {literal(target.cluster_name)}")
        if target.replica_name is not None:
            db.execute(f"SET cluster_replica = {literal(target.replica_name)}")
        row = db.query_one(_fingerprint_query(obj, ts))
        return (row["row_count"], row["checksum"])
    finally:
        db.close()


def fingerprint_probe(
    params: ConnParams,
    obj: ObjectInfo,
    home_targets: dict[str, list[ReplicaInfo]],
    scratch_cluster: str,
    timeout_secs: int,
    attempts: int = 3,
) -> list[ProbeResult]:
    """Compare incumbent index arrangements against a fresh read from persist.

    home_targets maps cluster name to the replicas of that cluster, for every
    cluster that holds an index on obj. All reads happen at one AS OF
    timestamp, chosen as the max over every involved cluster's own timestamp
    determination. The queries are issued concurrently so each acquires read
    holds while the timestamp is still within every input's read window. A
    sequential second query would find its inputs already compacted past the
    shared timestamp.
    """
    results: list[ProbeResult] = []

    targets: list[FingerprintTarget] = []
    for cluster_name, replicas in sorted(home_targets.items()):
        if not replicas:
            results.append(
                ProbeResult(
                    f"arrangement {cluster_name}",
                    Status.FAILED,
                    "cluster has no replicas, nothing to compare",
                )
            )
            continue
        for replica in replicas:
            targets.append(FingerprintTarget(cluster_name, replica.name))
    if not targets:
        return results

    scratch_target = FingerprintTarget(scratch_cluster, None)
    all_targets = targets + [scratch_target]

    clusters = sorted({t.cluster_name for t in all_targets})
    outcomes: dict[FingerprintTarget, tuple[str, str] | BaseException] = {}
    for attempt in range(attempts):
        try:
            ts = str(
                max(
                    _target_timestamp(params, obj, cluster, timeout_secs)
                    for cluster in clusters
                )
            )
        except Exception as e:
            results.append(_result_for_error("fingerprint timestamp", e))
            return results
        with ThreadPoolExecutor(max_workers=len(all_targets)) as pool:
            futures = {
                target: pool.submit(
                    _fingerprint_one, params, obj, target, ts, timeout_secs
                )
                for target in all_targets
            }
            outcomes = {}
            for target, future in futures.items():
                try:
                    outcomes[target] = future.result()
                except Exception as e:
                    outcomes[target] = e
        stale = any(
            isinstance(o, BaseException) and _is_stale_timestamp_error(error_message(o))
            for o in outcomes.values()
        )
        if not stale:
            break

    scratch_outcome = outcomes[scratch_target]
    if isinstance(scratch_outcome, BaseException):
        results.append(_result_for_error("fingerprint persist-read", scratch_outcome))
        return results

    persist_count, persist_checksum = scratch_outcome
    results.append(
        ProbeResult(
            "fingerprint persist-read",
            Status.OK,
            f"count={persist_count} checksum={persist_checksum}",
        )
    )
    for target in targets:
        probe = f"arrangement {target.cluster_name}/{target.replica_name}"
        outcome = outcomes[target]
        if isinstance(outcome, BaseException):
            results.append(_result_for_error(probe, outcome))
        elif outcome == scratch_outcome:
            results.append(ProbeResult(probe, Status.OK, "matches persist"))
        else:
            count, checksum = outcome
            results.append(
                ProbeResult(
                    probe,
                    Status.CORRUPT,
                    f"arrangement diverges from persist: count={count} "
                    f"checksum={checksum} vs persist count={persist_count} "
                    f"checksum={persist_checksum}",
                )
            )
    return results
