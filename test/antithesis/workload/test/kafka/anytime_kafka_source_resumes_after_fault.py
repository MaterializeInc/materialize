#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for `kafka-source-survives-broker-fault` and
`kafka-source-survives-clusterd-restart` (combined liveness signal).

Both catalog properties amount to: after a transient fault that prevents
the source from making progress, once the fault is over the source must
ingest the messages it was unable to read during the outage. Externally
this looks identical for either fault kind — `offset_committed` stalls
during the outage and resumes advancing afterward — so one anytime driver
records the stall-then-advance transition and we tag the corroborating
fault signal (kafka broker reachable / replica online) in `details` so
triage can distinguish the two cases on a hit.

Per-invocation state machine, per source:
  - `IDLE` (initial). On a successful sample, store the offset and move
    to `OBSERVING`.
  - `OBSERVING`. If the sample equals the stored value for STALL_TICKS
    consecutive ticks, move to `STALLED` (the source has stopped
    progressing — most likely fault-induced). Otherwise, refresh the
    stored value.
  - `STALLED`. On any sample strictly greater than the stalled value, fire
    the `sometimes(...)` recovery anchor and return to `OBSERVING` with
    the new value. Otherwise stay stalled.

Failed samples (clusterd unavailable, network partition) do not transition
the state machine — they are the fault-active condition we want to bridge
over. They are counted only so the `details` payload can corroborate the
recovery transition.

The driver also records two corroborating `sometimes(...)` signals so
triage can confirm Antithesis actually hit each of the two fault classes
this property cluster cares about:
  - replica went non-online (clusterd-restart signal)
  - direct Kafka admin metadata fetch failed (broker-fault signal)
"""

from __future__ import annotations

import os
import sys
import time

import helper_logging
from helper_pg import query_one_retry, query_retry

from antithesis.assertions import reachable, sometimes

LOG = helper_logging.setup_logging("driver.kafka_source_resumes_after_fault")

POLL_INTERVAL_S = 1.0
RUN_BUDGET_S = 45.0
# Number of consecutive identical samples after which we consider the source
# "stalled" rather than just briefly idle. Five seconds (5 ticks * 1s)
# comfortably exceeds the natural quiet-period between produces but is well
# below the fault-injection windows Antithesis schedules.
STALL_TICKS = 5

ANTITHESIS_CLUSTER = "antithesis_cluster"
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")


def _kafka_sources() -> list[str]:
    rows = query_retry(
        """
        SELECT s.name
        FROM mz_sources s
        JOIN mz_clusters c ON c.id = s.cluster_id
        WHERE c.name = %s AND s.type = 'kafka'
        """,
        (ANTITHESIS_CLUSTER,),
    )
    return [r[0] for r in rows]


def _offset_committed(source_name: str) -> int | None:
    """Aggregated offset_committed across workers for `source_name`."""
    row = query_one_retry(
        """
        SELECT MAX(ss.offset_committed)::bigint
        FROM mz_internal.mz_source_statistics ss
        JOIN mz_sources s ON s.id = ss.id
        WHERE s.name = %s
        """,
        (source_name,),
    )
    if row is None or row[0] is None:
        return None
    return int(row[0])


def _replica_non_online() -> bool:
    """Did any antithesis_cluster replica record an `offline` status in this
    timeline? Queries the audit history (`mz_cluster_replica_status_history`)
    rather than the current-state view so a transient offline window between
    two polls is still observable. See the matching helper in
    `anytime_fault_recovery_exercised.py` for the full reasoning.
    """
    try:
        row = query_one_retry(
            """
            SELECT EXISTS (
                SELECT 1
                FROM mz_internal.mz_cluster_replica_status_history h
                JOIN mz_cluster_replicas r ON r.id = h.replica_id
                JOIN mz_clusters c ON c.id = r.cluster_id
                WHERE c.name = %s AND h.status = 'offline'
            )
            """,
            (ANTITHESIS_CLUSTER,),
        )
    except Exception:  # noqa: BLE001
        return False
    return bool(row and row[0])


def _kafka_metadata_failed() -> bool:
    """Best-effort: did a direct Kafka metadata fetch fail?

    A successful Materialize-side ingestion still goes through the broker,
    so a metadata fetch failure here is a strong signal that the
    `materialized <-> kafka` channel was partitioned even though the
    `materialized <-> postgres-metadata` channel still works (the
    `kafka-source-survives-broker-fault` shape).

    Defensive imports because the kafka admin client only runs cleanly with
    a reachable broker. We avoid raising into the polling loop.
    """
    try:
        from confluent_kafka.admin import AdminClient
    except Exception:  # noqa: BLE001
        return False
    try:
        AdminClient({"bootstrap.servers": KAFKA_BROKER}).list_topics(timeout=2)
        return False
    except Exception:  # noqa: BLE001
        return True


def main() -> int:
    deadline = time.monotonic() + RUN_BUDGET_S

    # Per-source state machine.
    #   state: "OBSERVING" or "STALLED"
    #   last_value: most recent committed offset observed
    #   stall_streak: consecutive ticks at last_value
    states: dict[str, dict] = {}

    # Cross-source corroborating signals collected throughout this run.
    saw_replica_non_online = False
    saw_kafka_metadata_failure = False
    # Per-source: did we observe stall->advance at least once.
    resumed_after_stall: dict[str, bool] = {}

    while time.monotonic() < deadline:
        if _replica_non_online():
            saw_replica_non_online = True
        if _kafka_metadata_failed():
            saw_kafka_metadata_failure = True

        try:
            sources = _kafka_sources()
        except Exception as exc:  # noqa: BLE001
            LOG.info("source list query failed: %s; sleeping", exc)
            time.sleep(POLL_INTERVAL_S)
            continue

        for source in sources:
            try:
                observed = _offset_committed(source)
            except Exception as exc:  # noqa: BLE001
                LOG.info("offset_committed query failed for %s: %s", source, exc)
                continue
            if observed is None:
                continue

            st = states.setdefault(
                source,
                {"state": "OBSERVING", "last_value": observed, "stall_streak": 0},
            )

            if st["state"] == "OBSERVING":
                if observed == st["last_value"]:
                    st["stall_streak"] += 1
                    if st["stall_streak"] >= STALL_TICKS:
                        st["state"] = "STALLED"
                else:
                    # Progress: reset.
                    st["last_value"] = observed
                    st["stall_streak"] = 0
            else:  # STALLED
                if observed > st["last_value"]:
                    # Recovery transition: fire the per-source signal once
                    # per invocation (we still update state so we can detect
                    # additional stalls and resumes).
                    if not resumed_after_stall.get(source, False):
                        resumed_after_stall[source] = True
                        # Reaching here is the property: a source was stalled,
                        # then advanced. Use `reachable(...)` rather than
                        # `sometimes(True, ...)` per the SDK assertion-type
                        # guidance.
                        reachable(
                            "kafka source: offset_committed resumed advancing after a sustained stall",
                            {
                                "source": source,
                                "stalled_at": st["last_value"],
                                "observed_after_recovery": observed,
                                "stall_ticks_required": STALL_TICKS,
                                "saw_replica_non_online": saw_replica_non_online,
                                "saw_kafka_metadata_failure": saw_kafka_metadata_failure,
                            },
                        )
                    st["state"] = "OBSERVING"
                    st["last_value"] = observed
                    st["stall_streak"] = 0

        time.sleep(POLL_INTERVAL_S)

    sometimes(
        saw_replica_non_online,
        "kafka source resumes: observed antithesis_cluster replica non-online",
        {"resumed_sources": sorted(resumed_after_stall.keys())},
    )
    sometimes(
        saw_kafka_metadata_failure,
        "kafka source resumes: observed direct Kafka metadata fetch failure",
        {"resumed_sources": sorted(resumed_after_stall.keys())},
    )

    LOG.info(
        "kafka-source-resumes-after-fault done; sources_resumed=%d replica_offline=%s metadata_failed=%s",
        sum(1 for v in resumed_after_stall.values() if v),
        saw_replica_non_online,
        saw_kafka_metadata_failure,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
