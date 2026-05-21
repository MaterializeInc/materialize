#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for properties `iceberg-sink-no-data-loss` and
`iceberg-sink-no-duplication`.

Mirrors `parallel_driver_pg_cdc` from the other side of the pipeline:
every row INSERTed into the MZ-side `iceberg_src` table must
eventually appear — exactly once, with the correct value — in the
iceberg table the sink writes to via Polaris.

The driver inserts ROWS_PER_INVOCATION rows under a per-invocation
`batch_id`, waits for the sink to commit (observed via
`mz_sink_statistics.messages_committed` reaching at least
`starting + ROWS_PER_INVOCATION`), and asserts both the count and
the per-row values on the iceberg side via a duckdb query routed
through the Polaris REST catalog.

Why this is interesting for SS-148:

  The bug in SS-148 is "iceberg-rust's `Transaction::commit` reloads
  the table and re-applies the same data files when the catalog
  commit response is dropped, producing a snapshot that references
  the same data twice".  Symptomatically: COUNT(*) on the iceberg
  side > COUNT(*) on the MZ side.

  This driver makes that comparison directly: the MZ side knows
  exactly N rows under batch_id, the iceberg side is queried for
  COUNT(*) WHERE batch_id = X, and any duplicate snapshot under
  Antithesis network-fault injection would surface as `expected != N`.
  The driver does NOT specifically target the dropped-response
  failure mode — it just asserts the safety property that *would*
  catch it.  Whether Antithesis's general fault injection finds an
  interleaving that triggers SS-148 is the open question this group
  exists to answer.
"""

from __future__ import annotations

import sys
import time

import helper_iceberg
import helper_logging
import helper_random
from helper_pg import execute_retry, query_one_retry, query_retry

from antithesis.assertions import always, sometimes

LOG = helper_logging.setup_logging("driver.iceberg_no_loss_or_dup")

# Per-invocation INSERT count.  Sized smaller than the CDC drivers'
# 20 because each iceberg-side verification round-trips through a
# testdrive subprocess + a duckdb catalog query, which is materially
# heavier than a MZ-side SELECT.  Coverage comes from many
# invocations, not a big batch.
ROWS_PER_INVOCATION = 10

# Catchup budget: the sink's COMMIT INTERVAL is 2s, plus one full
# fault-orchestrator ON+OFF window (~80s) to cover injected pauses,
# plus a margin for catalog-side retries.  iceberg sinks under
# repeated faults can stall for tens of seconds even outside the
# active fault window because polaris's JDBC pool drains slowly.
CATCHUP_TIMEOUT_S = 180.0
POLL_INTERVAL_S = 1.0


def _sink_exists() -> bool:
    rows = query_retry(
        "SELECT 1 FROM mz_sinks WHERE name = %s", (helper_iceberg.SINK_NAME,)
    )
    return bool(rows)


def _insert_rows(batch_id: str) -> dict[str, str]:
    """INSERT ROWS_PER_INVOCATION rows into iceberg_src.

    Returns {id -> value} for every successfully inserted row.  Under
    fault injection an INSERT can fail; we skip the row rather than
    crashing so the driver keeps progressing.

    `id` is composed as `f"{batch_id}:{i}"` so concurrent driver
    invocations never collide on a key (each invocation gets a fresh
    batch_id from the Antithesis-routed RNG).  Same prefix-scoping
    pattern as the CDC drivers.
    """
    expected: dict[str, str] = {}
    for i in range(ROWS_PER_INVOCATION):
        row_id = f"{batch_id}:{i}"
        value = f"v{helper_random.random_int(0, 9999):04d}"
        try:
            execute_retry(
                f"INSERT INTO {helper_iceberg.SRC_TABLE} "
                f"(id, batch_id, value) VALUES (%s, %s, %s)",
                (row_id, batch_id, value),
            )
            expected[row_id] = value
        except Exception as exc:  # noqa: BLE001
            LOG.info("insert failed for row %s: %s; skipping", row_id, exc)
    return expected


def _wait_for_mz_visible(batch_id: str, expected_count: int) -> bool:
    """Poll the MZ-side count for this batch.  Returns True once the
    INSERTs are durably committed (visible to a strict-serializable
    SELECT in the workload's default isolation level).

    Quick-fire: typically completes inside one poll, but Antithesis can
    pause the coordinator on the materialized side mid-INSERT, in
    which case the row count we computed locally may include rows the
    server-side transaction will later roll back.  Wait for parity
    before asking iceberg about it.
    """
    deadline = time.monotonic() + CATCHUP_TIMEOUT_S
    while time.monotonic() < deadline:
        try:
            row = query_one_retry(
                f"SELECT COUNT(*)::bigint FROM {helper_iceberg.SRC_TABLE} "
                f"WHERE batch_id = %s",
                (batch_id,),
            )
            count = int(row[0]) if row and row[0] is not None else 0
        except Exception as exc:  # noqa: BLE001
            LOG.info("mz visibility poll failed: %s; retrying", exc)
            time.sleep(POLL_INTERVAL_S)
            continue
        if count >= expected_count:
            return True
        time.sleep(POLL_INTERVAL_S)
    return False


def _wait_for_sink_progress(
    starting_committed: int | None, expected_count: int
) -> bool:
    """Poll `mz_sink_statistics.messages_committed` until it has advanced
    by at least `expected_count` past whatever it was at when this
    driver started inserting.

    `messages_committed` increments by the record count of every data
    file the sink commits.  For an UPSERT sink writing fresh keys (our
    case — every `id` is `<batch_id>:<i>`), that's ~1 record per row
    INSERTed, so requiring `current >= starting + expected_count`
    means the sink has committed at least as many records as we
    submitted since we started.

    Note that the counter is sink-wide: under parallel drivers it
    advances for every batch's writes, not just ours.  So the gate
    isn't a guarantee our specific batch landed — it's still a
    necessary-but-not-sufficient signal that the sink can keep up with
    the aggregate write rate.  Definitive per-batch verification still
    comes from the iceberg-side `batch_id` query.

    None as `starting_committed` means the stats row wasn't populated
    yet at driver entry; we fall back to the loose "any non-None
    progress" gate in that case so the driver still makes forward
    progress through the boundary condition.
    """
    deadline = time.monotonic() + CATCHUP_TIMEOUT_S
    target = (
        starting_committed + expected_count if starting_committed is not None else None
    )
    while time.monotonic() < deadline:
        try:
            current = helper_iceberg.messages_committed()
        except Exception as exc:  # noqa: BLE001
            LOG.info("messages_committed poll failed: %s; retrying", exc)
            time.sleep(POLL_INTERVAL_S)
            continue
        if current is None:
            time.sleep(POLL_INTERVAL_S)
            continue
        if target is not None:
            if current >= target:
                LOG.info(
                    "sink progress: messages_committed start=%d now=%d target=%d",
                    starting_committed,
                    current,
                    target,
                )
                return True
        else:
            # starting_committed was None at entry; any populated stats
            # row counts as progress (legacy fallback for the early-
            # lifecycle boundary).
            LOG.info(
                "sink progress: messages_committed start=None now=%d "
                "(stats row populated; skipping target gate)",
                current,
            )
            return True
        time.sleep(POLL_INTERVAL_S)
    LOG.warning(
        "sink progress did not reach target within %.0fs (start=%s target=%s)",
        CATCHUP_TIMEOUT_S,
        starting_committed,
        target,
    )
    return False


def main() -> int:
    if not _sink_exists():
        # first_iceberg_setup must run before this driver.  Outside
        # Antithesis (e.g. snouty validate) the sink may not exist yet —
        # exit cleanly rather than erroring so validate can still proceed.
        LOG.warning(
            "iceberg sink %s not found; skipping (first_iceberg_setup must run first)",
            helper_iceberg.SINK_NAME,
        )
        return 0

    batch_id = f"i{helper_random.random_u64():016x}"
    LOG.info("driver starting; batch_id=%s", batch_id)

    # Record the sink's pre-INSERT progress so we can wait for it to
    # advance past whatever it was at when we started.  None is fine —
    # _wait_for_sink_progress handles the "stats not yet populated"
    # case.
    try:
        starting_committed = helper_iceberg.messages_committed()
    except Exception:  # noqa: BLE001
        starting_committed = None

    expected = _insert_rows(batch_id)
    if not expected:
        LOG.info("no rows inserted successfully this invocation; exiting cleanly")
        return 0

    LOG.info("inserted %d rows; waiting for mz visibility", len(expected))
    if not _wait_for_mz_visible(batch_id, len(expected)):
        # If the rows aren't visible in MZ yet, the iceberg sink can't
        # have flushed them either — bail before we run iceberg-side
        # assertions on stale data.
        LOG.info("mz visibility did not complete in budget; skipping iceberg checks")
        return 0

    LOG.info("mz visibility OK; waiting for sink to advance")
    progressed = _wait_for_sink_progress(starting_committed, len(expected))
    sometimes(
        progressed,
        "iceberg: sink advanced messages_committed within catchup budget",
        {
            "sink": helper_iceberg.SINK_NAME,
            "batch_id": batch_id,
            "starting_committed": starting_committed,
        },
    )
    if not progressed:
        # No commit yet — give the iceberg side one more catchup wait
        # before bailing.  Even without an observed progress tick the
        # commit may have landed (stats can lag the actual commit by
        # the worker-stats-flush interval).
        LOG.info("sink progress not observed; continuing to iceberg verification anyway")

    # Count check: exactly the SS-148 target.  A double-commit shows up
    # here as `observed > expected`; a stall shows up as
    # `observed < expected`.  Both fail the testdrive expected-rows
    # block.
    count_check = helper_iceberg.verify_iceberg_count(
        batch_id, len(expected), timeout_s=CATCHUP_TIMEOUT_S
    )
    if count_check.looks_transient:
        # iceberg-side network failure (polaris/minio unreachable).
        # Demote to a `sometimes` so a future invocation gets to make
        # the call.
        sometimes(
            False,
            "iceberg: duckdb count check exited transiently",
            {
                "batch_id": batch_id,
                "expected": len(expected),
                "exit_code": count_check.exit_code,
            },
        )
        LOG.info("count check transient; skipping per-row assertions")
        return 0

    always(
        count_check.matched,
        "iceberg: sink row count matches inserted count after catchup",
        {
            "sink": helper_iceberg.SINK_NAME,
            "batch_id": batch_id,
            "expected_count": len(expected),
            "exit_code": count_check.exit_code,
            # testdrive writes the meat of an assertion failure
            # (`file:line: error: DuckDB query result mismatch ...
            # expected ... actual ...`) to STDOUT; only the `^^^ +++`
            # divider + `!!! Error Report` footer land on STDERR.  Stash
            # both tails so triage can see what failed without re-running.
            "stdout_tail": count_check.stdout[-1500:],
            "stderr_tail": count_check.stderr[-1500:],
        },
    )

    # Liveness anchor.  If the count check passed, at least one
    # invocation completed end-to-end this run.
    sometimes(
        count_check.matched,
        "iceberg: sink caught up to all upstream INSERTs within budget",
        {
            "sink": helper_iceberg.SINK_NAME,
            "batch_id": batch_id,
            "rows_inserted": len(expected),
        },
    )

    if not count_check.matched:
        # Counts already disagree; per-row check is redundant noise.
        LOG.info("count mismatch; skipping per-row check")
        return 0

    # Per-row safety.  Catches per-row value mismatches that a pure
    # count match would miss (e.g. a value got reordered into a
    # different snapshot).  Same shape as parallel_driver_pg_cdc.
    rows_check = helper_iceberg.verify_iceberg_rows_match(
        batch_id, expected, timeout_s=CATCHUP_TIMEOUT_S
    )
    if rows_check.looks_transient:
        sometimes(
            False,
            "iceberg: duckdb per-row check exited transiently",
            {"batch_id": batch_id, "exit_code": rows_check.exit_code},
        )
        return 0

    always(
        rows_check.matched,
        "iceberg: sink rows match inserted values after catchup",
        {
            "sink": helper_iceberg.SINK_NAME,
            "batch_id": batch_id,
            "rows_inserted": len(expected),
            "exit_code": rows_check.exit_code,
            "stdout_tail": rows_check.stdout[-1500:],
            "stderr_tail": rows_check.stderr[-1500:],
        },
    )

    LOG.info("driver done; asserted on %d rows for batch_id=%s", len(expected), batch_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
