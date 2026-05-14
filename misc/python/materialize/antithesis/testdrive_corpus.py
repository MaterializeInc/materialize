# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Curated lists of `test/testdrive/*.td` files driven by the Antithesis
Test Composer area templates.

Each list is the corpus a single area template picks from at random — see
`antithesis/test/v1/testdrive_<area>/singleton_driver_random_<area>`. The
template uses the Antithesis SDK's `random_choice` so the platform's
randomness, replay, and search interact with the selection.

Why area lists rather than one flat list:

  * Antithesis groups assertions and triage signal by template; per-area
    buckets give us "Kafka-side regressions vs SQL-side regressions vs
    storage-recovery regressions" attribution at the platform level.
  * The `source` field passed to testdrive captures the specific file inside
    each timeline, so per-.td detail still reaches triage when needed.
  * Adding a new file is a one-line change here. No new wrapper script.

Why curated rather than auto-discover everything:

  * `test/testdrive` has ~278 files; ~257 are technically base-compatible
    (no external services), but several rely on RESET semantics, depend on
    specific runtime knobs, or are intentionally Nightly-only. Curating
    keeps the harness's signal-to-noise ratio high during early runs and
    lets us expand once we trust each batch.

Adding a file:

  1. Skim the .td; confirm it works against `materialized + redpanda`
     without external services and finishes in ~tens of seconds with
     `--no-reset`.
  2. Append it to the relevant `BASE_*` list below.
  3. Rebuild the test-driver image so the new file lands at
     `/opt/materialize/td/testdrive/<name>.td`.

The same pattern extends to scenarios with extra services — those will
grow new constants here (e.g. `PG_CDC`, `MYSQL_CDC`) when their scenarios
land.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Files we deliberately exclude from every list.
# ---------------------------------------------------------------------------

# Mirrors the `--slow`-gated skip list inside test/testdrive/mzcompose.py
# plus a few that are known to be flaky or to require non-default
# configuration. Anything in this set should never appear in a BASE_* list.
EXCLUDED: frozenset[str] = frozenset(
    [
        "explain-pushdown.td",  # nightly-only
        "fivetran-destination.td",  # needs fivetran service
        "kafka-upsert-sources.td",  # very long; covered by upsert_sources prototype
        "materialized-view-refresh-options.td",  # nightly-only
        "upsert-source-race.td",  # known flaky
    ]
)


# ---------------------------------------------------------------------------
# `base` scenario corpus — runs against materialized + redpanda only.
#
# Each list targets a single Antithesis Test Composer area template. Files
# may appear in more than one list (e.g. a recovery-flavored Kafka file in
# both BASE_KAFKA and BASE_RECOVERY) — Antithesis picks per area, so the
# overlap just means more chances to land on a high-value file.
# ---------------------------------------------------------------------------

# Pure SQL: tables, DDL, joins, transactions, system tables, EXPLAIN.
# Excludes anything that creates a Kafka/load-generator source.
BASE_SQL: list[str] = [
    "array.td",
    "char-varchar-multibyte.td",
    "concurrent-ddl.td",
    "constants.td",
    "decimal-overflow.td",
    "drop.td",
    "empty-array.td",
    "explain-timestamps.td",
    "fetch-tail-retraction.td",
    "fetch-timeout.td",
    "functions.td",
    "insert-select.td",
    "joins.td",
    "jsonb.td",
    "linear-join-fuel.td",
    "numeric.td",
    "oid.td",
    "runtime-errors.td",
    "search_path.td",
    "show_columns.td",
    "system-functions.td",
    "type_char_quoted.td",
]

# Kafka source/sink + Schema Registry. Covers UPSERT/NONE/DEBEZIUM
# envelopes, Avro/JSON/Protobuf formats, multi-partition sink behavior,
# exactly-once sinks, and the Avro resolution matrix.
BASE_KAFKA: list[str] = [
    "avro-decode-bad-json.td",
    "avro-decode-multi-record.td",
    "avro-decode-no-record.td",
    "avro-decode-uuid.td",
    "avro-resolution-less-columns.td",
    "avro-resolution-more-columns.td",
    "avro-resolution-no-publish-reader.td",
    "avro-resolution-no-publish-writer.td",
    "avro-resolution-types-array.td",
    "avro-resolution-types-map.td",
    "avro-resolution-types-records.td",
    "avro-resolution-types.td",
    "avro-resolution-union-concrete.td",
    "avro-resolution-union-reader.td",
    "avro-resolution-union-writer.td",
    "avro-resolution-unions.td",
    "kafka-alter-source.td",
    "kafka-avro-sinks-defaults.td",
    "kafka-avro-sinks.td",
    "kafka-exactly-once-sink.td",
    "kafka-json-sinks.td",
    "kafka-progress.td",
    "kafka-recreate-topic.td",
    "kafka-sink-empty-progress-topic.td",
    "kafka-sink-headers.td",
    "kafka-sink-multi-partition.td",
    "kafka-sink-statistics.td",
    "kafka-start-offset.td",
    "materialized-views.td",
    "mz-kafka-sources.td",
    "mz-sinks.td",
    "protobuf-map.td",
    "protobuf-recursive.td",
    "protobuf-required.td",
    "upsert-unordered-key.td",
]

# LOAD GENERATOR-only files: COUNTER, AUCTION, CLOCK, DATUMS sources.
# Exercises the storage ingest control plane independently of Kafka.
BASE_LOAD_GENERATOR: list[str] = [
    "compute-logical-backpressure.td",
    "dataflow-cleanup.td",
    "get-started.td",
    "github-6942.td",
    "index-advice.td",
    "index-source-stuck.td",
    "load-generator.td",
    "mz-support-privileges.td",
    "partition-by.td",
    "quickstart.td",
    "show-subsources.td",
    "source-statistics-view.td",
]

# Files chosen specifically because their property is "the system survives
# and recovers from a perturbation." High signal under Antithesis fault
# injection. Many of these intentionally overlap with BASE_KAFKA /
# BASE_LOAD_GENERATOR — the area-template split is for triage attribution,
# not exclusivity.
BASE_RECOVERY: list[str] = [
    "dataflow-cleanup.td",
    "hydration-status.td",
    "kafka-progress.td",
    "kafka-recreate-topic.td",
    "kafka-sink-empty-progress-topic.td",
    "materialized-views.td",
    "sequential-hydration.td",
    "status-history.td",
]


# ---------------------------------------------------------------------------
# Self-checks: run at import time so a typo in a list above fails fast
# locally rather than waiting for a timeline to actually try the file.
# ---------------------------------------------------------------------------


def _validate() -> None:
    overlap_with_excluded = (
        set(BASE_SQL + BASE_KAFKA + BASE_LOAD_GENERATOR + BASE_RECOVERY) & EXCLUDED
    )
    if overlap_with_excluded:
        raise ValueError(
            f"BASE_* lists contain EXCLUDED entries: {sorted(overlap_with_excluded)}"
        )

    for name, lst in (
        ("BASE_SQL", BASE_SQL),
        ("BASE_KAFKA", BASE_KAFKA),
        ("BASE_LOAD_GENERATOR", BASE_LOAD_GENERATOR),
        ("BASE_RECOVERY", BASE_RECOVERY),
    ):
        if not lst:
            raise ValueError(f"{name} is empty")
        seen: set[str] = set()
        for entry in lst:
            if not entry.endswith(".td"):
                raise ValueError(f"{name}: non-.td entry {entry!r}")
            if entry in seen:
                raise ValueError(f"{name}: duplicate entry {entry!r}")
            seen.add(entry)


_validate()
