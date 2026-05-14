# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import os
import subprocess
import time
from collections.abc import Iterable
from textwrap import dedent

from materialize.antithesis.sdk import (
    always,
    random_choice,
    random_int,
    reachable,
    sometimes,
    stop_faults,
)
from materialize.antithesis.testdrive_config import TestdriveConfig

TOPIC = "antithesis-upsert"
SOURCE = "antithesis_upsert"
SOURCE_TABLE = "antithesis_upsert_tbl"
EXPECTED_TABLE = "antithesis_upsert_expected"
CLUSTER = "antithesis_upsert_cluster"
KAFKA_CONNECTION = "antithesis_upsert_kafka"


def run_testdrive(
    script: str,
    *,
    config: TestdriveConfig | None = None,
    no_reset: bool = True,
    source: str,
    allow_failure: bool = False,
) -> bool:
    config = config or TestdriveConfig.from_env()
    cmd = config.base_command(no_reset=no_reset, source=source)
    try:
        subprocess.run(
            cmd,
            input=dedent(script).lstrip(),
            text=True,
            check=True,
        )
        return True
    except subprocess.CalledProcessError as e:
        if not allow_failure:
            raise
        reachable(
            "upsert source command saw tolerated testdrive failure",
            {"source": source, "returncode": e.returncode},
        )
        return False


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_string_list(values: Iterable[str]) -> str:
    return ", ".join(_sql_string(value) for value in values)


def _topic_name() -> str:
    return f"testdrive-{TOPIC}-${{testdrive.seed}}"


def setup_text_upsert_source() -> None:
    """Create a Kafka text/text upsert source and an expected-state table."""

    cluster_size = os.environ.get("MZ_ANTITHESIS_UPSERT_CLUSTER_SIZE")
    cluster_ddl = (
        f"> CREATE CLUSTER {CLUSTER} SIZE {_sql_string(cluster_size)};"
        if cluster_size
        else ""
    )
    cluster_clause = f"IN CLUSTER {CLUSTER}" if cluster_size else ""
    run_testdrive(
        f"""
        $ set-sql-timeout duration=60s

        $ kafka-create-topic topic={TOPIC} partitions=1

        > CREATE CONNECTION IF NOT EXISTS {KAFKA_CONNECTION}
          TO KAFKA (BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT);

        > DROP TABLE IF EXISTS {EXPECTED_TABLE};
        > DROP SOURCE IF EXISTS {SOURCE} CASCADE;
        > DROP CLUSTER IF EXISTS {CLUSTER} CASCADE;

        {cluster_ddl}

        > BEGIN
        > CREATE SOURCE {SOURCE}
          {cluster_clause}
          FROM KAFKA CONNECTION {KAFKA_CONNECTION} (TOPIC '{_topic_name()}');

        > CREATE TABLE {SOURCE_TABLE} FROM SOURCE {SOURCE} (REFERENCE "{_topic_name()}")
          KEY FORMAT TEXT
          VALUE FORMAT TEXT
          ENVELOPE UPSERT;
        > COMMIT

        > CREATE TABLE {EXPECTED_TABLE} (key text, value text);

        > SELECT count(*) FROM {SOURCE_TABLE};
        0
        """,
        no_reset=False,
        source="antithesis/upsert_sources/first_create_upsert_source",
    )
    reachable("upsert source setup completed", {"topic": TOPIC})


def write_random_upserts(*, allow_failure: bool) -> None:
    batch_size = int(os.environ.get("MZ_ANTITHESIS_UPSERT_BATCH_SIZE", "8"))
    batch_id = f"{int(time.time() * 1000)}_{random_int(1_000_000)}"

    latest: dict[str, str | None] = {}
    kafka_lines: list[str] = []
    key_space = int(os.environ.get("MZ_ANTITHESIS_UPSERT_KEY_SPACE", "64"))
    attempted_upserts = 0
    attempted_deletes = 0

    for i in range(batch_size):
        key = f"k{random_int(key_space):03d}"
        if random_choice([False, False, False, True]):
            attempted_deletes += 1
            latest[key] = None
            kafka_lines.append(f"{key}:")
        else:
            attempted_upserts += 1
            value = f"v_{batch_id}_{i}_{random_int(1_000_000)}"
            latest[key] = value
            kafka_lines.append(f"{key}:{value}")

    upserts = [(key, value) for key, value in latest.items() if value is not None]
    sql_delete_keys = list(latest)
    expected_update = ""
    if sql_delete_keys:
        expected_update += (
            f"> DELETE FROM {EXPECTED_TABLE} "
            f"WHERE key IN ({_sql_string_list(sql_delete_keys)});\n"
        )
    if upserts:
        values = ", ".join(
            f"({_sql_string(key)}, {_sql_string(value)})" for key, value in upserts
        )
        expected_update += f"> INSERT INTO {EXPECTED_TABLE} VALUES {values};\n"

    succeeded = run_testdrive(
        f"""
        $ set-sql-timeout duration=60s

        $ kafka-ingest format=bytes topic={TOPIC} key-format=bytes key-terminator=:
        {chr(10).join(kafka_lines)}

        {expected_update}
        """,
        no_reset=True,
        source="antithesis/upsert_sources/parallel_driver_write_upserts",
        allow_failure=allow_failure,
    )
    sometimes(
        succeeded,
        "upsert source write driver completed at least once",
        {
            "batch_size": batch_size,
            "attempted_upserts": attempted_upserts,
            "attempted_deletes": attempted_deletes,
            "latest_upserts": len(upserts),
        },
    )


def read_stale_safe(*, allow_failure: bool) -> None:
    succeeded = run_testdrive(
        f"""
        $ set-sql-timeout duration=30s

        > SELECT count(*) >= 0 FROM {SOURCE_TABLE};
        true

        > SELECT count(*) >= 0 FROM {EXPECTED_TABLE};
        true
        """,
        no_reset=True,
        source="antithesis/upsert_sources/parallel_driver_read_stale_safe",
        allow_failure=allow_failure,
    )
    sometimes(
        succeeded,
        "upsert source stale-safe read completed at least once",
        {"source_table": SOURCE_TABLE},
    )


def health_check(*, allow_failure: bool) -> None:
    succeeded = run_testdrive(
        f"""
        $ set-sql-timeout duration=30s

        > SELECT status IN ('running', 'starting', 'stalled')
          FROM mz_internal.mz_source_statuses
          WHERE name = '{SOURCE_TABLE}';
        true
        """,
        no_reset=True,
        source="antithesis/upsert_sources/anytime_upsert_source_health",
        allow_failure=allow_failure,
    )
    sometimes(
        succeeded,
        "upsert source health check completed at least once",
        {"source_table": SOURCE_TABLE},
    )


def verify_expected_rows_visible(*, source: str) -> None:
    run_testdrive(
        f"""
        $ set-sql-timeout duration=120s

        > SELECT e.key, e.value, u.text
          FROM {EXPECTED_TABLE} e
          LEFT JOIN {SOURCE_TABLE} u ON e.key = u.key
          WHERE u.key IS NULL OR u.text <> e.value;
        """,
        no_reset=True,
        source=source,
    )
    reachable("upsert source expected rows verified", {"source": source})


def write_sentinel_and_verify() -> None:
    stop_faults(int(os.environ.get("MZ_ANTITHESIS_QUIET_SECONDS", "20")))
    key = f"sentinel_{int(time.time() * 1000)}_{random_int(1_000_000)}"
    value = f"sentinel_value_{random_int(1_000_000)}"
    run_testdrive(
        f"""
        $ set-sql-timeout duration=120s

        $ kafka-ingest format=bytes topic={TOPIC} key-format=bytes key-terminator=:
        {key}:{value}

        > DELETE FROM {EXPECTED_TABLE} WHERE key = {_sql_string(key)};
        > INSERT INTO {EXPECTED_TABLE} VALUES ({_sql_string(key)}, {_sql_string(value)});

        > SELECT text FROM {SOURCE_TABLE} WHERE key = {_sql_string(key)};
        {value}
        """,
        no_reset=True,
        source="antithesis/upsert_sources/eventually_upsert_source_catches_up",
    )
    verify_expected_rows_visible(
        source="antithesis/upsert_sources/eventually_upsert_source_catches_up"
    )
    always(True, "upsert source catches up after quiet period", {"key": key})
