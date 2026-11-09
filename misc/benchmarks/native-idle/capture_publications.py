# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Post-measurement catalog publication diagnostics, not an idle CPU window."""

import gzip
import json
import time

from psycopg import sql


def capture_publications(run, conn, endpoints, curl, seconds):
    def save(name, value):
        (run / name).write_text(json.dumps(value, indent=2, default=str) + "\n")

    def metrics(label):
        timing = {}
        for role, record in endpoints.items():
            path = run / f"publication-{label}-{role}.prom"
            start = time.time()
            curl(record, "/metrics", path)
            timing[role] = {"start": start, "end": time.time()}
            with gzip.open(str(path) + ".gz", "wb") as output:
                output.write(path.read_bytes())
            path.unlink()
        save(f"publication-{label}-timing.json", timing)

    previous = conn.execute("SHOW log_filter").fetchone()[0]
    report = {
        "status": "running",
        "seconds": seconds,
        "previous_log_filter": previous,
        "scope": "Post-window diagnostic. DEBUG prefix logs are observations, not independent publications.",
    }
    save("publication-result.json", report)
    try:
        conn.execute(
            sql.SQL("ALTER SYSTEM SET log_filter = {}").format(
                sql.Literal(previous + ",mz_adapter::frontend_read_then_write=debug")
            )
        )
        # Exclude admission/configuration effects from enabling the diagnostic.
        time.sleep(15)
        metrics("start")
        report["start_wall"] = time.time()
        started = time.monotonic()
        time.sleep(seconds)
        report["elapsed"] = time.monotonic() - started
        report["end_wall"] = time.time()
        metrics("end")
        report["measurement_status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=repr(error))
        raise
    finally:
        conn.execute(
            sql.SQL("ALTER SYSTEM SET log_filter = {}").format(sql.Literal(previous))
        )
        save("publication-result.json", report)

    try:
        capture_inventory(run, conn)
    except BaseException as error:
        report.update(status="failed", error=repr(error))
        save("publication-result.json", report)
        raise
    report["status"] = "passed"
    save("publication-result.json", report)


def capture_inventory(run, conn):
    """Capture current ownership inventories, never publication rates."""

    def save(name, value):
        (run / name).write_text(json.dumps(value, indent=2, default=str) + "\n")

    def query(name, statement):
        cursor = conn.execute(statement)
        save(
            name,
            {
                "columns": [column.name for column in cursor.description],
                "rows": cursor.fetchall(),
            },
        )

    conn.execute("SET cluster = mz_catalog_server")
    # These inventories explain ownership after the timed diagnostic. They are
    # not update rates and must not be counted as journal publications.
    query(
        "publication-requirements.json",
        "SELECT data FROM mz_internal.mz_catalog_raw "
        "WHERE data->>'kind' IN ('ClientReadRequirement', "
        "'MaintainedReadRequirement', 'CollectionCompactionBound', 'ClientIncarnation')",
    )
    query(
        "publication-object-identities.json",
        "SELECT g.global_id, o.id, o.type, s.name AS schema, o.name "
        "FROM mz_internal.mz_object_global_ids g "
        "JOIN mz_catalog.mz_objects o ON o.id=g.id "
        "JOIN mz_catalog.mz_schemas s ON s.id=o.schema_id",
    )
    relations = conn.execute(
        "SELECT s.name, o.name FROM mz_catalog.mz_objects o "
        "JOIN mz_catalog.mz_schemas s ON s.id=o.schema_id "
        "WHERE (s.name='mz_internal' AND o.name IN "
        "('mz_replica_frontier_observations', 'mz_storage_frontier_observations', "
        "'mz_frontiers')) OR "
        "(s.name='mz_catalog' AND o.name='mz_cluster_replica_frontiers')"
    ).fetchall()
    for schema, name in relations:
        query(
            f"publication-{name}.json",
            sql.SQL("SELECT * FROM {}.{}").format(
                sql.Identifier(schema), sql.Identifier(name)
            ),
        )
