# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import json
import threading
import time
import urllib.error
import urllib.request

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.helpers.iceberg import (
    setup_polaris_for_iceberg,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.polaris import Polaris, PolarisBootstrap
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Mz(app_password=""),
    Postgres(),
    Minio(),
    PolarisBootstrap(),
    Polaris(),
    Materialized(
        depends_on=["minio"],
        sanity_restart=False,
        system_parameter_defaults={"enable_iceberg_sink": "true"},
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::sink::iceberg=debug",
        },
    ),
    Testdrive(),
    Mc(),
]


def _setup(c: Composition) -> str:
    """Start fresh and return the S3 access key."""
    c.down(destroy_volumes=True)
    c.up("postgres", "materialized")
    _, key = setup_polaris_for_iceberg(c)
    return key


def workflow_default(c: Composition) -> None:
    def process(name: str) -> None:
        if name == "default":
            return

        with c.test_case(name):
            c.workflow(name)

    c.test_parts(list(c.workflows.keys()), process)


def workflow_smoke(c: Composition) -> None:
    key = _setup(c)

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "catalog.td",
        "nested-records.td",
        "key-validation.td",
    )


def workflow_mode_append(c: Composition) -> None:
    key = _setup(c)

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "mode-append.td",
    )


def _polaris_get(table_url: str, access_token: str) -> dict:
    """GET table metadata from Polaris REST API (always returns latest)."""
    req = urllib.request.Request(
        table_url,
        headers={"Authorization": f"Bearer {access_token}"},
    )
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read())


def workflow_commit_conflict(c: Composition) -> None:
    """Verify no data loss when catalog commit conflicts occur.

    When a CatalogCommitConflicts error occurs during an Iceberg commit,
    the sink must retry the commit so that no data is lost.

    Strategy: Run a background thread that modifies the Polaris table
    metadata (adding dummy snapshots) to race with the sink's commits.
    When a modification lands between the sink's table refresh and its
    commit POST, the sink gets a CatalogCommitConflicts error.

    Verification uses DuckDB's iceberg_scan to count the rows in the
    Iceberg table. All inserted rows must be present.
    """
    key = _setup(c)

    # Phase 1: Create sink with short commit interval (2s) and initial data
    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "commit-conflict-setup.td",
    )

    # Phase 2: Wait for initial snapshot batch to commit
    print("Waiting 10s for initial snapshot batch to commit...")
    time.sleep(10)

    # Phase 3: Set up direct HTTP access to Polaris from host
    polaris_port = c.port("polaris", 8181)
    base_url = f"http://localhost:{polaris_port}"
    table_url = (
        f"{base_url}/api/catalog/v1/default_catalog"
        f"/namespaces/default_namespace/tables/conflict_table"
    )

    # Get access token via direct HTTP
    token_req = urllib.request.Request(
        f"{base_url}/api/catalog/v1/oauth/tokens",
        data=b"grant_type=client_credentials&client_id=root&client_secret=root&scope=PRINCIPAL_ROLE:ALL",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    token_resp = urllib.request.urlopen(token_req)
    access_token = json.loads(token_resp.read())["access_token"]
    print(f"Got Polaris access token (len={len(access_token)})")

    # Phase 4: Start background modification loop (~100/sec).
    # This rate triggers CatalogCommitConflicts on ~50% of sink commits.
    stop_event = threading.Event()
    stats = {"modifications": 0, "self_conflicts": 0, "errors": 0}

    def modify_table_loop() -> None:
        """Add dummy snapshots to race with the sink's commits."""
        while not stop_event.is_set():
            try:
                data = _polaris_get(table_url, access_token)
                metadata = data["metadata"]
                snap_id = metadata["current-snapshot-id"]

                snap = None
                for s in metadata.get("snapshots", []):
                    if s["snapshot-id"] == snap_id:
                        snap = s
                        break
                if snap is None:
                    continue

                # Dummy snapshot: same manifest-list and summary (including
                # mz-frontier) so the sink's fencing check passes.
                dummy_id = snap_id + 10_000_000 + stats["modifications"]
                dummy = {
                    "snapshot-id": dummy_id,
                    "parent-snapshot-id": snap_id,
                    "timestamp-ms": int(time.time() * 1000),
                    "sequence-number": snap.get("sequence-number", 0) + 1,
                    "summary": snap["summary"],
                    "manifest-list": snap["manifest-list"],
                    "schema-id": snap.get("schema-id", 0),
                }

                payload = json.dumps(
                    {
                        "requirements": [
                            {
                                "type": "assert-ref-snapshot-id",
                                "ref": "main",
                                "snapshot-id": snap_id,
                            }
                        ],
                        "updates": [
                            {"action": "add-snapshot", "snapshot": dummy},
                            {
                                "action": "set-snapshot-ref",
                                "ref-name": "main",
                                "type": "branch",
                                "snapshot-id": dummy_id,
                            },
                        ],
                    }
                )

                post_req = urllib.request.Request(
                    table_url,
                    data=payload.encode(),
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json",
                    },
                    method="POST",
                )
                try:
                    urllib.request.urlopen(post_req)
                    stats["modifications"] += 1
                except urllib.error.HTTPError as e:
                    if e.code == 409:
                        stats["self_conflicts"] += 1
                    else:
                        stats["errors"] += 1
            except Exception:
                stats["errors"] += 1
            time.sleep(0.01)

    thread = threading.Thread(target=modify_table_loop, daemon=True)
    thread.start()

    # Phase 5: Insert data continuously (1 row per second for 40 seconds).
    # With COMMIT INTERVAL '2s', this creates ~20 batches of ~2 rows each.
    num_extra_rows = 40
    for i in range(num_extra_rows):
        c.sql(f"INSERT INTO conflict_src VALUES ({i + 4}, 'row_{i + 4}')")
        if (i + 1) % 10 == 0:
            print(
                f"Inserted {i + 1}/{num_extra_rows} rows | "
                f"mods={stats['modifications']} "
                f"self_conflicts={stats['self_conflicts']} "
                f"errors={stats['errors']}"
            )
        time.sleep(1)

    # Phase 6: Stop modification loop and wait for final commits
    stop_event.set()
    thread.join(timeout=10)
    print(
        f"Modification loop finished: {stats['modifications']} successful mods, "
        f"{stats['self_conflicts']} self-conflicts, {stats['errors']} errors"
    )

    # Wait long enough for any remaining batches to commit. No more
    # modifications are racing, so any batch the sink still has pending
    # will commit within a few COMMIT INTERVAL cycles. The long wait
    # (60s) makes it clear that any missing records are permanently
    # lost — the persist frontier has advanced past them.
    print("Waiting 60s for any remaining batches to commit...")
    time.sleep(60)

    # Phase 7: Verify all 43 rows are present via DuckDB's iceberg_scan.
    total_expected = 3 + num_extra_rows  # 43
    print(f"Verifying all {total_expected} rows are present via DuckDB...")
    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "commit-conflict-verify.td",
    )
