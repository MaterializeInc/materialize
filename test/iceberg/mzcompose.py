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
from collections.abc import Callable

from materialize.mzcompose.composition import Composition, Service
from materialize.mzcompose.helpers.iceberg import (
    get_polaris_access_token,
    setup_polaris_for_iceberg,
)
from materialize.mzcompose.service import Service as ServiceDefinition
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
    ServiceDefinition(
        "polaris-proxy",
        {
            "image": "python:3.11-slim",
            "command": ["python", "-u", "polaris_proxy.py"],
            "working_dir": "/workdir",
            "volumes": [".:/workdir"],
            "ports": [8181],
            "environment": [
                "UPSTREAM_HOST=polaris",
                "UPSTREAM_PORT=8181",
                "PROXY_PORT=8181",
            ],
        },
    ),
    Materialized(
        depends_on=["minio"],
        sanity_restart=False,
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::sink::iceberg=debug",
        },
    ),
    Testdrive(),
    Mc(),
]


def _setup(
    c: Composition, vended: bool = False, static_credentials: bool = True
) -> str:
    """Start fresh and return the S3 access key."""
    c.down(destroy_volumes=True)
    c.up(
        "postgres",
        "materialized",
        Service("polaris-bootstrap", idle=True),
        Service("polaris", idle=True),
    )
    _, key = setup_polaris_for_iceberg(
        c, vended=vended, static_credentials=static_credentials
    )
    return key


def await_condition(what: str, timeout: float, check: Callable[[], bool]) -> None:
    """Poll `check` until it returns True, or raise after `timeout` seconds."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if check():
            return
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


def workflow_default(c: Composition) -> None:
    def process(name: str) -> None:
        if name == "default":
            return

        with c.test_case(name):
            c.workflow(name)

    c.test_parts(list(c.workflows.keys()), process)


def workflow_vended_credentials(c: Composition) -> None:
    """An Iceberg sink must work against a REST catalog that only hands out
    temporary, table-scoped credentials.

    The Polaris catalog is created with credential vending enabled and without
    the long-lived S3 credentials it would otherwise return to clients, so
    Materialize has no static credentials to fall back on. The sink can only
    reach MinIO with what Polaris mints for it in response to the
    `X-Iceberg-Access-Delegation: vended-credentials` request the Iceberg
    catalog connection sends."""
    key = _setup(c, vended=True, static_credentials=False)

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "vended-credentials.td",
    )


def workflow_smoke(c: Composition) -> None:
    key = _setup(c)

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "catalog.td",
        "nested-records.td",
        "key-validation.td",
    )


def workflow_gcp_connection_validation(c: Composition) -> None:
    """Regression test for SS-69: an Iceberg REST catalog connection that
    authenticates with a GCP connection must only target Google-operated catalog
    hosts. A GCP access token is a reusable, broadly-scoped bearer credential
    with no audience binding, so before this validation a principal with only
    USAGE on a GCP connection could point the catalog URL at an attacker host and
    exfiltrate the connection's service-account token. This exercises connection
    planning only and needs no Iceberg backend."""
    c.down(destroy_volumes=True)
    c.up("materialized")

    c.run_testdrive_files(
        "gcp-connection-validation.td",
    )


def workflow_oauth2_server_url(c: Composition) -> None:
    """OAUTH2 SERVER URL redirects a REST catalog connection's token exchange
    away from the endpoint the Iceberg specification derives from the catalog
    URL. Catalogs behind an auth gateway that will not serve an unauthenticated
    exchange, such as Databricks Unity Catalog, need it. This exercises
    connection planning only and needs no Iceberg backend."""
    c.down(destroy_volumes=True)
    c.up("materialized")

    c.run_testdrive_files(
        "oauth2-server-url.td",
    )


def workflow_mode_append(c: Composition) -> None:
    key = _setup(c)

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "mode-append.td",
    )


def workflow_idle_gap(c: Composition) -> None:
    """A caught-up Iceberg sink whose input goes quiet and then receives a
    write must stay healthy. Differential's arrange leaves a gap in the emitted
    batch stream across a data-free frontier advance, and `write_data_files`
    has to tolerate it rather than halting the dataflow."""
    key = _setup(c)

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "idle-gap.td",
    )


def workflow_alter_commit_interval(c: Composition) -> None:
    """ALTER SINK ... SET (COMMIT INTERVAL ...) restarts the sink dataflow
    with the new interval and subsequent batches follow the new cadence."""
    key = _setup(c)

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "alter-commit-interval.td",
    )


def workflow_empty_source(c: Composition) -> None:
    """A fresh Iceberg sink whose input closes after producing zero rows
    commits empty snapshots instead of stalling or erroring."""
    key = _setup(c)

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "empty-source.td",
    )


def workflow_finite_source(c: Composition) -> None:
    """A fresh Iceberg sink whose input contains data and then closes must
    commit all the data and then seal itself with a final empty-upper
    commit. Restarting Materialize afterwards must not re-commit or error."""
    key = _setup(c)

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "finite-source.td",
    )

    # The sink resumes from an Iceberg table whose committed frontier is
    # empty. It must come back healthy and idle, without committing
    # anything new.
    c.kill("materialized")
    c.up("materialized")

    c.run_testdrive_files(
        "--no-reset",
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "finite-source-verify.td",
    )


def workflow_alter_table_add_column(c: Composition) -> None:
    """A sink pointed at an Iceberg table whose schema is narrower than the
    sink's input relation must not panic the storage worker. The writer builds
    its Arrow column builders from the Iceberg table's schema, so a row with
    more datums than that schema has columns reaches `zip_eq` in
    `ArrowBuilder::add_row`."""
    key = _setup(c)

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "alter-table-add-column.td",
    )

    logs = c.invoke("logs", "materialized", capture=True)
    assert (
        "zip_eq" not in logs.stdout
    ), "storage worker panicked in ArrowBuilder::add_row"


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


def workflow_idempotent_retry(c: Composition) -> None:
    """Regression test: dropping a single catalog commit response must not
    fence the sink off or cause duplicate row commits."""
    key = _setup(c)
    c.invoke("up", "--detach", "--wait", "--no-recreate", "polaris-proxy")

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "idempotent-retry-setup.td",
    )

    proxy_base = f"http://localhost:{c.port('polaris-proxy', 8181)}"

    def proxy_post(path: str) -> None:
        with urllib.request.urlopen(
            urllib.request.Request(f"{proxy_base}{path}", data=b"", method="POST")
        ) as resp:
            resp.read()

    def proxy_status() -> dict[str, int]:
        with urllib.request.urlopen(f"{proxy_base}/__control/status") as resp:
            return json.loads(resp.read())

    # Arm the drop only once the sink has a commit through the proxy, so the
    # dropped commit is a steady-state one rather than table bootstrap.
    await_condition(
        "first sink commit",
        timeout=60,
        check=lambda: proxy_status()["commits_ok"] >= 1,
    )
    proxy_post("/__control/drop_next_commit")

    for i in range(10):
        c.sql(f"INSERT INTO retry_src VALUES ({i + 4}, 'row_{i + 4}')")
        time.sleep(1)

    await_condition(
        "dropped commit response",
        timeout=60,
        check=lambda: proxy_status()["commits_dropped"] >= 1,
    )

    def messages_committed() -> int:
        rows = c.sql_query(
            "SELECT COALESCE(SUM(messages_committed), 0) "
            "FROM mz_internal.mz_sink_statistics st "
            "JOIN mz_sinks s ON st.id = s.id "
            "WHERE s.name = 'retry_sink'"
        )
        return int(rows[0][0])

    # 3 initial rows + 10 inserted rows, all committed despite the dropped response.
    await_condition(
        "all 13 rows committed",
        timeout=120,
        check=lambda: messages_committed() >= 13,
    )

    status_rows = c.sql_query(
        "SELECT s.status, COALESCE(s.error, '') "
        "FROM mz_internal.mz_sink_statuses s "
        "JOIN mz_sinks ON s.id = mz_sinks.id "
        "WHERE mz_sinks.name = 'retry_sink'"
    )
    assert status_rows, "retry_sink not found in mz_sink_statuses"
    status, error = status_rows[0]
    assert status == "running", f"retry_sink is {status!r} (error={error!r})"

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "idempotent-retry-verify.td",
    )


def workflow_fenced_writer(c: Composition) -> None:
    """Regression test: once a snapshot with a newer mz-sink-version is on the
    table, the running sink must stop committing.

    Root cause: iceberg-rust's built-in commit path reloads the table and rebases
    onto the newest snapshot before every attempt, so the fenced-out sink never
    sees a conflict and commits right over the newer writer. Its snapshot then
    becomes the latest one, so even the startup fencing check of a later
    restart no longer notices the newer writer.

    So we can't use iceberg-rust's built-in commit path."""
    key = _setup(c)

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "fenced-writer-setup.td",
    )

    token = get_polaris_access_token(c)
    table_url = (
        f"http://localhost:{c.port('polaris', 8181)}"
        "/api/catalog/v1/default_catalog/namespaces/default_namespace/tables/fence_table"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def load_metadata() -> dict | None:
        req = urllib.request.Request(table_url, headers=headers)
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())["metadata"]
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise

    def first_commit_done() -> bool:
        meta = load_metadata()
        return meta is not None and meta.get("current-snapshot-id") not in (None, -1)

    await_condition("first sink commit", timeout=60, check=first_commit_done)

    def forge_newer_writer_snapshot() -> int:
        """Commit a snapshot that claims mz-sink-version 999, as a sink with a
        newer version would. Reuses the current snapshot's manifest list so the
        table stays readable without writing new files. Returns the forged
        snapshot's sequence number."""
        meta = load_metadata()
        assert meta is not None
        current_id = meta["refs"]["main"]["snapshot-id"]
        current = next(s for s in meta["snapshots"] if s["snapshot-id"] == current_id)
        forged_seq = meta["last-sequence-number"] + 1
        body = json.dumps(
            {
                "requirements": [
                    {
                        "type": "assert-ref-snapshot-id",
                        "ref": "main",
                        "snapshot-id": current_id,
                    }
                ],
                "updates": [
                    {
                        "action": "add-snapshot",
                        "snapshot": {
                            "snapshot-id": current_id + 1,
                            "parent-snapshot-id": current_id,
                            "sequence-number": forged_seq,
                            "timestamp-ms": int(time.time() * 1000),
                            "manifest-list": current["manifest-list"],
                            "schema-id": meta["current-schema-id"],
                            "summary": {
                                "operation": "append",
                                "mz-sink-id": "u0",
                                "mz-frontier": current["summary"]["mz-frontier"],
                                "mz-sink-version": "999",
                            },
                        },
                    },
                    {
                        "action": "set-snapshot-ref",
                        "ref-name": "main",
                        "type": "branch",
                        "snapshot-id": current_id + 1,
                    },
                ],
            }
        ).encode()
        req = urllib.request.Request(
            table_url, data=body, headers=headers, method="POST"
        )
        with urllib.request.urlopen(req) as resp:
            resp.read()
        return forged_seq

    # The sink may commit between reading the metadata and posting the forged
    # snapshot, failing our requirement. Retry on conflict with fresh metadata.
    for attempt in range(5):
        try:
            forged_seq = forge_newer_writer_snapshot()
            break
        except urllib.error.HTTPError as e:
            if e.code != 409 or attempt == 4:
                raise
    else:
        raise AssertionError("unreachable")

    # Give the fenced sink something to commit.
    for i in range(5):
        c.sql(f"INSERT INTO fence_src VALUES ({i + 4}, 'row_{i + 4}')")
        time.sleep(1)

    deadline = time.time() + 90
    while True:
        meta = load_metadata()
        assert meta is not None
        for snapshot in meta["snapshots"]:
            if (
                snapshot["sequence-number"] > forged_seq
                and snapshot["summary"].get("mz-sink-version") != "999"
            ):
                raise AssertionError(
                    "fenced sink committed past the newer writer: "
                    f"snapshot {snapshot['snapshot-id']} "
                    f"summary {snapshot['summary']}"
                )

        status_rows = c.sql_query(
            "SELECT s.status, COALESCE(s.error, '') "
            "FROM mz_internal.mz_sink_statuses s "
            "JOIN mz_sinks ON s.id = mz_sinks.id "
            "WHERE mz_sinks.name = 'fence_sink'"
        )
        assert status_rows, "fence_sink not found in mz_sink_statuses"
        status, error = status_rows[0]
        if status != "running" and ("another writer" in error or "Fenced off" in error):
            return
        if time.time() > deadline:
            raise AssertionError(f"sink not fenced: status={status!r} error={error!r}")
        time.sleep(1)


def workflow_prefix_writer(c: Composition) -> None:
    """Regression test: a snapshot whose frontier falls strictly inside the
    sink's pending batch must stop the sink from committing that batch.

    How this could happen in practice:

    Suppose we have a sink with commit interval 30s.
    Then we alter the sink's commit interval to 1s.

    Let the table's frontier be X.
    The new version of the sink can commit a batch [X, X + 1s).
    The old version of the sink will try to commit a batch [X, X + 30s).

    Committing both batches would result in duplicate writes."""
    key = _setup(c)

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "prefix-writer-setup.td",
    )

    token = get_polaris_access_token(c)
    table_url = (
        f"http://localhost:{c.port('polaris', 8181)}"
        "/api/catalog/v1/default_catalog/namespaces/default_namespace/tables/prefix_table"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def load_metadata() -> dict | None:
        req = urllib.request.Request(table_url, headers=headers)
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())["metadata"]
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise

    def load_current() -> tuple[dict, dict] | None:
        """Table metadata plus its current snapshot, once the sink has committed."""
        meta = load_metadata()
        if meta is None:
            return None
        snapshot_id = meta.get("current-snapshot-id")
        if snapshot_id in (None, -1):
            return None
        snapshot = next(s for s in meta["snapshots"] if s["snapshot-id"] == snapshot_id)
        return meta, snapshot

    def frontier_of(snapshot: dict) -> int:
        elements = json.loads(snapshot["summary"]["mz-frontier"])
        assert len(elements) == 1, f"expected a single frontier element: {elements}"
        return elements[0]

    def committed_frontier() -> int | None:
        current = load_current()
        return None if current is None else frontier_of(current[1])

    await_condition(
        "first sink commit", timeout=60, check=lambda: committed_frontier() is not None
    )
    first_frontier = committed_frontier()
    assert first_frontier is not None

    # The first commit covers the source snapshot and ends just past the as_of.
    # The catch-up batch that follows it can be just as narrow, so wait for that
    # one to commit too. Only then is the pending batch a full commit interval
    # wide, which is what makes room for a frontier strictly inside it.
    def catchup_committed() -> bool:
        frontier = committed_frontier()
        return frontier is not None and frontier > first_frontier

    await_condition("catch-up commit", timeout=120, check=catchup_committed)

    def forge_prefix_snapshot() -> tuple[int, int]:
        """Commit a snapshot claiming the running sink's own ID and version, one
        second past its last committed frontier: what a second incarnation with
        a shorter commit interval leaves behind. Reuses the current snapshot's
        manifest list so the table stays readable without writing new files.
        Returns the forged sequence number and frontier."""
        current = load_current()
        assert current is not None
        meta, snapshot = current
        current_id = snapshot["snapshot-id"]
        forged_seq = meta["last-sequence-number"] + 1
        # Frontiers are milliseconds, so one second past the last commit.
        forged_frontier = frontier_of(snapshot) + 1000
        body = json.dumps(
            {
                "requirements": [
                    {
                        "type": "assert-ref-snapshot-id",
                        "ref": "main",
                        "snapshot-id": current_id,
                    }
                ],
                "updates": [
                    {
                        "action": "add-snapshot",
                        "snapshot": {
                            "snapshot-id": current_id + 1,
                            "parent-snapshot-id": current_id,
                            "sequence-number": forged_seq,
                            "timestamp-ms": int(time.time() * 1000),
                            "manifest-list": snapshot["manifest-list"],
                            "schema-id": meta["current-schema-id"],
                            "summary": {
                                "operation": "append",
                                "mz-sink-id": snapshot["summary"]["mz-sink-id"],
                                "mz-frontier": json.dumps([forged_frontier]),
                                "mz-sink-version": snapshot["summary"][
                                    "mz-sink-version"
                                ],
                            },
                        },
                    },
                    {
                        "action": "set-snapshot-ref",
                        "ref-name": "main",
                        "type": "branch",
                        "snapshot-id": current_id + 1,
                    },
                ],
            }
        ).encode()
        req = urllib.request.Request(
            table_url, data=body, headers=headers, method="POST"
        )
        with urllib.request.urlopen(req) as resp:
            resp.read()
        return forged_seq, forged_frontier

    # The sink may commit between reading the metadata and posting the forged
    # snapshot, failing our requirement. Retry on conflict with fresh metadata.
    for attempt in range(5):
        try:
            forged_seq, forged_frontier = forge_prefix_snapshot()
            break
        except urllib.error.HTTPError as e:
            if e.code != 409 or attempt == 4:
                raise
    else:
        raise AssertionError("unreachable")

    # Put a row in the range the forged snapshot claims to have committed, so the
    # overlap the sink must refuse is not empty.
    c.sql("INSERT INTO prefix_src VALUES (4, 'd')")

    deadline = time.time() + 120
    while True:
        meta = load_metadata()
        assert meta is not None
        for snapshot in meta["snapshots"]:
            if snapshot["sequence-number"] > forged_seq:
                raise AssertionError(
                    "sink committed a batch overlapping the forged writer's "
                    f"range: forged frontier {forged_frontier}, batch started at "
                    f"{forged_frontier - 1000}, snapshot {snapshot['snapshot-id']} "
                    f"summary {snapshot['summary']}"
                )

        status_rows = c.sql_query(
            "SELECT s.status, COALESCE(s.error, '') "
            "FROM mz_internal.mz_sink_statuses s "
            "JOIN mz_sinks ON s.id = mz_sinks.id "
            "WHERE mz_sinks.name = 'prefix_sink'"
        )
        assert status_rows, "prefix_sink not found in mz_sink_statuses"
        status, error = status_rows[0]
        if status != "running" and ("another writer" in error or "Fenced off" in error):
            return
        if time.time() > deadline:
            raise AssertionError(f"sink not fenced: status={status!r} error={error!r}")
        time.sleep(1)


def workflow_large_upsert_batch(c: Composition) -> None:
    """Regression test for database-issues#11326: DeltaWriter seen_rows
    eviction caused equality deletes within the same snapshot, which
    have no effect — leaving duplicate rows in the committed table."""
    key = _setup(c)

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "large-upsert-batch.td",
    )


def workflow_range_noncanonical(c: Composition) -> None:
    """Regression test for database-issues#11330: COPY FROM PARQUET must
    canonicalize range values reconstructed from external Parquet, otherwise
    non-canonical encodings written by other engines land verbatim in MZ rows
    and break equality against values constructed inside MZ. DuckDB authors the
    Parquet file directly in minio so the bytes are not something MZ's sink
    produced."""
    key = _setup(c)

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "range-noncanonical.td",
    )
