# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Native Postgres source tests, functional.
"""

import glob
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from textwrap import dedent
from typing import Any

import psycopg
from psycopg import Connection

from materialize import MZ_ROOT, buildkite
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import Service as MzComposeService
from materialize.mzcompose.service import ServiceConfig
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy

# Set the max slot WAL keep size to 10MB
DEFAULT_PG_EXTRA_COMMAND = ["-c", "max_slot_wal_keep_size=10"]


class PostgresRecvlogical(MzComposeService):
    """
    Command to start a replication.
    """

    def __init__(self, replication_slot_name: str, publication_name: str) -> None:
        command: list[str] = [
            "pg_recvlogical",
            "--start",
            "--slot",
            f"{replication_slot_name}",
            "--file",
            "-",
            # We pass the maximum allowed fsync-interval (~24 days) to prevent
            # this process from advancing the slot. The purpose of this reader
            # is to just mark the slot as busy, not to move its reserved WAL
            # forward.
            "--fsync-interval",
            "2147483",
            "--dbname",
            "postgres",
            "--host",
            "postgres",
            "--port",
            "5432",
            "--username",
            "postgres",
            "--no-password",
            "-o",
            "proto_version=1",
            "-o",
            f"publication_names={publication_name}",
        ]
        config: ServiceConfig = {"mzbuild": "postgres"}

        config.update(
            {
                "command": command,
                "allow_host_ports": True,
                "ports": ["5432"],
                "environment": ["PGPASSWORD=postgres"],
            }
        )

        super().__init__(name="pg_recvlogical", config=config)


def create_postgres(
    pg_version: str | None, extra_command: list[str] = DEFAULT_PG_EXTRA_COMMAND
) -> Postgres:
    if pg_version is None:
        image = None
    else:
        image = f"postgres:{pg_version}"

    return Postgres(
        image=image, extra_command=extra_command, volumes=["secrets:/certs:ro"]
    )


SERVICES = [
    Mz(app_password=""),
    Materialized(
        volumes_extra=["secrets:/share/secrets"],
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::source::postgres=trace,info"
        },
        default_replication_factor=2,
    ),
    Testdrive(),
    TestCerts(),
    Toxiproxy(),
    create_postgres(pg_version=None),
    PostgresRecvlogical(
        replication_slot_name="", publication_name=""
    ),  # Overriden below
]


def get_targeted_pg_version(parser: WorkflowArgumentParser) -> str | None:
    parser.add_argument(
        "--pg-version",
        type=str,
    )

    args, _ = parser.parse_known_args()
    pg_version = args.pg_version

    if pg_version is not None:
        print(f"Running with Postgres version {pg_version}")

    return pg_version


# TODO: redesign ceased status database-issues#7687
# Test that how subsource statuses work across a variety of scenarios
# def workflow_statuses(c: Composition, parser: WorkflowArgumentParser) -> None:
#     c.up("materialized", "postgres", "toxiproxy")
#     c.run_testdrive_files("status/01-setup.td")

#     with c.override(Testdrive(no_reset=True)):
#         # Restart mz
#         c.kill("materialized")
#         c.up("materialized")

#         c.run_testdrive_files(
#             "status/02-after-mz-restart.td",
#             "status/03-toxiproxy-interrupt.td",
#             "status/04-drop-publication.td",
#         )


def workflow_replication_slots(c: Composition, parser: WorkflowArgumentParser) -> None:
    pg_version = get_targeted_pg_version(parser)
    with c.override(
        create_postgres(
            pg_version=pg_version, extra_command=["-c", "max_replication_slots=2"]
        )
    ):
        c.up("materialized", "postgres")
        c.run_testdrive_files("override/replication-slots.td")


def workflow_slot_creation_timeout(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Test that replication slot creation times out while an upstream transaction is in
    progress and succeeds once that transaction ends.
    """
    pg_version = get_targeted_pg_version(parser)
    with c.override(create_postgres(pg_version=pg_version)):
        c.up("materialized", "postgres")

        pg_conn = psycopg.connect(
            host="localhost",
            user="postgres",
            password="postgres",
            port=c.default_port("postgres"),
            autocommit=True,
        )
        pg_conn.execute("""
            ALTER USER postgres WITH replication;
            DROP SCHEMA IF EXISTS public CASCADE;
            CREATE SCHEMA public;
            DROP PUBLICATION IF EXISTS mz_source;
            CREATE PUBLICATION mz_source FOR ALL TABLES;
            CREATE TABLE t1 (pk INTEGER PRIMARY KEY, f2 TEXT);
            ALTER TABLE t1 REPLICA IDENTITY FULL;
            INSERT INTO t1 VALUES (1, 'one');
            """)

        # Logical slot creation waits for every transaction that holds a transaction ID.
        pg_conn.autocommit = False
        pg_conn.execute("INSERT INTO t1 VALUES (2, 'two')")

        c.run_testdrive_files(
            f"--var=default-replica-size=scale={Materialized.Size.DEFAULT_SIZE},workers={Materialized.Size.DEFAULT_SIZE}",
            "override/slot-creation-timeout-part-1.td",
        )

        pg_conn.commit()

        c.run_testdrive_files("--no-reset", "override/slot-creation-timeout-part-2.td")


def workflow_wal_level(c: Composition, parser: WorkflowArgumentParser) -> None:
    pg_version = get_targeted_pg_version(parser)
    for wal_level in ["replica", "minimal"]:
        with c.override(
            create_postgres(
                pg_version=pg_version,
                extra_command=[
                    "-c",
                    "max_wal_senders=0",
                    "-c",
                    f"wal_level={wal_level}",
                ],
            )
        ):
            c.up("materialized", "postgres")
            c.run_testdrive_files("override/insufficient-wal-level.td")


def workflow_replication_disabled(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    pg_version = get_targeted_pg_version(parser)
    with c.override(
        create_postgres(
            pg_version=pg_version, extra_command=["-c", "max_wal_senders=0"]
        )
    ):
        c.up("materialized", "postgres")
        c.run_testdrive_files("override/replication-disabled.td")


def workflow_silent_connection_drop(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Test that mz can regain a replication slot that is used by another service.
    """

    pg_version = get_targeted_pg_version(parser)
    with c.override(
        create_postgres(
            pg_version=pg_version,
            extra_command=[
                "-c",
                "wal_sender_timeout=0",
            ],
        ),
    ):
        c.up("postgres")

        pg_conn = psycopg.connect(
            host="localhost",
            user="postgres",
            password="postgres",
            port=c.default_port("postgres"),
        )

        _verify_exactly_n_replication_slots_exist(pg_conn, n=0)

        c.up("materialized")

        c.run_testdrive_files(
            "--no-reset",
            f"--var=default-replica-size=scale={Materialized.Size.DEFAULT_SIZE},workers={Materialized.Size.DEFAULT_SIZE}",
            "override/silent-connection-drop-part-1.td",
        )

        _verify_exactly_n_replication_slots_exist(pg_conn, n=1)

        _await_postgres_replication_slot_state(
            pg_conn,
            await_active=False,
            error_message="Replication slot is still active",
        )

        _claim_postgres_replication_slot(c, pg_conn)

        _await_postgres_replication_slot_state(
            pg_conn,
            await_active=True,
            error_message="Replication slot has not been claimed",
        )

        c.run_testdrive_files("--no-reset", "override/silent-connection-drop-part-2.td")

        _verify_exactly_n_replication_slots_exist(pg_conn, n=1)


def _await_postgres_replication_slot_state(
    pg_conn: Connection, await_active: bool, error_message: str
) -> None:
    for i in range(1, 5):
        is_active = _is_postgres_activation_slot_active(pg_conn)

        if is_active == await_active:
            return
        else:
            time.sleep(1)

    raise RuntimeError(error_message)


def _get_postgres_replication_slot_name(pg_conn: Connection) -> str:
    cursor = pg_conn.cursor()
    cursor.execute("SELECT slot_name FROM pg_replication_slots;")
    return cursor.fetchall()[0][0]


def _claim_postgres_replication_slot(c: Composition, pg_conn: Connection) -> None:
    replicator = PostgresRecvlogical(
        replication_slot_name=_get_postgres_replication_slot_name(pg_conn),
        publication_name="mz_source",
    )

    with c.override(replicator):
        c.up(replicator.name)


def _is_postgres_activation_slot_active(pg_conn: Connection) -> bool:
    cursor = pg_conn.cursor()
    cursor.execute("SELECT active FROM pg_replication_slots;")
    is_active = cursor.fetchall()[0][0]
    return is_active


def _verify_exactly_n_replication_slots_exist(pg_conn: Connection, n: int) -> None:
    cursor = pg_conn.cursor()
    cursor.execute("SELECT count(*) FROM pg_replication_slots;")
    count_slots = cursor.fetchall()[0][0]
    assert (
        count_slots == n
    ), f"Expected {n} replication slot(s) but found {count_slots} slot(s)"


def workflow_single_replica_source_notice(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Test the notice that is emitted when a cluster containing single-replica
    (OLTP) sources ends up with more than one replica: raising the replication
    factor of a cluster with such a source, adding a replica (billed or
    unbilled) to a cluster with such a source, and creating such a source on a
    cluster that already has more than one replica. Also checks that graceful
    resizes and their overlap replicas emit no spurious notice.
    """

    pg_version = get_targeted_pg_version(parser)
    with c.override(create_postgres(pg_version=pg_version)):
        c.up("materialized", "postgres")

        pg_conn = psycopg.connect(
            host="localhost",
            user="postgres",
            password="postgres",
            port=c.default_port("postgres"),
        )
        pg_conn.autocommit = True
        with pg_conn.cursor() as cur:
            cur.execute("ALTER USER postgres WITH replication;")
            cur.execute("DROP SCHEMA IF EXISTS public CASCADE;")
            cur.execute("CREATE SCHEMA public;")
            cur.execute("CREATE TABLE t (a int);")
            cur.execute("ALTER TABLE t REPLICA IDENTITY FULL;")
            cur.execute("DROP PUBLICATION IF EXISTS mz_source;")
            cur.execute("CREATE PUBLICATION mz_source FOR ALL TABLES;")

        conn = c.sql_connection()
        mz_system_conn = c.sql_connection(user="mz_system", port=6877)

        def collect_notices(conn: Connection) -> list[str]:
            collected: list[str] = []
            conn.add_notice_handler(
                lambda diag: collected.append(diag.message_primary or "")
            )
            return collected

        notices = collect_notices(conn)
        mz_system_notices = collect_notices(mz_system_conn)

        def expect_notice(collected: list[str], cluster: str, sources: str) -> None:
            expected = (
                f'cluster "{cluster}" has more than one replica, but the following '
                f"sources in it always run on only the first replica: {sources}"
            )
            assert (
                expected in collected
            ), f"expected notice {expected!r} in {collected!r}"
            collected.clear()

        def expect_no_notice(collected: list[str]) -> None:
            unexpected = [n for n in collected if "run on only the first replica" in n]
            assert not unexpected, f"unexpected notice(s): {unexpected!r}"
            collected.clear()

        size = f"scale={Materialized.Size.DEFAULT_SIZE},workers={Materialized.Size.DEFAULT_SIZE}"
        with conn.cursor() as cur:
            # Workflows in a CI shard share the Materialize catalog (the
            # containers are recreated between workflows, but the mzdata volume
            # survives), so drop any leftovers from earlier workflows or a
            # previous run of this one.
            cur.execute("DROP CONNECTION IF EXISTS pg CASCADE")
            cur.execute("DROP SECRET IF EXISTS pgpass CASCADE")
            cur.execute("DROP CLUSTER IF EXISTS c1 CASCADE")
            cur.execute("DROP CLUSTER IF EXISTS c2 CASCADE")
            cur.execute("DROP CLUSTER IF EXISTS c3 CASCADE")
            cur.execute("DROP CLUSTER IF EXISTS c4 CASCADE")
            notices.clear()

            cur.execute("CREATE SECRET pgpass AS 'postgres'")
            cur.execute(
                "CREATE CONNECTION pg TO POSTGRES "
                "(HOST postgres, DATABASE postgres, USER postgres, PASSWORD SECRET pgpass)"
            )

            # A source on a single-replica cluster emits no notice.
            cur.execute(
                f"CREATE CLUSTER c1 (SIZE '{size}', REPLICATION FACTOR 1)".encode()
            )
            cur.execute(
                "CREATE SOURCE src1 IN CLUSTER c1 "
                "FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')"
            )
            expect_no_notice(notices)

            # Raising the replication factor of a cluster with such a source
            # emits the notice.
            cur.execute("ALTER CLUSTER c1 SET (REPLICATION FACTOR 2)")
            expect_notice(notices, "c1", '"materialize.public.src1"')

            # Lowering it again does not.
            cur.execute("ALTER CLUSTER c1 SET (REPLICATION FACTOR 1)")
            expect_no_notice(notices)

            # Adding an unbilled replica to a managed cluster with such a
            # source also emits the notice, to the internal session that
            # created the replica.
            with mz_system_conn.cursor() as system_cur:
                system_cur.execute(
                    f"CREATE CLUSTER REPLICA c1.free "
                    f"(SIZE '{size}', BILLED AS 'free', INTERNAL)".encode()
                )
            expect_notice(mz_system_notices, "c1", '"materialize.public.src1"')
            expect_no_notice(notices)
            with mz_system_conn.cursor() as system_cur:
                system_cur.execute("DROP CLUSTER REPLICA c1.free")

            # A graceful resize (a shape change writes a reconfiguration
            # record and runs an overlap replica until cut-over) does not
            # change how many replicas the cluster aims for, so it emits no
            # notice.
            other_size = f"scale={Materialized.Size.DEFAULT_SIZE},workers=1"
            cur.execute(f"ALTER CLUSTER c1 SET (SIZE '{other_size}')".encode())
            expect_no_notice(notices)

            # Nor does creating such a source while the reconfiguration may
            # still be in flight: the overlap replica is not counted.
            cur.execute(
                "CREATE SOURCE src5 IN CLUSTER c1 "
                "FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')"
            )
            expect_no_notice(notices)

            # Raising the replication factor together with a shape change
            # routes through the reconfiguration path and emits the notice.
            # Changing the replication factor is refused while the previous
            # reconfiguration is still in flight, so retry until it settles.
            for _ in range(60):
                try:
                    cur.execute(
                        f"ALTER CLUSTER c1 SET (SIZE '{size}', REPLICATION FACTOR 2)".encode()
                    )
                    break
                except psycopg.Error as e:
                    assert "reconfiguration is in progress" in str(
                        e
                    ), f"unexpected error: {e}"
                    time.sleep(1)
            else:
                raise RuntimeError("reconfiguration did not settle")
            expect_notice(
                notices,
                "c1",
                '"materialize.public.src1", "materialize.public.src5"',
            )

            # Creating such a source on a cluster that already has two replicas
            # emits the notice.
            cur.execute(
                f"CREATE CLUSTER c2 (SIZE '{size}', REPLICATION FACTOR 2)".encode()
            )
            expect_no_notice(notices)
            cur.execute(
                "CREATE SOURCE src2 IN CLUSTER c2 "
                "FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')"
            )
            expect_notice(notices, "c2", '"materialize.public.src2"')

            # Creating such a source in a DDL transaction includes the source
            # in the notice even though its creation is only staged when the
            # notice is emitted.
            cur.execute(
                f"CREATE CLUSTER c4 (SIZE '{size}', REPLICATION FACTOR 2)".encode()
            )
            expect_no_notice(notices)
            cur.execute("BEGIN")
            cur.execute(
                "CREATE SOURCE src4 IN CLUSTER c4 "
                "FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')"
            )
            cur.execute("COMMIT")
            expect_notice(notices, "c4", '"materialize.public.src4"')

            # Adding a replica to an unmanaged cluster with such a source emits
            # the notice.
            cur.execute(f"CREATE CLUSTER c3 REPLICAS (r1 (SIZE '{size}'))".encode())
            cur.execute(
                "CREATE SOURCE src3 IN CLUSTER c3 "
                "FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')"
            )
            expect_no_notice(notices)
            cur.execute(f"CREATE CLUSTER REPLICA c3.r2 (SIZE '{size}')".encode())
            expect_notice(notices, "c3", '"materialize.public.src3"')

            # The same for an unbilled replica taking an unmanaged cluster from
            # one replica to two.
            cur.execute("DROP CLUSTER REPLICA c3.r2")
            expect_no_notice(notices)
            with mz_system_conn.cursor() as system_cur:
                system_cur.execute(
                    f"CREATE CLUSTER REPLICA c3.free "
                    f"(SIZE '{size}', BILLED AS 'free', INTERNAL)".encode()
                )
            expect_notice(mz_system_notices, "c3", '"materialize.public.src3"')


def workflow_offset_committed_seed(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Adding a table to a source must not report any other table's
    `offset_committed` below the value it reported before. The statistics
    views keep the higher value, so a regression is only visible as an
    `offset_committed erroneously regressed` error in the clusterd log.

    The added table's snapshot is held open with an ACCESS EXCLUSIVE lock on
    its primary key index, taken by REINDEX in an open transaction: planning
    the snapshot COPY opens the index, purification does not. Inserts keep the
    WAL moving across the DDL so the slot position, which trails the committed
    LSN by up to one feedback interval, differs from what t1 last reported.
    """
    pg_version = get_targeted_pg_version(parser)
    regression = "offset_committed erroneously regressed"

    def pg_connect() -> Connection:
        return psycopg.connect(
            host="localhost",
            user="postgres",
            password="postgres",
            port=c.default_port("postgres"),
        )

    def insert_until(stop: threading.Event) -> None:
        with pg_connect() as conn:
            conn.autocommit = True
            while not stop.is_set():
                conn.execute("INSERT INTO t1 (v) VALUES ('tick')")
                time.sleep(0.02)

    with c.override(
        create_postgres(pg_version=pg_version),
        Materialized(
            volumes_extra=["secrets:/share/secrets"],
            additional_system_parameter_defaults={
                "log_filter": "mz_storage::source::postgres=trace,info",
                # Ship per-worker statistics often, so the value worker 0 holds
                # when a seed arrives is fresh rather than up to 10s old.
                "storage_statistics_collection_interval": "100",
                "storage_statistics_interval": "200",
                "min_timestamp_interval": "50ms",
            },
            default_replication_factor=2,
        ),
    ):
        c.up("materialized", "postgres")
        c.run_testdrive_files("override/offset-committed-seed-part-1.td")

        # Not committed until the end, so the index lock is held throughout.
        lock_conn = pg_connect()
        lock_conn.execute("REINDEX INDEX t2_pkey")
        stop_inserts = threading.Event()
        with ThreadPoolExecutor(max_workers=1) as executor:
            inserter = executor.submit(insert_until, stop_inserts)
            try:
                c.run_testdrive_files(
                    "--no-reset", "override/offset-committed-seed-part-2.td"
                )
                # Several statistics ticks for the re-rendered source to report its seeds.
                time.sleep(5)
            finally:
                stop_inserts.set()
                lock_conn.rollback()
                lock_conn.close()
                # Re-raises if the inserter died, which would have left the
                # WAL idle and the run meaningless.
                inserter.result()

        regressions = c.invoke("logs", "materialized", capture=True).stdout.count(
            regression
        )
        assert regressions == 0, f"{regressions} offset_committed regressions logged"


def workflow_cdc(c: Composition, parser: WorkflowArgumentParser) -> None:
    pg_version = get_targeted_pg_version(parser)

    parser.add_argument(
        "filter",
        nargs="*",
        default=["*.td"],
        help="limit to only the files matching filter",
    )
    args = parser.parse_args()

    matching_files = []
    for filter in args.filter:
        matching_files.extend(glob.glob(filter, root_dir=MZ_ROOT / "test" / "pg-cdc"))

    if pg_version is not None:
        # Vanilla Postgres images don't have SSL configured, skip SSL tests
        matching_files = [f for f in matching_files if not f.startswith("pg-cdc-ssl")]

    sharded_files: list[str] = buildkite.shard_list(
        sorted(matching_files), lambda file: file
    )
    print(f"Files: {sharded_files}")

    c.up(Service("test-certs", idle=True))
    ssl_ca = c.run("test-certs", "cat", "/secrets/ca.crt", capture=True).stdout
    ssl_cert = c.run("test-certs", "cat", "/secrets/certuser.crt", capture=True).stdout
    ssl_key = c.run("test-certs", "cat", "/secrets/certuser.key", capture=True).stdout
    ssl_wrong_cert = c.run(
        "test-certs", "cat", "/secrets/postgres.crt", capture=True
    ).stdout
    ssl_wrong_key = c.run(
        "test-certs", "cat", "/secrets/postgres.key", capture=True
    ).stdout
    ssl_ca_unrelated = c.run(
        "test-certs", "cat", "/secrets/ca-selective.crt", capture=True
    ).stdout

    with c.override(create_postgres(pg_version=pg_version)):
        c.up("materialized", "test-certs", "postgres")
        c.test_parts(
            sharded_files,
            lambda file: c.run_testdrive_files(
                f"--var=ssl-ca={ssl_ca}",
                f"--var=ssl-cert={ssl_cert}",
                f"--var=ssl-key={ssl_key}",
                f"--var=ssl-wrong-cert={ssl_wrong_cert}",
                f"--var=ssl-wrong-key={ssl_wrong_key}",
                f"--var=ssl-ca-unrelated={ssl_ca_unrelated}",
                f"--var=default-replica-size=scale={Materialized.Size.DEFAULT_SIZE},workers={Materialized.Size.DEFAULT_SIZE}",
                f"--var=default-storage-size=scale={Materialized.Size.DEFAULT_SIZE},workers=1",
                file,
            ),
        )


def workflow_large_scale(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    The goal is to test a large scale Postgres instance and to make sure that we can successfully ingest data from it quickly.
    """
    pg_version = get_targeted_pg_version(parser)
    with c.override(
        create_postgres(
            pg_version=pg_version, extra_command=["-c", "max_replication_slots=3"]
        )
    ):
        c.up("materialized", "postgres", Service("testdrive", idle=True))

        # Set up the Postgres server with the initial records, set up the connection to
        # the Postgres server in Materialize.
        c.testdrive(dedent("""
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                ALTER USER postgres WITH replication;
                DROP SCHEMA IF EXISTS public CASCADE;
                DROP PUBLICATION IF EXISTS mz_source;
                CREATE SCHEMA public;

                > CREATE SECRET IF NOT EXISTS pgpass AS 'postgres'
                > CREATE CONNECTION IF NOT EXISTS pg TO POSTGRES (HOST postgres, DATABASE postgres, USER postgres, PASSWORD SECRET pgpass)

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                DROP TABLE IF EXISTS products;
                CREATE TABLE products (id int NOT NULL, name varchar(255) DEFAULT NULL, merchant_id int NOT NULL, price int DEFAULT NULL, status int DEFAULT NULL, created_at timestamp NULL, recordSizePayload text, PRIMARY KEY (id));
                ALTER TABLE products REPLICA IDENTITY FULL;
                CREATE PUBLICATION mz_source FOR ALL TABLES;

                > DROP SOURCE IF EXISTS s1 CASCADE;
                """))

    def make_inserts(c: Composition, start: int, batch_num: int):
        c.testdrive(
            args=["--no-reset"],
            input=dedent(f"""
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO products (id, name, merchant_id, price, status, created_at, recordSizePayload) SELECT {start} + row_number() OVER (), 'name' || ({start} + row_number() OVER ()), ({start} + row_number() OVER ()) % 1000, ({start} + row_number() OVER ()) % 1000, ({start} + row_number() OVER ()) % 10, '2024-12-12'::DATE, repeat('x', 1000000) FROM generate_series(1, {batch_num});
            """),
        )

    num_rows = 100_000  # out of memory with 200_000 rows
    batch_size = 10_000
    for i in range(0, num_rows, batch_size):
        batch_num = min(batch_size, num_rows - i)
        make_inserts(c, i, batch_num)

    # Update pg_class.relpages so Materialize's ctid-based parallel snapshot
    # can partition across workers from the first read.
    c.testdrive(
        args=["--no-reset"],
        input=dedent("""
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            ANALYZE products;
        """),
    )

    c.testdrive(
        args=["--no-reset"],
        input=dedent(f"""
            > CREATE SOURCE s1
              FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
            > CREATE TABLE products FROM SOURCE s1 (REFERENCE products);
            > SELECT COUNT(*) FROM products;
            {num_rows}
            """),
    )

    make_inserts(c, num_rows, 1)

    c.testdrive(
        args=["--no-reset"],
        input=dedent(f"""
            > SELECT COUNT(*) FROM products;
            {num_rows + 1}
            """),
    )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    def process(name: str) -> None:
        # The kill points of `truncate-replay-kill` depend on wall clock timing.
        if name in ("default", "large-scale", "truncate-replay-kill"):
            return

        c.kill("postgres")
        c.rm("postgres")
        c.kill("materialized")
        c.rm("materialized")

        with c.test_case(name):
            c.workflow(name, *parser.args)

    workflows_with_internal_sharding = ["cdc"]
    sharded_workflows = workflows_with_internal_sharding + buildkite.shard_list(
        [
            w
            for w in c.workflows
            if w not in workflows_with_internal_sharding and w != "migration"
        ],
        lambda w: w,
    )
    print(
        f"Workflows in shard with index {buildkite.get_parallelism_index()}: {sharded_workflows}"
    )
    c.test_parts(sharded_workflows, process)


def _exec(conn: Connection, sql: str) -> Any:
    """Executes `sql` on `conn` and returns the cursor."""
    return conn.execute(sql.encode())


def _truncate_materialized(
    params: dict[str, str],
    replication_factor: int = 1,
    sanity_restart: bool = True,
    environment_extra: list[str] = [],
) -> Materialized:
    return Materialized(
        volumes_extra=["secrets:/share/secrets"],
        additional_system_parameter_defaults={"log_filter": "info", **params},
        default_replication_factor=replication_factor,
        sanity_restart=sanity_restart,
        environment_extra=environment_extra,
    )


def _pg_connect(c: Composition) -> Connection:
    conn = psycopg.connect(
        host="localhost",
        user="postgres",
        password="postgres",
        port=c.default_port("postgres"),
    )
    conn.autocommit = True
    return conn


def _mz_connect(c: Composition) -> Connection:
    conn = psycopg.connect(
        host="localhost",
        user="materialize",
        port=c.default_port("materialized"),
    )
    conn.autocommit = True
    return conn


def _query(conn: Connection, sql: str) -> Any:
    """The rows of `sql`, or the text of the error it fails with."""
    try:
        return _exec(conn, sql).fetchall()
    except psycopg.Error as e:
        return str(e)


def _wait_for(
    conn: Connection, sql: str, matches: Callable[[Any], bool], timeout: float = 300
) -> Any:
    start = time.time()
    while True:
        got = _query(conn, sql)
        if matches(got):
            return got
        if time.time() - start > timeout:
            raise AssertionError(f"{sql}: gave up waiting, last result {got}")
        time.sleep(0.1)


def _create_truncate_source(
    c: Composition, tables: list[str], cluster_size: str = "scale=1,workers=1"
) -> Connection:
    """Creates a source with one table for each upstream table in `tables`."""
    mz = _mz_connect(c)
    _exec(mz, f"CREATE CLUSTER src SIZE '{cluster_size}'")
    _exec(mz, "CREATE SECRET pgpass AS 'postgres'")
    _exec(
        mz,
        "CREATE CONNECTION pg TO POSTGRES (HOST postgres, DATABASE postgres, "
        "USER postgres, PASSWORD SECRET pgpass)",
    )
    _exec(
        mz,
        "CREATE SOURCE mz_source IN CLUSTER src FROM POSTGRES CONNECTION pg "
        "(PUBLICATION 'mz_source')",
    )
    for t in tables:
        _exec(mz, f"CREATE TABLE {t} FROM SOURCE mz_source (REFERENCE {t})")
    return mz


def _clusterd_pid(c: Composition, mz: Connection) -> str:
    """The PID of the `src` cluster's clusterd process in the materialized container."""
    cluster_id = _exec(mz, "SELECT id FROM mz_clusters WHERE name = 'src'").fetchall()[
        0
    ][0]
    out = c.exec(
        "materialized",
        "bash",
        "-c",
        "for p in /proc/[0-9]*; do "
        "if tr '\\0' ' ' < $p/cmdline 2>/dev/null "
        f"| grep -q -- '^[^ ]*clusterd .*{cluster_id}'; "
        "then basename $p; fi; done",
        capture=True,
        silent=True,
    ).stdout.split()
    assert len(out) == 1, f"clusterd pids for {cluster_id}: {out}"
    return out[0]


def _parse_duration(text: str) -> float:
    """Parses a Rust `Duration` debug representation into seconds."""
    for unit, scale in [("ns", 1e-9), ("µs", 1e-6), ("ms", 1e-3), ("s", 1.0)]:
        if text.endswith(unit):
            return float(text[: -len(unit)]) * scale
    raise ValueError(text)


def _materialized_logs(c: Composition) -> list[str]:
    return c.invoke(
        "logs", "materialized", capture=True, silent=True
    ).stdout.splitlines()


def workflow_truncate_replay(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Runs the truncate test with the retraction buffer disabled, so every
    truncate that needs updates not yet durable replays the replication slot.
    """
    with c.override(
        _truncate_materialized(
            {"pg_source_truncate_buffer_max_bytes": "0"}, replication_factor=2
        )
    ):
        c.up("materialized", "postgres")
        c.run_testdrive_files("truncate-table.td")
        replays = [line for line in _materialized_logs(c) if "replayed" in line]
        assert replays, "no truncate replayed"


def workflow_truncate_replay_schema(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    A schema change between a truncate and the replay of the updates before it
    leaves the table as the buffered retraction does: a compatible change keeps
    the post-truncate rows, an incompatible one errors the table.

    The snapshot retraction is rate limited, so each replay starts seconds after
    the schema change, which follows its truncate after about a second.
    """
    tables = ["tadd", "tdrop", "ttype"]
    rows = 10000
    expected_error = "incompatible schema change"

    def run(buffer_max_bytes: str) -> tuple[dict[str, Any], int]:
        with c.override(
            _truncate_materialized(
                {
                    "pg_source_truncate_buffer_max_bytes": buffer_max_bytes,
                    "pg_source_truncate_retract_max_rows_per_sec": "1000",
                    # Only the replication stream reports the schema changes, which
                    # keeps the outcome independent of when the periodic check runs.
                    "pg_schema_validation_interval": "1h",
                },
                # The incompatible schema changes leave sources stalled.
                sanity_restart=False,
            )
        ):
            c.down(destroy_volumes=True)
            c.up("materialized", "postgres")
            pg = _pg_connect(c)
            _exec(pg, "CREATE TABLE tadd (id int, a text)")
            _exec(pg, "CREATE TABLE tdrop (id int, a text, b text)")
            _exec(pg, "CREATE TABLE ttype (id int, a text)")
            _exec(pg, "CREATE TABLE tsentinel (id int)")
            for t in tables + ["tsentinel"]:
                _exec(pg, f"ALTER TABLE {t} REPLICA IDENTITY FULL")
            _exec(
                pg,
                "INSERT INTO tdrop SELECT g, 'a', 'b' "
                f"FROM generate_series(1, {rows}) g",
            )
            for t in ["tadd", "ttype"]:
                _exec(
                    pg,
                    f"INSERT INTO {t} SELECT g, 'a' FROM generate_series(1, {rows}) g",
                )
            _exec(pg, "CREATE PUBLICATION mz_source FOR ALL TABLES")
            mz = _create_truncate_source(c, tables + ["tsentinel"])
            for t in tables:
                _wait_for(mz, f"SELECT count(*) FROM {t}", lambda r: r == [(rows,)])
            # Durable, so the updates the truncates retract next are not.
            time.sleep(3)

            # The first change of each table after its snapshot carries its schema, which the
            # first pass verifies against the upstream schema it then finds. Let it verify the
            # original schema before the schema changes.
            for t, row in [("tadd", "(-1, 'pre')"), ("tdrop", "(-1, 'pre', 'pre')")]:
                _exec(pg, f"INSERT INTO {t} VALUES {row}")
            _exec(pg, "INSERT INTO ttype VALUES (-1, 'pre')")
            for t in tables:
                _wait_for(mz, f"SELECT count(*) FROM {t}", lambda r: r == [(rows + 1,)])

            def truncate_then_alter(
                table: str, pre: str, alter: str, post: str
            ) -> None:
                _exec(pg, f"INSERT INTO {table} VALUES {pre}")
                _exec(pg, f"TRUNCATE {table}")
                # Wait until the first pass is past the truncate, so that it verifies the
                # schema of the transactions before it while that schema is current. The
                # sentinel's frontier passes the truncate's wall clock time only then.
                truncated_at = int(time.time() * 1000)
                _wait_for(
                    mz,
                    "SELECT f.write_frontier::text::numeric "
                    "FROM mz_internal.mz_frontiers f JOIN mz_objects o "
                    "ON o.id = f.object_id WHERE o.name = 'tsentinel'",
                    lambda r: isinstance(r, list)
                    and len(r) == 1
                    and r[0][0] > truncated_at + 1000,
                )
                _exec(pg, f"ALTER TABLE {table} {alter}")
                _exec(pg, f"INSERT INTO {table} VALUES {post}")

            truncate_then_alter(
                "tadd", "(0, 'pre')", "ADD COLUMN c int", "(1, 'post', 7)"
            )
            truncate_then_alter(
                "tdrop", "(0, 'pre', 'pre')", "DROP COLUMN b", "(1, 'post')"
            )
            truncate_then_alter(
                "ttype", "(0, 'pre')", "ALTER COLUMN a TYPE int USING 0", "(1, 5)"
            )

            # Each truncate is retracted once, by the buffer or by a replay.
            start = time.time()
            while sum(
                1 for line in _materialized_logs(c) if "retracted truncate" in line
            ) < len(tables):
                if time.time() - start > 120:
                    for line in _materialized_logs(c):
                        if any(
                            w in line for w in ["truncat", "replay", "panic", "Stalled"]
                        ):
                            print(f"LOG {line[:400]}")
                    raise AssertionError("truncates not retracted")
                time.sleep(0.5)
            time.sleep(3)
            results: dict[str, Any] = {"tadd": _query(mz, "SELECT * FROM tadd")}
            for t in ["tdrop", "ttype"]:
                got = _query(mz, f"SELECT * FROM {t}")
                incompatible = isinstance(got, str) and expected_error in got
                results[t] = expected_error if incompatible else got
            replays = sum(1 for line in _materialized_logs(c) if "replayed" in line)
            return results, replays

    buffered, buffered_replays = run(str(256 << 20))
    replayed, replays = run("0")
    print(f"buffered: {buffered}, replays: {buffered_replays}")
    print(f"replayed: {replayed}, replays: {replays}")
    expected = {"tadd": [(1, "post")], "tdrop": expected_error, "ttype": expected_error}
    assert buffered == expected, buffered
    assert buffered_replays == 0, buffered_replays
    assert replays == len(tables), f"expected {len(tables)} replays, saw {replays}"
    assert replayed == expected, replayed


def workflow_truncate_replay_kill(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Kills clusterd while a truncate replays a large transaction, at points early,
    in the middle, and late in the replay, and checks that the table and a
    materialized view on it converge to the upstream contents after the restart.
    """
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--repetitions", type=int, default=2)
    args = parser.parse_args()

    def replay_lines() -> list[str]:
        return [line for line in _materialized_logs(c) if "replaying from" in line]

    def done_lines() -> list[str]:
        return [line for line in _materialized_logs(c) if "replayed" in line]

    with c.override(
        _truncate_materialized({"pg_source_truncate_buffer_max_bytes": "0"})
    ):
        c.up("materialized", "postgres")
        pg = _pg_connect(c)
        _exec(pg, "DROP PUBLICATION IF EXISTS mz_source")
        _exec(pg, "DROP TABLE IF EXISTS t")
        _exec(pg, "CREATE TABLE t (id bigint, pad text)")
        _exec(pg, "ALTER TABLE t REPLICA IDENTITY FULL")
        _exec(pg, "CREATE PUBLICATION mz_source FOR ALL TABLES")
        mz = _create_truncate_source(c, ["t"])
        _exec(
            mz,
            "CREATE MATERIALIZED VIEW t_summary AS "
            "SELECT count(*) AS c, sum(id) AS s, count(DISTINCT (id, pad)) AS d FROM t",
        )
        _exec(mz, "CREATE DEFAULT INDEX ON t")
        # Before the initial snapshot of `t` is durable, a truncate it covers needs no
        # retraction at all.
        _exec(pg, "INSERT INTO t VALUES (0, 'warm')")
        _wait_for(mz, "SELECT c FROM t_summary", lambda r: r == [(1,)])
        _exec(pg, "DELETE FROM t")
        _wait_for(mz, "SELECT c FROM t_summary", lambda r: r == [(0,)])

        def iteration(kill_fraction: float | None, duration: float, n: int) -> str:
            started = len(replay_lines())
            done = len(done_lines())
            _exec(
                pg,
                "INSERT INTO t SELECT g, md5(g::text) "
                f"FROM generate_series(1, {args.rows}) g",
            )
            _exec(pg, "TRUNCATE t")
            post = [(n * 10 + i, f"post{i}") for i in range(3)]
            for id, pad in post:
                _exec(pg, f"INSERT INTO t VALUES ({id}, '{pad}')")
            # A truncate needs no replay when its retraction finds the updates before it
            # durable, which this loop does not exclude.
            start = time.time()
            while len(replay_lines()) == started and len(done_lines()) == done:
                if time.time() - start > 60:
                    break
                time.sleep(0.05)
            replaying = len(replay_lines()) > started
            outcome = "no kill" if replaying else "no replay"
            if replaying and kill_fraction is not None:
                time.sleep(kill_fraction * duration)
                in_flight = len(done_lines()) == done
                c.exec("materialized", "kill", "-9", _clusterd_pid(c, mz), silent=True)
                outcome = "killed mid-replay" if in_flight else "killed after replay"
            expected = sorted(post)
            _wait_for(mz, "SELECT * FROM t ORDER BY id", lambda r: r == expected)
            total = sum(id for id, _ in post)
            _wait_for(
                mz,
                "SELECT c, s, d FROM t_summary",
                lambda r: r == [(3, total, 3)],
            )
            # The rows are exactly the upstream rows, with multiplicity one.
            values = ", ".join(f"({id}, '{pad}')" for id, pad in post)
            for sql in [
                f"SELECT id, pad FROM t EXCEPT ALL VALUES {values}",
                f"VALUES {values} EXCEPT ALL SELECT id, pad FROM t",
            ]:
                assert _query(mz, sql) == [], (sql, _query(mz, sql))
            # The view stays live.
            _exec(pg, f"INSERT INTO t VALUES ({n * 10 + 9}, 'live')")
            _wait_for(
                mz,
                "SELECT c FROM t_summary",
                lambda r: r == [(4,)],
            )
            _exec(pg, f"DELETE FROM t WHERE id = {n * 10 + 9}")
            _wait_for(mz, "SELECT c FROM t_summary", lambda r: r == [(3,)])
            return outcome

        def logged_iteration(
            kill_fraction: float | None, duration: float, n: int
        ) -> str:
            try:
                return iteration(kill_fraction, duration, n)
            except AssertionError:
                for line in _materialized_logs(c):
                    if any(
                        w in line
                        for w in ["truncat", "replay", "panic", "started replication"]
                    ):
                        print(f"LOG {line[:400]}")
                raise

        logged_iteration(None, 0, 0)
        durations = [
            _parse_duration(line.split(" updates, in ")[1].split(",")[0])
            for line in done_lines()
        ]
        duration = max(durations) if durations else 2.0
        print(f"replay without kill took {duration:.2f}s")

        outcomes = []
        n = 1
        for _ in range(args.repetitions):
            for fraction in [0.1, 0.4, 0.7]:
                outcome = logged_iteration(fraction, duration, n)
                print(f"kill at {fraction:.0%} of the replay: {outcome}")
                outcomes.append((fraction, outcome))
                n += 1
        mid = [o for o in outcomes if o[1] == "killed mid-replay"]
        assert len(mid) >= 2 * args.repetitions, outcomes


def workflow_truncate_replay_failpoint(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Fails a truncate's replay of a large transaction once, at its start, in its
    middle, and at its end, and checks that the table and a materialized view on
    it converge to the upstream contents after the source restarts.
    """
    parser.add_argument("--rows", type=int, default=100_000)
    args = parser.parse_args()

    for point in ["start", "mid", "end"]:
        failpoint = f"pg_truncate_replay_{point}"
        with c.override(
            _truncate_materialized(
                {"pg_source_truncate_buffer_max_bytes": "0"},
                # clusterd reads failpoints from the environment at startup. `1*return`
                # fails once, so the replay after the restart completes.
                environment_extra=[f"FAILPOINTS={failpoint}=1*return"],
            )
        ):
            c.down(destroy_volumes=True)
            c.up("materialized", "postgres")
            pg = _pg_connect(c)
            _exec(pg, "CREATE TABLE t (id bigint, pad text)")
            _exec(pg, "ALTER TABLE t REPLICA IDENTITY FULL")
            _exec(pg, "CREATE PUBLICATION mz_source FOR ALL TABLES")
            mz = _create_truncate_source(c, ["t"])
            _exec(
                mz,
                "CREATE MATERIALIZED VIEW t_summary AS "
                "SELECT count(*) AS c, sum(id) AS s, count(DISTINCT (id, pad)) AS d "
                "FROM t",
            )
            _exec(mz, "CREATE DEFAULT INDEX ON t")
            # A truncate that the initial snapshot covers needs no retraction, so wait
            # for it to be durable.
            _exec(pg, "INSERT INTO t VALUES (0, 'warm')")
            _wait_for(mz, "SELECT c FROM t_summary", lambda r: r == [(1,)])
            _exec(pg, "DELETE FROM t")
            _wait_for(mz, "SELECT c FROM t_summary", lambda r: r == [(0,)])

            # The truncate follows the insert before the insert is durable, and the
            # disabled buffer makes the retraction replay it.
            _exec(
                pg,
                "INSERT INTO t SELECT g, md5(g::text) "
                f"FROM generate_series(1, {args.rows}) g",
            )
            _exec(pg, "TRUNCATE t")
            post = [(1, "post0"), (2, "post1"), (3, "post2")]
            for id, pad in post:
                _exec(pg, f"INSERT INTO t VALUES ({id}, '{pad}')")

            try:
                _wait_for(mz, "SELECT * FROM t ORDER BY id", lambda r: r == post)
                _wait_for(
                    mz, "SELECT c, s, d FROM t_summary", lambda r: r == [(3, 6, 3)]
                )
                values = ", ".join(f"({id}, '{pad}')" for id, pad in post)
                for sql in [
                    f"SELECT id, pad FROM t EXCEPT ALL VALUES {values}",
                    f"VALUES {values} EXCEPT ALL SELECT id, pad FROM t",
                ]:
                    assert _query(mz, sql) == [], (sql, _query(mz, sql))
                # The view stays live.
                _exec(pg, "INSERT INTO t VALUES (4, 'live')")
                _wait_for(mz, "SELECT c, s FROM t_summary", lambda r: r == [(4, 10)])

                logs = _materialized_logs(c)
                fired = [
                    line for line in logs if f"failpoint {failpoint} fired" in line
                ]
                replays = [line for line in logs if "replayed" in line]
                assert len(fired) == 1, f"{failpoint} fired {len(fired)} times"
                assert replays, "no replay completed"
                print(f"{failpoint}: fired once, then {len(replays)} replay completed")
            except AssertionError:
                for line in _materialized_logs(c):
                    if any(
                        w in line
                        for w in [
                            "truncat",
                            "replay",
                            "panic",
                            "failpoint",
                            "synthetic",
                        ]
                    ):
                        print(f"LOG {line[:400]}")
                raise
