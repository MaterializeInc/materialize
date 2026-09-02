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
import time
from textwrap import dedent

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
        if name in ("default", "large-scale"):
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
