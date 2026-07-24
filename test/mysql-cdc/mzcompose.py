# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Functional test for the native (non-Debezium) MySQL sources.
"""

import glob
import threading
from textwrap import dedent

from materialize import MZ_ROOT, buildkite
from materialize.mysql_util import (
    retrieve_invalid_ssl_context_for_mysql,
    retrieve_ssl_context_for_mysql,
)
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy


def create_mysql(mysql_version: str, binlog_full_metadata: bool = True) -> MySql:
    additional_args = []
    if binlog_full_metadata:
        additional_args.extend(
            [
                "--binlog-row-metadata=FULL",
                "--gtid_mode=ON",
                "--enforce_gtid_consistency=ON",
            ]
        )
    return MySql(version=mysql_version, additional_args=additional_args)


def create_mysql_replica(
    mysql_version: str, binlog_full_metadata: bool = True
) -> MySql:
    additional_args = [
        "--gtid_mode=ON",
        "--enforce_gtid_consistency=ON",
        "--skip-replica-start",
        "--server-id=2",
    ]
    if binlog_full_metadata:
        additional_args.append("--binlog-row-metadata=FULL")
    return MySql(
        name="mysql-replica",
        version=mysql_version,
        use_seeded_image=False,
        additional_args=additional_args,
    )


SERVICES = [
    Mz(app_password=""),
    Materialized(
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::source::mysql=trace,info"
        },
        default_replication_factor=2,
    ),
    create_mysql(MySql.DEFAULT_VERSION),
    create_mysql_replica(MySql.DEFAULT_VERSION),
    TestCerts(),
    Toxiproxy(),
    Testdrive(default_timeout="180s"),
]


def get_targeted_mysql_version(parser: WorkflowArgumentParser) -> str:
    parser.add_argument(
        "--mysql-version",
        default=MySql.DEFAULT_VERSION,
        type=str,
    )

    args, _ = parser.parse_known_args()
    print(f"Running with MySQL version {args.mysql_version}")
    return args.mysql_version


def reset_binlog_stmt(mysql_version: str) -> str:
    # RESET BINARY LOGS AND GTIDS was introduced in MySQL 8.2;
    # RESET MASTER was removed in MySQL 8.4.
    major_minor = tuple(int(p) for p in mysql_version.split(".")[:2])
    return "RESET MASTER" if major_minor < (8, 2) else "RESET BINARY LOGS AND GTIDS"


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    def process(name: str) -> None:
        if name in ("default", "large-scale"):
            return
        with c.test_case(name):
            c.workflow(name, *parser.args)

    workflows_with_internal_sharding = ["cdc"]
    sharded_workflows = workflows_with_internal_sharding + buildkite.shard_list(
        [w for w in c.workflows if w not in workflows_with_internal_sharding],
        lambda w: w,
    )
    print(
        f"Workflows in shard with index {buildkite.get_parallelism_index()}: {sharded_workflows}"
    )
    c.test_parts(sharded_workflows, process)


def workflow_cdc(c: Composition, parser: WorkflowArgumentParser) -> None:
    mysql_version = get_targeted_mysql_version(parser)

    parser.add_argument(
        "filter",
        nargs="*",
        default=["*.td"],
        help="limit to only the files matching filter",
    )
    args = parser.parse_args()

    matching_files = []
    for filter in args.filter:
        matching_files.extend(
            glob.glob(filter, root_dir=MZ_ROOT / "test" / "mysql-cdc")
        )
    sharded_files: list[str] = buildkite.shard_list(
        sorted(matching_files), lambda file: file
    )
    print(f"Files: {sharded_files}")

    with c.override(create_mysql(mysql_version)):
        c.up("materialized", "mysql")

        valid_ssl_context = retrieve_ssl_context_for_mysql(c)
        wrong_ssl_context = retrieve_invalid_ssl_context_for_mysql(c)

        c.sources_and_sinks_ignored_from_validation.add("drop_table")

        c.test_parts(
            sharded_files,
            lambda file: c.run_testdrive_files(
                f"--var=ssl-ca={valid_ssl_context.ca}",
                f"--var=ssl-client-cert={valid_ssl_context.client_cert}",
                f"--var=ssl-client-key={valid_ssl_context.client_key}",
                f"--var=ssl-wrong-ca={wrong_ssl_context.ca}",
                f"--var=ssl-wrong-client-cert={wrong_ssl_context.client_cert}",
                f"--var=ssl-wrong-client-key={wrong_ssl_context.client_key}",
                f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
                "--var=mysql-user-password=us3rp4ssw0rd",
                f"--var=default-replica-size=scale={Materialized.Size.DEFAULT_SIZE},workers={Materialized.Size.DEFAULT_SIZE}",
                f"--var=default-storage-size=scale={Materialized.Size.DEFAULT_SIZE},workers=1",
                file,
            ),
        )


def workflow_replica_connection(c: Composition, parser: WorkflowArgumentParser) -> None:
    mysql_version = get_targeted_mysql_version(parser)
    with c.override(create_mysql(mysql_version), create_mysql_replica(mysql_version)):
        c.up("materialized", "mysql", "mysql-replica")
        c.run_testdrive_files(
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            f"--var=reset-binlog={reset_binlog_stmt(mysql_version)}",
            "override/10-replica-connection.td",
        )


def workflow_schema_change_restart(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Validates that a schema change done to a table after the MySQL source is created
    but before the snapshot is completed is detected after a restart.
    """

    mysql_version = get_targeted_mysql_version(parser)
    with c.override(create_mysql(mysql_version)):
        c.up("materialized", "mysql")
        c.run_testdrive_files(
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            "schema-restart/before-restart.td",
        )

    with c.override(Testdrive(no_reset=True), create_mysql(mysql_version)):
        # Restart mz
        c.kill("materialized")
        c.up("materialized")

        c.run_testdrive_files(
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            "schema-restart/after-restart.td",
        )


def _make_inserts(*, txns: int, txn_size: int) -> tuple[str, int]:
    sql = "\n".join([f"""
            SET @i:=0;
            INSERT INTO many_inserts (f2) SELECT @i:=@i+1 FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT {txn_size};
            """ for i in range(0, txns)])
    records = txns * txn_size
    return (sql, records)


def workflow_many_inserts(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Tests a scenario that caused a consistency issue in the past. We insert a
    large number of rows into a table, then create a source for that table while
    simultaneously writing to the table in a background thread, then finally
    verify that the exact count and checksum of rows is captured by the source.

    In earlier incarnations of the MySQL source, the source accidentally failed
    to snapshot inside of a repeatable read transaction.

    The source runs on a multi-worker cluster so the snapshot is split into
    per-worker PK ranges, and the background writes mix inserts, updates and
    deletes so every kind of concurrent change races the range reads and the
    subsequent rewind.
    """
    mysql_version = get_targeted_mysql_version(parser)
    with c.override(create_mysql(mysql_version)):
        c.up("materialized", "mysql", Service("testdrive", idle=True))

        # Records to insert before creating the source. Their pk values are
        # deterministic (pk = f2 = 1..initial_records), so the concurrent
        # update and delete below can target pk ranges of these rows while
        # keeping the final checksum exact.
        initial_sql, initial_records = _make_inserts(txns=1, txn_size=1_000_000)

        # Records to insert concurrently with creating the source, with an
        # update and a delete of disjoint pk ranges of the initial rows
        # interleaved between the insert transactions.
        first_sql, first_records = _make_inserts(txns=300, txn_size=100)
        second_sql, second_records = _make_inserts(txns=300, txn_size=100)
        third_sql, third_records = _make_inserts(txns=400, txn_size=100)
        # The indentation matches the _make_inserts blocks so the dedent in
        # do_inserts still finds a common prefix.
        concurrent_sql = "\n".join(
            [
                first_sql,
                "            UPDATE many_inserts SET f2 = f2 + 10 WHERE pk BETWEEN 100001 AND 200000;",
                second_sql,
                "            DELETE FROM many_inserts WHERE pk BETWEEN 1 AND 50000;",
                third_sql,
            ]
        )
        concurrent_records = first_records + second_records + third_records

        expected_count = initial_records + concurrent_records - 50_000
        expected_sum = (
            # initial rows: f2 = 1..initial_records
            initial_records * (initial_records + 1) // 2
            # each concurrent insert transaction contributes f2 = 1..100
            + (concurrent_records // 100) * (100 * 101 // 2)
            # update delta
            + 10 * 100_000
            # deleted rows: f2 = pk = 1..50000
            - 50_000 * 50_001 // 2
        )

        # Set up the MySQL server with the initial records, set up the connection to
        # the MySQL server in Materialize.
        c.testdrive(
            dedent(f"""
                $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER SYSTEM SET max_mysql_connections = 100

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                > CREATE SECRET IF NOT EXISTS mysqlpass AS '{MySql.DEFAULT_ROOT_PASSWORD}'
                > CREATE CONNECTION IF NOT EXISTS mysql_conn TO MYSQL (HOST mysql, USER root, PASSWORD SECRET mysqlpass)

                $ mysql-execute name=mysql
                DROP DATABASE IF EXISTS public;
                CREATE DATABASE public;
                USE public;
                DROP TABLE IF EXISTS many_inserts;
                CREATE TABLE many_inserts (pk SERIAL PRIMARY KEY, f2 BIGINT);
                """)
            + dedent(initial_sql)
            + dedent("""
                > DROP SOURCE IF EXISTS s1 CASCADE;
                """)
        )

    # Start inserting in the background.

    def do_inserts(c: Composition):
        x = dedent(f"""
            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

            $ mysql-execute name=mysql
            USE public;
            {concurrent_sql}
            """)
        c.testdrive(args=["--no-reset"], input=x)

    insert_thread = threading.Thread(target=do_inserts, args=(c,))
    print("--- Start many concurrent inserts")
    insert_thread.start()

    # Create the source on a multi-worker cluster so the snapshot is split
    # into per-worker PK ranges.
    c.testdrive(
        args=["--no-reset"],
        input=dedent("""
            > CREATE CLUSTER many_inserts_cluster SIZE 'scale=1,workers=8'
            > CREATE SOURCE s1
                IN CLUSTER many_inserts_cluster
                FROM MYSQL CONNECTION mysql_conn;
            > CREATE TABLE many_inserts FROM SOURCE s1 (REFERENCE public.many_inserts);
            """),
    )

    # Ensure the source eventually sees the right number of records.
    insert_thread.join()

    print("--- Validate concurrent writes")
    c.testdrive(
        args=["--no-reset"],
        input=dedent(f"""
            > SELECT count(*), sum(f2) FROM many_inserts
            {expected_count} {expected_sum}
            """),
    )


def workflow_snapshot_network_disruption(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Cut the network mid-way through a parallel PK-range snapshot and verify
    that the retried snapshot converges to exact, duplicate-free data.

    The source connects through toxiproxy with a per-connection bandwidth cap
    so the range reads are slow enough to be interrupted reliably. The data is
    loaded into MySQL directly, bypassing the proxy.
    """
    mysql_version = get_targeted_mysql_version(parser)
    rows = 2_000_000
    expected_sum = 7 * rows * (rows + 1) // 2
    with c.override(create_mysql(mysql_version)):
        c.up("materialized", "mysql", "toxiproxy", Service("testdrive", idle=True))

        c.testdrive(
            dedent(f"""
                $ http-request method=POST url=http://toxiproxy:8474/proxies content-type=application/json
                {{
                  "name": "mysql",
                  "listen": "0.0.0.0:3306",
                  "upstream": "mysql:3306",
                  "enabled": true
                }}

                $ http-request method=POST url=http://toxiproxy:8474/proxies/mysql/toxics content-type=application/json
                {{
                  "name": "mysql_bandwidth",
                  "type": "bandwidth",
                  "stream": "downstream",
                  "attributes": {{"rate": 500}}
                }}

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                DROP DATABASE IF EXISTS public;
                CREATE DATABASE public;
                USE public;
                CREATE TABLE ten (f1 INTEGER);
                INSERT INTO ten VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
                CREATE TABLE big_pk (pk BIGINT PRIMARY KEY, f2 BIGINT);
                SET @i := 0;
                INSERT INTO big_pk SELECT @i := @i + 1, @i * 7 FROM ten a1, ten a2, ten a3, ten a4, ten a5, ten a6, ten a7 LIMIT {rows};

                > CREATE SECRET mysqlpass AS '{MySql.DEFAULT_ROOT_PASSWORD}'
                > CREATE CONNECTION mysql_toxi_conn TO MYSQL (
                    HOST toxiproxy,
                    USER root,
                    PASSWORD SECRET mysqlpass
                  )

                > CREATE CLUSTER snapshot_disruption_cluster SIZE 'scale=1,workers=8'
                > CREATE SOURCE big_pk_source
                  IN CLUSTER snapshot_disruption_cluster
                  FROM MYSQL CONNECTION mysql_toxi_conn;
                > CREATE TABLE big_pk FROM SOURCE big_pk_source (REFERENCE public.big_pk);

                # Wait until the PK-range reads are mid-flight.
                > SELECT sum(u.snapshot_records_staged) > 0
                  FROM mz_internal.mz_source_statistics u
                  JOIN mz_objects o ON o.id = u.id
                  WHERE o.name IN ('big_pk_source', 'big_pk');
                true

                # Cut every proxied connection mid-read.
                $ http-request method=POST url=http://toxiproxy:8474/proxies/mysql content-type=application/json
                {{
                  "name": "mysql",
                  "listen": "0.0.0.0:3306",
                  "upstream": "mysql:3306",
                  "enabled": false
                }}

                > SELECT count(*) > 0 FROM mz_internal.mz_source_statuses WHERE error LIKE '%Connection refused%';
                true

                # Restore the network at full speed and expect the snapshot to
                # be retried to exact completion.
                $ http-request method=DELETE url=http://toxiproxy:8474/proxies/mysql/toxics/mysql_bandwidth

                $ http-request method=POST url=http://toxiproxy:8474/proxies/mysql content-type=application/json
                {{
                  "name": "mysql",
                  "listen": "0.0.0.0:3306",
                  "upstream": "mysql:3306",
                  "enabled": true
                }}

                > SELECT count(*), count(DISTINCT pk), sum(f2) FROM big_pk;
                {rows} {rows} {expected_sum}

                > SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'big_pk_source';
                running
                """),
        )


def workflow_snapshot_max_connections(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Several parallel snapshots racing for a max_connections budget far below
    their combined peak demand must all hydrate eventually.

    During setup a parallel snapshot holds up to 2 * workers + 1 upstream
    connections per source (every worker's read connection plus the leader's
    sampling and lock connections), so four workers=8 sources want ~68
    connections at peak while MySQL only grants 25. Sources whose connects are
    rejected restart transiently and must converge as competitors finish and
    release connections.
    """
    mysql_version = get_targeted_mysql_version(parser)
    num_sources = 4
    rows = 200_000
    expected_sum = 3 * rows * (rows + 1) // 2
    with c.override(create_mysql(mysql_version)):
        c.up("materialized", "mysql", Service("testdrive", idle=True))

        setup = [dedent(f"""
                $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER SYSTEM SET max_mysql_connections = 1000

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                SET GLOBAL max_connections = 25;
                DROP DATABASE IF EXISTS public;
                CREATE DATABASE public;
                USE public;
                """)]
        for i in range(num_sources):
            setup.append(dedent(f"""
                    $ mysql-execute name=mysql
                    CREATE TABLE conn_squeeze_{i} (pk BIGINT PRIMARY KEY, f2 BIGINT);
                    SET @i := 0;
                    INSERT INTO conn_squeeze_{i} SELECT @i := @i + 1, @i * 3 FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT {rows};
                    """))
        setup.append(dedent(f"""
                > CREATE SECRET mysqlpass AS '{MySql.DEFAULT_ROOT_PASSWORD}'
                > CREATE CONNECTION mysql_conn TO MYSQL (
                    HOST mysql,
                    USER root,
                    PASSWORD SECRET mysqlpass
                  )
                """))
        for i in range(num_sources):
            setup.append(dedent(f"""
                    > CREATE CLUSTER conn_squeeze_cluster_{i} SIZE 'scale=1,workers=8'
                    > CREATE SOURCE conn_squeeze_source_{i}
                      IN CLUSTER conn_squeeze_cluster_{i}
                      FROM MYSQL CONNECTION mysql_conn;
                    > CREATE TABLE conn_squeeze_{i} FROM SOURCE conn_squeeze_source_{i} (REFERENCE public.conn_squeeze_{i});
                    """))
        validate = [dedent(f"""
                > SELECT count(*), count(DISTINCT pk), sum(f2) FROM conn_squeeze_{i};
                {rows} {rows} {expected_sum}
                """) for i in range(num_sources)]
        validate.append(dedent(f"""
                > SELECT count(*) FROM mz_internal.mz_source_statuses
                  WHERE name LIKE 'conn_squeeze_source_%' AND status = 'running';
                {num_sources}

                $ mysql-execute name=mysql
                SET GLOBAL max_connections = 1000;
                """))
        c.testdrive(
            args=["--default-timeout=600s"],
            input="\n".join(setup + validate),
        )


def workflow_large_scale(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    The goal is to test a large scale MySQL instance and to make sure that we can successfully ingest data from it quickly.
    """
    mysql_version = get_targeted_mysql_version(parser)
    with c.override(create_mysql(mysql_version)):
        c.up("materialized", "mysql", Service("testdrive", idle=True))

        # Set up the MySQL server with the initial records, set up the connection to
        # the MySQL server in Materialize.
        c.testdrive(dedent(f"""
                $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER SYSTEM SET max_mysql_connections = 100

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                > CREATE SECRET IF NOT EXISTS mysqlpass AS '{MySql.DEFAULT_ROOT_PASSWORD}'
                > CREATE CONNECTION IF NOT EXISTS mysql_conn TO MYSQL (HOST mysql, USER root, PASSWORD SECRET mysqlpass)

                $ mysql-execute name=mysql
                DROP DATABASE IF EXISTS public;
                CREATE DATABASE public;
                USE public;
                DROP TABLE IF EXISTS products;
                CREATE TABLE products (id int NOT NULL, name varchar(255) DEFAULT NULL, merchant_id int NOT NULL, price int DEFAULT NULL, status int DEFAULT NULL, created_at timestamp NULL DEFAULT CURRENT_TIMESTAMP(), recordSizePayload longtext, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
                ALTER TABLE products DISABLE KEYS;

                > DROP SOURCE IF EXISTS s1 CASCADE;
                """))

    def make_inserts(c: Composition, start: int, batch_num: int):
        c.testdrive(
            args=["--no-reset"],
            input=dedent(f"""
            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
            $ mysql-execute name=mysql
            SET foreign_key_checks = 0;
            USE public;
            SET @i:={start};
            INSERT INTO products (id, name, merchant_id, price, status, created_at, recordSizePayload) SELECT @i:=@i+1, CONCAT("name", @i), @i % 1000, @i % 1000, @i % 10, '2024-12-12', repeat('x', 1000000) FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT {batch_num};
            """),
        )

    num_rows = 100_000  # out of disk with 200_000 rows
    batch_size = 100
    for i in range(0, num_rows, batch_size):
        batch_num = min(batch_size, num_rows - i)
        make_inserts(c, i, batch_num)

    c.testdrive(
        args=["--no-reset"],
        input=dedent(f"""
            > CREATE SOURCE s1
                FROM MYSQL CONNECTION mysql_conn;
            > CREATE TABLE products FROM SOURCE s1 (REFERENCE public.products);
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


def workflow_source_timeouts(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Test source connect timeout using toxiproxy to drop network traffic.
    """
    mysql_version = get_targeted_mysql_version(parser)
    with c.override(
        Materialized(
            sanity_restart=False,
            additional_system_parameter_defaults={
                "log_filter": "mz_storage::source::mysql=trace,info"
            },
            default_replication_factor=2,
        ),
        Toxiproxy(),
        create_mysql(mysql_version),
    ):
        c.up("materialized", "mysql", "toxiproxy")
        c.run_testdrive_files(
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            "proxied/*.td",
        )
