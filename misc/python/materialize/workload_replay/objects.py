# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Object creation functions for workload replay.
"""

from __future__ import annotations

import re
import time
from textwrap import dedent
from typing import Any

import confluent_kafka  # type: ignore
import psycopg
import pymysql
from confluent_kafka.admin import AdminClient  # type: ignore
from psycopg.sql import SQL, Identifier, Literal

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.helpers.iceberg import (
    create_polaris_namespace,
    get_polaris_access_token,
    setup_polaris_for_iceberg,
)
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.workload_replay.util import (
    get_kafka_topic,
    get_mysql_reference_db_table,
    get_postgres_reference_db_schema_table,
    get_sql_server_reference_db_schema_table,
    to_sql_server_data_type,
)


def run_create_objects_part_1(
    c: Composition, services: set[str], workload: dict[str, Any], verbose: bool
) -> None:
    """Create clusters, databases, schemas, types, connections, and prepare sources."""
    c.sql(
        "DROP CLUSTER IF EXISTS quickstart CASCADE",
        user="mz_system",
        port=6877,
        print_statement=verbose,
    )
    c.sql(
        "DROP DATABASE IF EXISTS materialize CASCADE",
        user="mz_system",
        port=6877,
        print_statement=verbose,
    )
    c.sql(
        "ALTER SYSTEM SET max_schemas_per_database = 1000000",
        user="mz_system",
        port=6877,
        print_statement=verbose,
    )
    c.sql(
        "ALTER SYSTEM SET max_tables = 1000000",
        user="mz_system",
        port=6877,
        print_statement=verbose,
    )
    c.sql(
        "ALTER SYSTEM SET max_materialized_views = 1000000",
        user="mz_system",
        port=6877,
        print_statement=verbose,
    )
    c.sql(
        "ALTER SYSTEM SET max_sources = 1000000",
        user="mz_system",
        port=6877,
        print_statement=verbose,
    )
    c.sql(
        "ALTER SYSTEM SET max_sinks = 1000000",
        user="mz_system",
        port=6877,
        print_statement=verbose,
    )
    c.sql(
        "ALTER SYSTEM SET max_roles = 1000000",
        user="mz_system",
        port=6877,
        print_statement=verbose,
    )
    c.sql(
        "ALTER SYSTEM SET max_clusters = 1000000",
        user="mz_system",
        port=6877,
        print_statement=verbose,
    )
    c.sql(
        "ALTER SYSTEM SET max_replicas_per_cluster = 1000000",
        user="mz_system",
        port=6877,
        print_statement=verbose,
    )
    c.sql(
        "ALTER SYSTEM SET max_secrets = 1000000",
        user="mz_system",
        port=6877,
        print_statement=verbose,
    )
    c.sql(
        "ALTER SYSTEM SET webhook_concurrent_request_limit = 1000000",
        user="mz_system",
        port=6877,
        print_statement=verbose,
    )

    print("Creating clusters")
    for name, cluster in workload["clusters"].items():
        if cluster["managed"]:
            # Need at least one replica for everything to hydrate
            create_sql = cluster["create_sql"].replace(
                "REPLICATION FACTOR = 0", "REPLICATION FACTOR = 1"
            )
            c.sql(create_sql, user="mz_system", port=6877, print_statement=verbose)
        else:
            raise ValueError("Handle unmanaged clusters")

    print("Creating databases")
    for db in workload["databases"]:
        c.sql(
            SQL("CREATE DATABASE {}").format(Identifier(db)),
            user="mz_system",
            port=6877,
            print_statement=verbose,
        )

    print("Creating schemas")
    for db, schemas in workload["databases"].items():
        for schema in schemas:
            if schema == "public":
                continue
            c.sql(
                SQL("CREATE SCHEMA {}.{}").format(Identifier(db), Identifier(schema)),
                user="mz_system",
                port=6877,
                print_statement=verbose,
            )

    print("Creating types")
    for schemas in workload["databases"].values():
        for items in schemas.values():
            for typ in items["types"].values():
                c.sql(
                    typ["create_sql"],
                    user="mz_system",
                    port=6877,
                    print_statement=verbose,
                )

    print("Preparing sources")
    if "postgres" in services:
        c.sql(
            "CREATE DATABASE IF NOT EXISTS materialize;\nCREATE SCHEMA IF NOT EXISTS public;\nCREATE SECRET pgpass AS 'postgres'",
            user="mz_system",
            port=6877,
            print_statement=verbose,
        )

    if "mysql" in services:
        c.sql(
            f"CREATE SECRET mysqlpass AS '{MySql.DEFAULT_ROOT_PASSWORD}'",
            user="mz_system",
            port=6877,
            print_statement=verbose,
        )
        mysql_conn = pymysql.connect(
            host="127.0.0.1",
            user="root",
            password=MySql.DEFAULT_ROOT_PASSWORD,
            port=c.default_port("mysql"),
            autocommit=True,
        )
        with mysql_conn.cursor() as cur:
            cur.execute("DROP DATABASE IF EXISTS `public`")
            cur.execute("CREATE DATABASE `public`")
        mysql_conn.close()

    if "sql-server" in services:
        c.sql(
            f"CREATE SECRET sql_server_pass AS '{SqlServer.DEFAULT_SA_PASSWORD}'",
            user="mz_system",
            port=6877,
            print_statement=verbose,
        )

    has_iceberg = any(
        connection["type"] == "iceberg-catalog"
        for schemas in workload["databases"].values()
        for items in schemas.values()
        for connection in items["connections"].values()
    )
    iceberg_credentials: tuple[str, str] | None = None
    if has_iceberg:
        print("Setting up Polaris for Iceberg sinks")
        c.sql(
            "ALTER SYSTEM SET enable_iceberg_sink = true",
            user="mz_system",
            port=6877,
            print_statement=verbose,
        )
        iceberg_credentials = setup_polaris_for_iceberg(c)
        # Create any additional namespaces referenced by iceberg sinks
        namespaces: set[str] = set()
        for schemas in workload["databases"].values():
            for items in schemas.values():
                for sink in items["sinks"].values():
                    match = re.search(
                        r"NAMESPACE\s*=?\s*'([^']+)'",
                        sink.get("create_sql", ""),
                        re.IGNORECASE,
                    )
                    if match:
                        namespaces.add(match.group(1))
        namespaces.discard("default_namespace")
        if namespaces:
            access_token = get_polaris_access_token(c)
            for ns in namespaces:
                create_polaris_namespace(c, access_token, namespace=ns)

    print("Creating connections")
    pg_conn: psycopg.Connection | None = None
    existing_dbs = {"postgres": {"postgres"}, "sql-server": set()}
    for db, schemas in workload["databases"].items():
        for schema, items in schemas.items():
            for name, connection in items["connections"].items():
                if connection["type"] == "postgres":
                    match = re.search(
                        r"DATABASE\s*=\s*('?)([a-zA-Z_][a-zA-Z0-9_]*|\w+)\1(?=\s*[,\)])",
                        connection["create_sql"],
                        re.IGNORECASE,
                    )
                    assert match, f"No database found in {connection['create_sql']}"
                    ref_database = match.group(2)
                    if ref_database not in existing_dbs["postgres"]:
                        if pg_conn is None:
                            pg_conn = psycopg.connect(
                                host="127.0.0.1",
                                port=c.default_port("postgres"),
                                user="postgres",
                                password="postgres",
                                dbname="postgres",
                                autocommit=True,
                            )
                        with pg_conn.cursor() as cur:
                            cur.execute(
                                SQL("CREATE DATABASE {}").format(
                                    Identifier(ref_database)
                                )
                            )
                        existing_dbs["postgres"].add(ref_database)
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO POSTGRES (HOST postgres, DATABASE {}, PASSWORD SECRET pgpass, USER postgres)"
                        ).format(
                            Identifier(db),
                            Identifier(schema),
                            Identifier(name),
                            Literal(ref_database),
                        ),
                        user="mz_system",
                        port=6877,
                        print_statement=verbose,
                    )
                elif connection["type"] == "mysql":
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO MYSQL (HOST mysql, PASSWORD SECRET mysqlpass, USER root)"
                        ).format(Identifier(db), Identifier(schema), Identifier(name)),
                        user="mz_system",
                        port=6877,
                        print_statement=verbose,
                    )
                elif connection["type"] == "sql-server":
                    match = re.search(
                        r"DATABASE\s*=\s*('?)([a-zA-Z_][a-zA-Z0-9_]*|\w+)\1(?=\s*[,\)])",
                        connection["create_sql"],
                        re.IGNORECASE,
                    )
                    assert match, f"No database found in {connection['create_sql']}"
                    ref_database = match.group(2)
                    if ref_database not in existing_dbs["sql-server"]:
                        c.testdrive(dedent(f"""
                                $ sql-server-connect name=sql-server
                                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}
                                $ sql-server-execute name=sql-server
                                CREATE DATABASE {ref_database};
                                USE test;
                                EXEC sys.sp_cdc_enable_db;
                                ALTER DATABASE {ref_database} SET ALLOW_SNAPSHOT_ISOLATION ON;
                                """))
                        existing_dbs["sql-server"].add(ref_database)
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO SQL SERVER (HOST 'sql-server', PASSWORD SECRET sql_server_pass, USER {}, DATABASE {}, PORT 1433)"
                        ).format(
                            Identifier(db),
                            Identifier(schema),
                            Identifier(name),
                            Identifier(SqlServer.DEFAULT_USER),
                            Identifier(ref_database),
                        ),
                        user="mz_system",
                        port=6877,
                        print_statement=verbose,
                    )
                elif connection["type"] == "kafka":
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO KAFKA (BROKER 'kafka', SECURITY PROTOCOL PLAINTEXT)"
                        ).format(Identifier(db), Identifier(schema), Identifier(name)),
                        user="mz_system",
                        port=6877,
                        print_statement=verbose,
                    )
                elif connection["type"] == "confluent-schema-registry":
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO CONFLUENT SCHEMA REGISTRY (URL 'http://schema-registry:8081')"
                        ).format(Identifier(db), Identifier(schema), Identifier(name)),
                        user="mz_system",
                        port=6877,
                        print_statement=verbose,
                    )
                elif connection["type"] == "ssh-tunnel":
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO SSH TUNNEL (HOST 'ssh-bastion-host', USER 'mz', PORT 22)"
                        ).format(Identifier(db), Identifier(schema), Identifier(name)),
                        user="mz_system",
                        port=6877,
                        print_statement=verbose,
                    )
                elif connection["type"] == "aws":
                    if iceberg_credentials is not None:
                        username, key = iceberg_credentials
                        secret_name = f"{name}_secret"
                        c.sql(
                            SQL("CREATE SECRET {}.{}.{} AS {}").format(
                                Identifier(db),
                                Identifier(schema),
                                Identifier(secret_name),
                                Literal(key),
                            ),
                            user="mz_system",
                            port=6877,
                            print_statement=verbose,
                        )
                        c.sql(
                            SQL(
                                "CREATE CONNECTION {}.{}.{} TO AWS ("
                                "ACCESS KEY ID = {}, "
                                "SECRET ACCESS KEY = SECRET {}.{}.{}, "
                                "ENDPOINT = 'http://minio:9000/', "
                                "REGION = 'us-east-1')"
                            ).format(
                                Identifier(db),
                                Identifier(schema),
                                Identifier(name),
                                Literal(username),
                                Identifier(db),
                                Identifier(schema),
                                Identifier(secret_name),
                            ),
                            user="mz_system",
                            port=6877,
                            print_statement=verbose,
                        )
                    # else: skip, can't run outside of cloud
                elif connection["type"] == "iceberg-catalog":
                    assert (
                        iceberg_credentials is not None
                    ), "Iceberg catalog connection requires polaris service"
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO ICEBERG CATALOG ("
                            "CATALOG TYPE = 'REST', "
                            "URL = 'http://polaris:8181/api/catalog', "
                            "CREDENTIAL = 'root:root', "
                            "WAREHOUSE = 'default_catalog', "
                            "SCOPE = 'PRINCIPAL_ROLE:ALL')"
                        ).format(
                            Identifier(db),
                            Identifier(schema),
                            Identifier(name),
                        ),
                        user="mz_system",
                        port=6877,
                        print_statement=verbose,
                    )
                elif connection["type"] == "aws-privatelink":
                    pass  # can't run outside of cloud
                else:
                    raise ValueError(f"Unhandled connection type {connection['type']}")

    print("Preparing sources")
    # Cache connections to upstream databases to avoid per-DDL overhead.
    pg_source_conns: dict[str, psycopg.Connection] = {}
    mysql_source_conns: dict[str, pymysql.Connection] = {}

    def get_pg_source_conn(database: str) -> psycopg.Connection:
        if database not in pg_source_conns:
            pg_source_conns[database] = c.sql_connection(
                service="postgres",
                user="postgres",
                password="postgres",
                database=database,
                port=5432,
            )
        return pg_source_conns[database]

    def get_mysql_source_conn(database: str) -> pymysql.Connection:
        if database not in mysql_source_conns:
            mysql_source_conns[database] = pymysql.connect(
                host="127.0.0.1",
                user="root",
                password=MySql.DEFAULT_ROOT_PASSWORD,
                database=database,
                port=c.default_port("mysql"),
                autocommit=True,
            )
        return mysql_source_conns[database]

    for schemas in workload["databases"].values():
        for items in schemas.values():
            for name, source in items["sources"].items():
                first = True
                for child in source.get("children", {}).values():
                    if source["type"] == "mysql":
                        ref_database, ref_table = get_mysql_reference_db_table(child)
                        columns = [
                            f"{column['name']} {column['type']} {'NULL' if column['nullable'] else 'NOT NULL'} {'' if column['default'] is None else 'DEFAULT ' + column['default']}"
                            for column in child["columns"]
                        ]
                        if first:
                            init_conn = pymysql.connect(
                                host="127.0.0.1",
                                user="root",
                                password=MySql.DEFAULT_ROOT_PASSWORD,
                                port=c.default_port("mysql"),
                                autocommit=True,
                            )
                            with init_conn.cursor() as cur:
                                cur.execute(f"DROP DATABASE IF EXISTS `{ref_database}`")
                                cur.execute(f"CREATE DATABASE `{ref_database}`")
                                cur.execute(
                                    f"CREATE TABLE `{ref_database}`.dummy (f1 INTEGER)"
                                )
                            init_conn.close()
                            first = False
                        conn = get_mysql_source_conn(ref_database)
                        with conn.cursor() as cur:
                            cur.execute(
                                f"CREATE TABLE `{ref_table}` ({', '.join(columns)})"
                            )
                    elif source["type"] == "postgres":
                        ref_database, ref_schema, ref_table = (
                            get_postgres_reference_db_schema_table(child)
                        )
                        match = re.search(
                            r"\bPUBLICATION\s*=\s*'?(?P<pub>[A-Za-z_][A-Za-z0-9_]*)'?",
                            source["create_sql"],
                        )
                        assert match, f"Publication not found in {source}"
                        publication = match.group(1)
                        columns = [
                            f"{column['name']} {column['type']} {'NULL' if column['nullable'] else 'NOT NULL'} DEFAULT {'NULL' if column['default'] is None else column['default']}"
                            for column in child["columns"]
                        ]
                        conn = get_pg_source_conn(ref_database)
                        with conn.cursor() as cur:
                            if first:
                                cur.execute(SQL("ALTER USER postgres WITH replication"))
                                cur.execute(
                                    SQL("DROP PUBLICATION IF EXISTS {}").format(
                                        Identifier(publication)
                                    )
                                )
                                cur.execute(
                                    SQL("CREATE PUBLICATION {} FOR ALL TABLES").format(
                                        Identifier(publication)
                                    )
                                )
                                first = False
                            cur.execute(
                                SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                                    Identifier(ref_schema)
                                )
                            )
                            cur.execute(
                                f'CREATE TABLE "{ref_schema}"."{ref_table}" ({", ".join(columns)})'.encode()
                            )
                            cur.execute(
                                SQL("ALTER TABLE {}.{} REPLICA IDENTITY FULL").format(
                                    Identifier(ref_schema), Identifier(ref_table)
                                )
                            )
                    elif source["type"] == "sql-server":
                        ref_database, ref_schema, ref_table = (
                            get_sql_server_reference_db_schema_table(child)
                        )
                        columns = [
                            f"{column['name']} {to_sql_server_data_type(column['type'])} {'NULL' if column['nullable'] else 'NOT NULL'} DEFAULT {'NULL' if column['default'] is None else column['default']}"
                            for column in child["columns"]
                        ]
                        if first:
                            c.testdrive(dedent(f"""
                                    $ sql-server-connect name=sql-server
                                    server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                                    $ sql-server-execute name=sql-server
                                    IF DB_ID(N'{ref_database}') IS NULL BEGIN CREATE DATABASE {ref_database}; END
                                    USE {ref_database};
                                    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{ref_schema}')BEGIN EXEC(N'CREATE SCHEMA {ref_schema}'); END
                                    """))
                            first = False
                        c.testdrive(dedent(f"""
                            $ sql-server-connect name=sql-server
                            server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                            $ sql-server-execute name=sql-server
                            USE {ref_database};
                            CREATE TABLE "{ref_schema}"."{ref_table}" ({", ".join(columns)});
                            EXEC sys.sp_cdc_enable_table @source_schema = '{ref_schema}', @source_name = '{ref_table}', @role_name = 'SA', @supports_net_changes = 0;
                                """))

                if source["type"] == "kafka":
                    topic = get_kafka_topic(source)
                    kafka_conf: dict[str, str | int | float | bool] = {
                        "bootstrap.servers": f"127.0.0.1:{c.default_port('kafka')}"
                    }
                    admin_client = AdminClient(kafka_conf)
                    admin_client.create_topics(
                        [
                            confluent_kafka.admin.NewTopic(  # type: ignore
                                topic, num_partitions=1, replication_factor=1
                            )
                        ]
                    )

                    # Have to wait for topics to be created before creating sources/tables using them
                    while True:
                        md = admin_client.list_topics(timeout=2)
                        if topic in md.topics and md.topics[topic].error is None:
                            break
                        print(f"Waiting for topic: {topic}")
                        time.sleep(1)

                if source["type"] == "webhook":
                    # Checking secrets makes ingestion into webhooks difficult, remove the check instead
                    source["create_sql"] = re.sub(
                        r"\s*CHECK\s*\(.*?\)\s*;",
                        "",
                        source["create_sql"],
                        flags=re.DOTALL | re.IGNORECASE,
                    )

    for conn in pg_source_conns.values():
        conn.close()
    for conn in mysql_source_conns.values():
        conn.close()
    if pg_conn is not None:
        pg_conn.close()

    return


def run_create_objects_part_2(
    c: Composition, services: set[str], workload: dict[str, Any], verbose: bool
) -> None:
    """Create sources, tables, views, materialized views, indexes, and sinks."""
    print("Creating sources")
    for schemas in workload["databases"].values():
        for items in schemas.values():
            for name, source in items["sources"].items():
                c.sql(
                    source["create_sql"],
                    user="mz_system",
                    port=6877,
                    print_statement=verbose,
                )

                for child in source.get("children", {}).values():
                    if child["type"] == "table":
                        # TODO: Remove when https://github.com/MaterializeInc/database-issues/issues/10034 is fixed
                        create_sql = re.sub(
                            r",?\s*DETAILS\s*=\s*'[^']*'",
                            "",
                            child["create_sql"],
                            flags=re.IGNORECASE,
                        )
                        create_sql = re.sub(
                            r"\sWITH \(\s*\)", "", create_sql, flags=re.IGNORECASE
                        )
                        if source["type"] == "load-generator":
                            # TODO: Remove when https://github.com/MaterializeInc/database-issues/issues/10010 is fixed
                            create_sql = re.sub(
                                r"mz_load_generators\.",
                                "",
                                create_sql,
                                flags=re.IGNORECASE,
                            )
                        create_sql = re.sub(
                            r"\s*\(.*\)\s+FROM\s+",
                            " FROM ",
                            create_sql,
                            flags=re.IGNORECASE | re.DOTALL,
                        )
                        c.sql(
                            create_sql,
                            user="mz_system",
                            port=6877,
                            print_statement=verbose,
                        )

    print("Creating tables")
    for schemas in workload["databases"].values():
        for items in schemas.values():
            for table in items["tables"].values():
                c.sql(
                    table["create_sql"],
                    user="mz_system",
                    port=6877,
                    print_statement=verbose,
                )

    print("Creating views, materialized views, sinks")

    # Collect all object names that already exist (sources, tables, and their children).
    known_names: set[str] = set()
    for db_name, schemas in workload["databases"].items():
        for schema_name, items in schemas.items():
            for name in items.get("tables", {}):
                known_names.add(f"{db_name}.{schema_name}.{name}")
            for name, source in items.get("sources", {}).items():
                known_names.add(f"{db_name}.{schema_name}.{name}")
                for child_name in source.get("children", {}):
                    known_names.add(f"{db_name}.{schema_name}.{child_name}")

    # Build {fqn: create_sql} for objects to create, and compute dependencies
    # by scanning the SQL text for references to known or peer object names.
    to_create: dict[str, str] = {}
    for db_name, schemas in workload["databases"].items():
        for schema_name, items in schemas.items():
            for obj_type in ("views", "materialized_views", "sinks"):
                for name, obj in items.get(obj_type, {}).items():
                    fqn = f"{db_name}.{schema_name}.{name}"
                    to_create[fqn] = obj["create_sql"]
                    known_names.add(fqn)

    deps: dict[str, set[str]] = {}
    for fqn, sql in to_create.items():
        deps[fqn] = {other for other in known_names if other != fqn and other in sql}

    # Topological sort: create objects whose dependencies are already satisfied first.
    created = known_names - set(to_create.keys())
    ordered: list[str] = []
    remaining = set(to_create.keys())
    while remaining:
        batch = {n for n in remaining if deps[n].issubset(created)}
        if not batch:
            break
        ordered.extend(sorted(batch))
        created |= batch
        remaining -= batch

    # Any leftovers (cycles or missed dependencies) go at the end.
    ordered.extend(sorted(remaining))

    # Execute in dependency order, falling back to retry for any edge cases.
    pending = []
    for fqn in ordered:
        try:
            c.sql(to_create[fqn], user="mz_system", port=6877, print_statement=verbose)
        except psycopg.Error as e:
            if "unknown catalog item" in str(e):
                pending.append(to_create[fqn])
                continue
            raise

    # Retry any that failed due to missed dependencies.
    while pending:
        progress = False
        for create in pending.copy():
            try:
                c.sql(create, user="mz_system", port=6877, print_statement=verbose)
            except psycopg.Error as e:
                if "unknown catalog item" in str(e):
                    continue
                raise
            pending.remove(create)
            progress = True
        if not progress:
            raise RuntimeError(f"No progress, remaining creates: {pending}")

    print("Creating indexes")
    for schemas in workload["databases"].values():
        for items in schemas.values():
            for index in items["indexes"].values():
                c.sql(
                    index["create_sql"],
                    user="mz_system",
                    port=6877,
                    print_statement=verbose,
                )
