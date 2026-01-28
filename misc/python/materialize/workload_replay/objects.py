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
from confluent_kafka.admin import AdminClient  # type: ignore
from psycopg.sql import SQL, Identifier, Literal

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.workload_replay.util import (
    get_kafka_topic,
    get_mysql_reference_db_table,
    get_postgres_reference_db_schema_table,
    get_sql_server_reference_db_schema_table,
    to_sql_server_data_type,
)


def run_create_objects_part_1(
    c: Composition, services: set[str], workload: dict[str, Any]
) -> None:
    """Create clusters, databases, schemas, types, connections, and prepare sources."""
    c.sql("DROP CLUSTER IF EXISTS quickstart CASCADE", user="mz_system", port=6877)
    c.sql("DROP DATABASE IF EXISTS materialize CASCADE", user="mz_system", port=6877)
    c.sql(
        "ALTER SYSTEM SET max_schemas_per_database = 1000000",
        user="mz_system",
        port=6877,
    )
    c.sql("ALTER SYSTEM SET max_tables = 1000000", user="mz_system", port=6877)
    c.sql(
        "ALTER SYSTEM SET max_materialized_views = 1000000", user="mz_system", port=6877
    )
    c.sql("ALTER SYSTEM SET max_sources = 1000000", user="mz_system", port=6877)
    c.sql("ALTER SYSTEM SET max_sinks = 1000000", user="mz_system", port=6877)
    c.sql("ALTER SYSTEM SET max_roles = 1000000", user="mz_system", port=6877)
    c.sql("ALTER SYSTEM SET max_clusters = 1000000", user="mz_system", port=6877)
    c.sql(
        "ALTER SYSTEM SET max_replicas_per_cluster = 1000000",
        user="mz_system",
        port=6877,
    )
    c.sql("ALTER SYSTEM SET max_secrets = 1000000", user="mz_system", port=6877)
    c.sql(
        "ALTER SYSTEM SET webhook_concurrent_request_limit = 1000000",
        user="mz_system",
        port=6877,
    )

    print("Creating clusters")
    for name, cluster in workload["clusters"].items():
        if cluster["managed"]:
            # Need at least one replica for everything to hydrate
            create_sql = cluster["create_sql"].replace(
                "REPLICATION FACTOR = 0", "REPLICATION FACTOR = 1"
            )
            c.sql(create_sql, user="mz_system", port=6877)
        else:
            raise ValueError("Handle unmanaged clusters")

    print("Creating databases")
    for db in workload["databases"]:
        c.sql(
            SQL("CREATE DATABASE {}").format(Identifier(db)),
            user="mz_system",
            port=6877,
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
            )

    print("Creating types")
    for schemas in workload["databases"].values():
        for items in schemas.values():
            for typ in items["types"].values():
                c.sql(typ["create_sql"], user="mz_system", port=6877)

    print("Preparing sources")
    if "postgres" in services:
        c.testdrive(
            dedent(
                """
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            CREATE DATABASE IF NOT EXISTS materialize;
            CREATE SCHEMA IF NOT EXISTS public;
            CREATE SECRET pgpass AS 'postgres'
            """
            )
        )

    if "mysql" in services:
        c.testdrive(
            dedent(
                """
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

            $ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}
            $ mysql-execute name=mysql
            DROP DATABASE IF EXISTS public;
            CREATE DATABASE public;
            """
            )
        )

    if "sql-server" in services:
        c.testdrive(
            dedent(
                f"""
            $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
            CREATE SECRET sql_server_pass AS '{SqlServer.DEFAULT_SA_PASSWORD}'
                """
            )
        )

    print("Creating connections")
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
                        c.testdrive(
                            dedent(
                                f"""
                                $ postgres-execute connection=postgres://postgres:postgres@postgres
                                CREATE DATABASE {ref_database};
                                """
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
                    )
                elif connection["type"] == "mysql":
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO MYSQL (HOST mysql, PASSWORD SECRET mysqlpass, USER root)"
                        ).format(Identifier(db), Identifier(schema), Identifier(name)),
                        user="mz_system",
                        port=6877,
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
                        c.testdrive(
                            dedent(
                                f"""
                                $ sql-server-connect name=sql-server
                                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}
                                $ sql-server-execute name=sql-server
                                CREATE DATABASE {ref_database};
                                USE test;
                                EXEC sys.sp_cdc_enable_db;
                                ALTER DATABASE {ref_database} SET ALLOW_SNAPSHOT_ISOLATION ON;
                                """
                            )
                        )
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
                    )
                elif connection["type"] == "kafka":
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO KAFKA (BROKER 'kafka', SECURITY PROTOCOL PLAINTEXT)"
                        ).format(Identifier(db), Identifier(schema), Identifier(name)),
                        user="mz_system",
                        port=6877,
                    )
                elif connection["type"] == "confluent-schema-registry":
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO CONFLUENT SCHEMA REGISTRY (URL 'http://schema-registry:8081')"
                        ).format(Identifier(db), Identifier(schema), Identifier(name)),
                        user="mz_system",
                        port=6877,
                    )
                elif connection["type"] == "ssh-tunnel":
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO SSH TUNNEL (HOST 'ssh-bastion-host', USER 'mz', PORT 22)"
                        ).format(Identifier(db), Identifier(schema), Identifier(name)),
                        user="mz_system",
                        port=6877,
                    )
                elif connection["type"] in ("aws-privatelink", "aws"):
                    pass  # can't run outside of cloud
                else:
                    raise ValueError(f"Unhandled connection type {connection['type']}")

    print("Preparing sources")
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
                            c.testdrive(
                                dedent(
                                    f"""
                                    $ mysql-connect name=mysql url=mysql://root@mysql password=${{arg.mysql-root-password}}
                                    $ mysql-execute name=mysql
                                    DROP DATABASE IF EXISTS `{ref_database}`;
                                    CREATE DATABASE `{ref_database}`;
                                    CREATE TABLE `{ref_database}`.dummy (f1 INTEGER);
                                    """
                                )
                            )
                            first = False
                        c.testdrive(
                            dedent(
                                f"""
                            $ mysql-connect name=mysql url=mysql://root@mysql password=${{arg.mysql-root-password}}
                            $ mysql-execute name=mysql
                            CREATE TABLE `{ref_database}`.`{ref_table}` ({", ".join(columns)});
                                """
                            )
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
                        if first:
                            c.testdrive(
                                dedent(
                                    f"""
                                    $ postgres-execute connection=postgres://postgres:postgres@postgres/{ref_database}
                                    ALTER USER postgres WITH replication;

                                    DROP PUBLICATION IF EXISTS "{publication}";
                                    CREATE PUBLICATION "{publication}" FOR ALL TABLES;
                                    """
                                )
                            )
                            first = False
                        c.testdrive(
                            dedent(
                                f"""
                            $ postgres-execute connection=postgres://postgres:postgres@postgres/{ref_database}
                            CREATE SCHEMA IF NOT EXISTS "{ref_schema}";
                            CREATE TABLE "{ref_schema}"."{ref_table}" ({", ".join(columns)});
                            ALTER TABLE "{ref_schema}"."{ref_table}" REPLICA IDENTITY FULL;
                                """
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
                            c.testdrive(
                                dedent(
                                    f"""
                                    $ sql-server-connect name=sql-server
                                    server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                                    $ sql-server-execute name=sql-server
                                    IF DB_ID(N'{ref_database}') IS NULL BEGIN CREATE DATABASE {ref_database}; END
                                    USE {ref_database};
                                    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{ref_schema}')BEGIN EXEC(N'CREATE SCHEMA {ref_schema}'); END
                                    """
                                )
                            )
                            first = False
                        c.testdrive(
                            dedent(
                                f"""
                            $ sql-server-connect name=sql-server
                            server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                            $ sql-server-execute name=sql-server
                            USE {ref_database};
                            CREATE TABLE "{ref_schema}"."{ref_table}" ({", ".join(columns)});
                            EXEC sys.sp_cdc_enable_table @source_schema = '{ref_schema}', @source_name = '{ref_table}', @role_name = 'SA', @supports_net_changes = 0;
                                """
                            )
                        )

                if source["type"] == "kafka":
                    topic = get_kafka_topic(source)
                    kafka_conf = {
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


def run_create_objects_part_2(
    c: Composition, services: set[str], workload: dict[str, Any]
) -> None:
    """Create sources, tables, views, materialized views, and sinks."""
    print("Creating sources")
    for schemas in workload["databases"].values():
        for items in schemas.values():
            for name, source in items["sources"].items():
                c.sql(source["create_sql"], user="mz_system", port=6877)

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
                        c.sql(create_sql, user="mz_system", port=6877)

    print("Creating tables")
    for schemas in workload["databases"].values():
        for items in schemas.values():
            for table in items["tables"].values():
                c.sql(table["create_sql"], user="mz_system", port=6877)

    print("Creating view, materialized views, sinks")
    pending = set()
    for schemas in workload["databases"].values():
        for items in schemas.values():
            for view in items["views"].values():
                pending.add(view["create_sql"])
            for mv in items["materialized_views"].values():
                pending.add(mv["create_sql"])
            for sink in items["sinks"].values():
                pending.add(sink["create_sql"])

    # TODO: Handle sink -> source roundtrips: scan for topic names to create a dependency graph
    while pending:
        progress = False
        for create in pending.copy():
            try:
                c.sql(create, user="mz_system", port=6877)
            except psycopg.Error as e:
                if "unknown catalog item" in str(e):
                    continue
                raise
            pending.remove(create)
            progress = True
        if not progress:
            raise RuntimeError(f"No progress, remaining creates: {pending}")
