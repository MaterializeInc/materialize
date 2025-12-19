# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import sys
import time
from datetime import datetime, timezone
from typing import Any, LiteralString

import psycopg
import yaml
from psycopg.sql import SQL, Composed, Identifier, Literal


def query(
    conn: psycopg.Connection, sql: LiteralString | bytes | SQL | Composed
) -> list[Any]:
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def attach_source_statistics(
    conn: psycopg.Connection,
    source: dict[str, Any],
    start_time: float,
    end_time: float,
) -> None:
    source_id = source["id"]
    with conn.cursor() as cur:
        cur.execute("SET CLUSTER = mz_catalog_server")
        sql = SQL(
            """
            SUBSCRIBE (
                SELECT
                    messages_received,
                    bytes_received
                FROM mz_internal.mz_source_statistics_with_history
                WHERE id = {}
            )
            WITH (PROGRESS) AS OF AT LEAST TIMESTAMP {}"""
        ).format(
            Literal(source_id),
            Literal(datetime.fromtimestamp(start_time, tz=timezone.utc)),
        )
        first_timestamp = None
        for (
            mz_timestamp,
            mz_progress,
            mz_diff,
            messages_received,
            bytes_received,
        ) in cur.stream(sql):
            if mz_diff == -1:
                continue
            if mz_progress:
                if mz_timestamp / 1000 > end_time + 60:
                    break
                continue
            if "bytes_total" not in source:
                first_timestamp = datetime.fromtimestamp(
                    int(mz_timestamp / 1000), tz=timezone.utc
                )
                source["bytes_total"] = int(bytes_received)
                source["messages_total"] = int(messages_received)
            if mz_timestamp / 1000 > end_time:
                assert "bytes_total" in source and "messages_total" in source
                assert first_timestamp
                duration = (
                    datetime.fromtimestamp(int(mz_timestamp / 1000), tz=timezone.utc)
                    - first_timestamp
                ).total_seconds()
                if duration:
                    if int(bytes_received) > source["bytes_total"]:
                        source["bytes_second"] = (
                            int(bytes_received) - source["bytes_total"]
                        ) / duration
                    source["bytes_total"] = int(bytes_received)
                    if int(messages_received) > source["messages_total"]:
                        source["messages_second"] = (
                            int(messages_received) - source["messages_total"]
                        ) / duration
                    source["messages_total"] = int(messages_received)
                    break


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="mz-workload-record",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Records a workload profile from a running Materialize instance",
    )

    parser.add_argument("mz_url", type=str)
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="Path to write the workload.yml, - for stdout",
        default=f"workload_{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%S')}.yml",
    )
    parser.add_argument(
        "--time",
        type=int,
        help="How long to record the workload, in seconds",
        default=360,
    )
    parser.add_argument("--expensive", action=argparse.BooleanOptionalAction)
    parser.add_argument("--anonymize", action=argparse.BooleanOptionalAction)

    args = parser.parse_args()

    conn = psycopg.connect(args.mz_url)
    conn.autocommit = True

    workload = {
        "databases": {},
        "clusters": {},
        "queries": [],
        "mz_workload_version": "1.0.0",
    }

    print("Fetching databases", file=sys.stderr)
    for (db,) in query(conn, "SELECT name FROM mz_databases"):
        workload["databases"][db] = {}

    print("Fetching schemas", file=sys.stderr)
    for schema, db in query(
        conn,
        "SELECT mz_schemas.name, mz_databases.name FROM mz_schemas JOIN mz_databases ON mz_schemas.database_id = mz_databases.id",
    ):
        workload["databases"][db][schema] = {
            "tables": {},
            "views": {},
            "materialized_views": {},
            "indexes": {},
            "types": {},
            "connections": {},
            "sources": {},
            "sinks": {},
        }

    print("Fetching views", file=sys.stderr)
    for view, schema, db in query(
        conn,
        "SELECT mz_views.name, mz_schemas.name, mz_databases.name FROM mz_views JOIN mz_schemas ON mz_views.schema_id = mz_schemas.id JOIN mz_databases ON mz_schemas.database_id = mz_databases.id",
    ):
        create_sql = query(
            conn,
            SQL("SELECT create_sql FROM (SHOW CREATE VIEW {}.{}.{})").format(
                Identifier(db), Identifier(schema), Identifier(view)
            ),
        )[0][0]
        workload["databases"][db][schema]["views"][view] = {"create_sql": create_sql}

    print("Fetching materialized views", file=sys.stderr)
    for mv, schema, db in query(
        conn,
        "SELECT mz_materialized_views.name, mz_schemas.name, mz_databases.name FROM mz_materialized_views JOIN mz_schemas ON mz_materialized_views.schema_id = mz_schemas.id JOIN mz_databases ON mz_schemas.database_id = mz_databases.id",
    ):
        create_sql = query(
            conn,
            SQL(
                "SELECT create_sql FROM (SHOW CREATE MATERIALIZED VIEW {}.{}.{})"
            ).format(Identifier(db), Identifier(schema), Identifier(mv)),
        )[0][0]
        workload["databases"][db][schema]["materialized_views"][mv] = {
            "create_sql": create_sql
        }

    print("Fetching data types", file=sys.stderr)
    for typ, schema, db in query(
        conn,
        "SELECT mz_types.name, mz_schemas.name, mz_databases.name FROM mz_types JOIN mz_schemas ON mz_types.schema_id = mz_schemas.id JOIN mz_databases ON mz_schemas.database_id = mz_databases.id",
    ):
        create_sql = query(
            conn,
            SQL("SELECT create_sql FROM (SHOW CREATE TYPE {}.{}.{})").format(
                Identifier(db), Identifier(schema), Identifier(typ)
            ),
        )[0][0]
        workload["databases"][db][schema]["types"][typ] = {"create_sql": create_sql}

    print("Fetching connections", file=sys.stderr)
    for connection, schema, db, typ in query(
        conn,
        "SELECT mz_connections.name, mz_schemas.name, mz_databases.name, mz_connections.type FROM mz_connections JOIN mz_schemas ON mz_connections.schema_id = mz_schemas.id JOIN mz_databases ON mz_schemas.database_id = mz_databases.id",
    ):
        create_sql = query(
            conn,
            SQL("SELECT create_sql FROM (SHOW CREATE CONNECTION {}.{}.{})").format(
                Identifier(db), Identifier(schema), Identifier(connection)
            ),
        )[0][0]
        workload["databases"][db][schema]["connections"][connection] = {
            "create_sql": create_sql,
            "type": typ,
        }

    print("Fetching sources", file=sys.stderr)
    for source_id, source, schema, db, typ in query(
        conn,
        "SELECT mz_sources.id, mz_sources.name, mz_schemas.name, mz_databases.name, mz_sources.type FROM mz_sources JOIN mz_schemas ON mz_sources.schema_id = mz_schemas.id JOIN mz_databases ON mz_schemas.database_id = mz_databases.id WHERE type not in ('subsource', 'progress')",
    ):
        create_sql = query(
            conn,
            SQL("SELECT create_sql FROM (SHOW CREATE SOURCE {}.{}.{})").format(
                Identifier(db), Identifier(schema), Identifier(source)
            ),
        )[0][0]
        obj = {
            "create_sql": create_sql,
            "type": typ,
            "id": source_id,
        }
        columns = []
        for column, nullable, column_type, default in query(
            conn,
            SQL(
                "SELECT name, nullable, type, default FROM mz_columns WHERE id = {} ORDER BY position"
            ).format(Literal(source_id)),
        ):
            columns.append(
                {
                    "name": column,
                    "nullable": nullable,
                    "type": column_type,
                    "default": default,
                }
            )
        if columns:
            obj["columns"] = columns
        workload["databases"][db][schema]["sources"][source] = obj

    print("Fetching subsources", file=sys.stderr)
    for (
        subsource_id,
        subsource,
        schema,
        db,
        typ,
        source,
        source_schema,
        source_db,
    ) in query(
        conn,
        "SELECT mz_sources.id, mz_sources.name, mz_schemas.name, mz_databases.name, mz_sources.type, source.name, mz_schemas2.name, mz_databases2.name FROM mz_sources JOIN mz_schemas ON mz_sources.schema_id = mz_schemas.id JOIN mz_databases ON mz_schemas.database_id = mz_databases.id JOIN mz_internal.mz_object_dependencies ON object_id = mz_sources.id JOIN mz_sources AS source ON referenced_object_id = source.id JOIN mz_schemas AS mz_schemas2 ON source.schema_id = mz_schemas2.id JOIN mz_databases AS mz_databases2 ON mz_schemas2.database_id = mz_databases2.id WHERE mz_sources.type = 'subsource'",
    ):
        create_sql = query(
            conn,
            SQL("SELECT create_sql FROM (SHOW CREATE SOURCE {}.{}.{})").format(
                Identifier(db), Identifier(schema), Identifier(subsource)
            ),
        )[0][0]
        if args.expensive:
            rows = query(
                conn,
                SQL("SELECT count(*) FROM {}.{}.{}").format(
                    Identifier(db), Identifier(schema), Identifier(subsource)
                ),
            )[0][0]
        else:
            rows = 0
        columns = []
        obj = {
            "create_sql": create_sql,
            "name": subsource,
            "schema": schema,
            "database": db,
            "id": subsource_id,
            "rows": rows,
            "type": "subsource",
        }

        for column, nullable, column_type, default in query(
            conn,
            SQL(
                "SELECT name, nullable, type, default FROM mz_columns WHERE id = {} ORDER BY position"
            ).format(Literal(subsource_id)),
        ):
            columns.append(
                {
                    "name": column,
                    "nullable": nullable,
                    "type": column_type,
                    "default": default,
                }
            )

            if rows > 0:
                if args.expensive:
                    avg_size = query(
                        conn,
                        SQL("SELECT avg(pg_column_size({})) FROM {}.{}.{}").format(
                            Identifier(column),
                            Identifier(db),
                            Identifier(schema),
                            Identifier(subsource),
                        ),
                    )[0][0]
                    columns[-1]["avg_size"] = (
                        int(avg_size) if avg_size is not None else None
                    )
                else:
                    # TODO: This goes OoM, can we sample better?
                    # avg_size = query(
                    #     conn,
                    #     SQL(
                    #         "SELECT avg(pg_column_size({})) FROM (SELECT {} FROM {}.{}.{} LIMIT 100)"
                    #     ).format(
                    #         Identifier(column),
                    #         Identifier(column),
                    #         Identifier(db),
                    #         Identifier(schema),
                    #         Identifier(subsource),
                    #     ),
                    # )[0][0]
                    pass

        if columns:
            obj["columns"] = columns

        workload["databases"][source_db][source_schema]["sources"][source].setdefault(
            "children", {}
        )[f"{db}.{schema}.{subsource}"] = obj

    print("Fetching tables", file=sys.stderr)
    for table, schema, db, id in query(
        conn,
        "SELECT mz_tables.name, mz_schemas.name, mz_databases.name, mz_tables.id FROM mz_tables JOIN mz_schemas ON mz_tables.schema_id = mz_schemas.id JOIN mz_databases ON mz_schemas.database_id = mz_databases.id",
    ):
        create_sql = query(
            conn,
            SQL("SELECT create_sql FROM (SHOW CREATE TABLE {}.{}.{})").format(
                Identifier(db), Identifier(schema), Identifier(table)
            ),
        )[0][0]
        if args.expensive:
            rows = query(
                conn,
                SQL("SELECT count(*) FROM {}.{}.{}").format(
                    Identifier(db), Identifier(schema), Identifier(table)
                ),
            )[0][0]
        else:
            rows = 0
        columns = []
        for column, typ, nullable, default in query(
            conn,
            SQL(
                "SELECT c.name, c.type, c.nullable, c.default from mz_internal.mz_object_fully_qualified_names AS ofqn JOIN mz_columns AS c ON c.id = ofqn.id WHERE ofqn.name = {} and ofqn.schema_name = {} and ofqn.database_name = {} ORDER BY c.position"
            ).format(Literal(table), Literal(schema), Literal(db)),
        ):
            columns.append(
                {"name": column, "type": typ, "nullable": nullable, "default": default}
            )
            if rows > 0:
                if args.expensive:
                    avg_size = query(
                        conn,
                        SQL("SELECT avg(pg_column_size({})) FROM {}.{}.{}").format(
                            Identifier(column),
                            Identifier(db),
                            Identifier(schema),
                            Identifier(table),
                        ),
                    )[0][0]
                    columns[-1]["avg_size"] = (
                        int(avg_size) if avg_size is not None else None
                    )
                else:
                    # TODO: This goes OoM, can we sample better?
                    # avg_size = query(
                    #     conn,
                    #     SQL(
                    #         "SELECT avg(pg_column_size({})) FROM (SELECT {} FROM {}.{}.{} LIMIT 100)"
                    #     ).format(
                    #         Identifier(column),
                    #         Identifier(column),
                    #         Identifier(db),
                    #         Identifier(schema),
                    #         Identifier(table),
                    #     ),
                    # )[0][0]
                    pass

        obj = {
            "create_sql": create_sql,
            "rows": rows,
            "columns": columns,
            "id": id,
        }

        if "FROM SOURCE" in create_sql:
            source, source_schema, source_db = query(
                conn,
                SQL(
                    "SELECT mz_sources.name, mz_schemas.name, mz_databases.name FROM mz_internal.mz_object_dependencies JOIN mz_sources ON referenced_object_id = mz_sources.id JOIN mz_schemas ON mz_sources.schema_id = mz_schemas.id JOIN mz_databases ON mz_schemas.database_id = mz_databases.id WHERE object_id = {}"
                ).format(Literal(id)),
            )[0]
            obj["type"] = "table"
            obj["schema"] = schema
            obj["database"] = db
            obj["name"] = table
            workload["databases"][source_db][source_schema]["sources"][
                source
            ].setdefault("children", {})[f"{db}.{schema}.{table}"] = obj
        else:
            workload["databases"][db][schema]["tables"][table] = obj

    print("Fetching sinks", file=sys.stderr)
    for sink, schema, db, typ in query(
        conn,
        "SELECT mz_sinks.name, mz_schemas.name, mz_databases.name, mz_sinks.type FROM mz_sinks JOIN mz_schemas ON mz_sinks.schema_id = mz_schemas.id JOIN mz_databases ON mz_schemas.database_id = mz_databases.id",
    ):
        create_sql = query(
            conn,
            SQL("SELECT create_sql FROM (SHOW CREATE SINK {}.{}.{})").format(
                Identifier(db), Identifier(schema), Identifier(sink)
            ),
        )[0][0]
        workload["databases"][db][schema]["sinks"][sink] = {
            "create_sql": create_sql,
            "type": typ,
        }

    print("Fetching indexes", file=sys.stderr)
    for index, schema, database in query(
        conn,
        "SELECT mz_indexes.name, schema_name, database_name FROM mz_indexes JOIN mz_internal.mz_object_fully_qualified_names AS ofqn ON on_id = ofqn.id WHERE schema_name NOT IN ('mz_catalog', 'mz_internal', 'mz_introspection')",
    ):
        create_sql = query(
            conn,
            SQL("SELECT create_sql FROM (SHOW CREATE INDEX {}.{}.{})").format(
                Identifier(database), Identifier(schema), Identifier(index)
            ),
        )[0][0]
        workload["databases"][database][schema]["indexes"][index] = {
            "create_sql": create_sql
        }

    print("Fetching clusters", file=sys.stderr)
    for cluster, managed in query(
        conn, "SELECT name, managed FROM mz_clusters WHERE id LIKE 'u%'"
    ):
        workload["clusters"][cluster] = {"managed": managed}
        if managed:
            workload["clusters"][cluster]["create_sql"] = query(
                conn,
                SQL("SELECT create_sql FROM (SHOW CREATE CLUSTER {})").format(
                    Identifier(cluster)
                ),
            )[0][0]

    end_time = time.time()
    start_time = end_time - args.time

    print("Fetching queries", file=sys.stderr)
    for (
        sql,
        cluster,
        database,
        search_path,
        statement_type,
        finished_status,
        params,
        transaction_isolation,
        began_at,
        duration,
        result_size,
    ) in query(
        conn,
        SQL(
            "SELECT sql, cluster_name, database_name, search_path, statement_type, finished_status, params, transaction_isolation, began_at, finished_at - began_at, result_size FROM mz_internal.mz_recent_activity_log WHERE began_at > {} ORDER BY began_at ASC"
        ).format(Literal(datetime.fromtimestamp(start_time, tz=timezone.utc))),
    ):
        assert (
            search_path[0] == "{" and search_path[-1] == "}"
        ), f"Unexpected search path: {search_path}"
        workload["queries"].append(
            {
                "sql": sql,
                "cluster": cluster,
                "database": database,
                "search_path": search_path[1:-1].split(","),
                "statement_type": statement_type,
                "finished_status": finished_status,
                "params": params,
                "transaction_isolation": transaction_isolation,
                "began_at": began_at,
                "duration": duration.total_seconds() if duration is not None else None,
                "result_size": result_size,
            }
        )

    print("Fetching source/subsource/table statistics", file=sys.stderr)
    for schemas in workload["databases"].values():
        for items in schemas.values():
            for source in items["sources"].values():
                attach_source_statistics(conn, source, start_time, end_time)
                for child in source.get("children", {}).values():
                    attach_source_statistics(conn, child, start_time, end_time)

    if args.anonymize:
        raise NotImplementedError  # TODO

    if args.output == "-":
        yaml.dump(workload, sys.stdout)
    else:
        print(f"Writing workload recording to {args.output}", file=sys.stderr)
        with open(args.output, "w") as f:
            yaml.dump(workload, f)

    return 0


if __name__ == "__main__":
    sys.exit(main())
