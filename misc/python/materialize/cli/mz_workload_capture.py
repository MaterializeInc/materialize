# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import os
import re
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, LiteralString

import psycopg
import yaml
from psycopg.sql import SQL, Composable, Composed, Identifier, Literal

from materialize import MZ_ROOT, mzbuild


@contextmanager
def timed(label: str, *, width: int = 52):
    start = time.time()
    print(f"{label:<{width}} ", file=sys.stderr, end="", flush=True)
    try:
        yield
    finally:
        dt = time.time() - start
        print(f" [{dt:7.2f}s]", file=sys.stderr)


def to_sql_string(q: bytes | SQL | Composable | LiteralString, conn) -> str:
    if isinstance(q, Composable):
        return q.as_string(conn)
    if isinstance(q, bytes):
        return q.decode()
    return q


VERBOSE = False


def query(
    conn: psycopg.Connection,
    sql: LiteralString | bytes | SQL | Composed,
    timeout: int | None = None,
) -> list[Any]:
    if VERBOSE:
        print(f"\n> {to_sql_string(sql, conn)}")

    cancel_timer: threading.Timer | None = None

    try:
        if timeout is not None:
            cancel_timer = threading.Timer(timeout, conn.cancel)
            cancel_timer.start()

        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    except psycopg.errors.QueryCanceled:
        if VERBOSE:
            print("Too slow")
        return []
    except KeyboardInterrupt:
        conn.cancel()
        if VERBOSE:
            raise
        print(f"\n> {to_sql_string(sql, conn)}")
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        if cancel_timer is not None:
            cancel_timer.cancel()


def attach_source_statistics(
    conn: psycopg.Connection,
    name: str,
    source: dict[str, Any],
    start_time: float,
    end_time: float,
) -> None:
    try:
        if VERBOSE:
            print(f"Subscribing to mz_source_statistics_with_history for {name}")
        attach_source_statistics_internal(conn, source, start_time, end_time)
    except KeyboardInterrupt:
        conn.cancel()
        if VERBOSE:
            raise
        print(f"\nSubscribing to mz_source_statistics_with_history for {name}")
        attach_source_statistics_internal(conn, source, start_time, end_time)


def attach_source_statistics_internal(
    conn: psycopg.Connection,
    source: dict[str, Any],
    start_time: float,
    end_time: float,
) -> None:
    source_id = source["id"]
    if "bytes_total" in source:
        del source["bytes_total"]
    if "bytes_second" in source:
        del source["bytes_second"]
    if "messages_total" in source:
        del source["messages_total"]
    if "messages_second" in source:
        del source["messages_second"]
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


def attach_avg_column_sizes(
    conn: psycopg.Connection,
    db: str,
    schema: str,
    obj_name: str,
    obj: dict[str, Any],
) -> None:
    if "columns" not in obj:
        return

    avg_exprs = [
        SQL("avg(pg_column_size({}))").format(Identifier(col["name"]))
        for col in obj["columns"]
    ]

    query_sql = SQL("SELECT {} FROM {}.{}.{} LIMIT 100").format(
        SQL(", ").join(avg_exprs),
        Identifier(db),
        Identifier(schema),
        Identifier(obj_name),
    )

    rows = query(conn, query_sql, timeout=60)
    if not rows:
        return
    for col, avg_size in zip(obj["columns"], rows[0]):
        col["avg_size"] = int(avg_size) if avg_size is not None else None


def _build_capturable_objects(workload: dict[str, Any]) -> list[dict[str, Any]]:
    """Walk workload dict and return flat list of capturable objects with source_type info."""
    objects = []
    for db, schemas in workload["databases"].items():
        for schema, items in schemas.items():
            for name, table in items["tables"].items():
                objects.append(
                    {
                        "database": db,
                        "schema": schema,
                        "name": name,
                        "fqn": f"{db}.{schema}.{name}",
                        "source_type": "table",
                        "parent_source_fqn": None,
                        "columns": table.get("columns", []),
                        "id": table.get("id"),
                    }
                )

            for name, source in items["sources"].items():
                source_fqn = f"{db}.{schema}.{name}"
                source_type = source["type"]

                if source_type == "load-generator":
                    continue

                children = source.get("children", {})
                if children:
                    for child_fqn, child in children.items():
                        parts = child_fqn.split(".", 2)
                        if len(parts) == 3:
                            child_db, child_schema, child_name = parts
                        else:
                            child_db = child.get("database", db)
                            child_schema = child.get("schema", schema)
                            child_name = child.get("name", child_fqn)
                        objects.append(
                            {
                                "database": child_db,
                                "schema": child_schema,
                                "name": child_name,
                                "fqn": f"{child_db}.{child_schema}.{child_name}",
                                "source_type": source_type,
                                "parent_source_fqn": source_fqn,
                                "columns": child.get("columns", []),
                                "id": child.get("id"),
                            }
                        )
                else:
                    objects.append(
                        {
                            "database": db,
                            "schema": schema,
                            "name": name,
                            "fqn": source_fqn,
                            "source_type": source_type,
                            "parent_source_fqn": None,
                            "columns": source.get("columns", []),
                            "id": source.get("id"),
                        }
                    )
    return objects


def _build_object_meta(obj: dict[str, Any]) -> dict[str, Any]:
    """Build the metadata dict for one capturable object."""
    return {
        "database": obj["database"],
        "schema": obj["schema"],
        "name": obj["name"],
        "source_type": obj["source_type"],
        "parent_source_fqn": obj["parent_source_fqn"],
        "columns": [
            {
                "name": c["name"],
                "type": c["type"],
                "nullable": c.get("nullable", True),
            }
            for c in obj["columns"]
        ],
    }


def _acquire_persistcli_image() -> str:
    """Use mzbuild to resolve and acquire the jobs image (contains persistcli).

    Returns the full image spec (e.g. registry/jobs:mzbuild-FINGERPRINT).
    """
    repo = mzbuild.Repository(MZ_ROOT)
    deps = repo.resolve_dependencies([repo.images["jobs"]])
    deps.acquire()
    return deps["jobs"].spec()


def _lookup_shard_ids(
    conn: psycopg.Connection,
) -> dict[str, str]:
    """Query mz_internal.mz_storage_shards to map object_id -> shard_id."""
    result: dict[str, str] = {}
    rows = query(conn, "SELECT object_id, shard_id FROM mz_internal.mz_storage_shards")
    for object_id, shard_id in rows:
        result[object_id] = shard_id
    return result


def _persistcli_cmd(jobs_image: str, container: str | None) -> list[str]:
    """Return the command prefix for running persistcli via docker run.

    When container is set, shares its volumes and network so persistcli
    can access file:// blob storage and the consensus database.
    """
    cmd = ["docker", "run", "--rm"]
    if container:
        cmd += ["--volumes-from", container, f"--network=container:{container}"]
    cmd.append(jobs_image)
    # The jobs image entrypoint is "persistcli", so args follow directly.
    return cmd


def _capture_initial_data_persist(
    jobs_image: str,
    blob_uri: str,
    consensus_uri: str,
    objects: list[dict[str, Any]],
    shard_map: dict[str, str],
    output_dir: str,
    container: str | None = None,
) -> dict[str, int]:
    """Capture initial data via persistcli export --mode snapshot (Parquet).

    Streams Parquet output from persistcli stdout directly to the host file,
    avoiding docker cp overhead.

    Returns a mapping of shard_id -> as_of timestamp used for each snapshot,
    so that subscribe mode can use the same as_of to exclude initial data.
    """
    os.makedirs(output_dir, exist_ok=True)

    as_of_map: dict[str, int] = {}

    for obj in objects:
        fqn = obj["fqn"]
        obj_id = obj.get("id")
        shard_id = shard_map.get(obj_id, "") if obj_id else ""

        if not shard_id:
            print(
                f"  Warning: no shard found for {fqn} (id={obj_id}), skipping",
                file=sys.stderr,
            )
            continue

        host_path = os.path.join(output_dir, f"{fqn}.parquet")

        try:
            print(f"  {fqn} (shard {shard_id})", file=sys.stderr, flush=True)
            with open(host_path, "wb") as f:
                result = subprocess.run(
                    _persistcli_cmd(jobs_image, container)
                    + [
                        "export",
                        "--shard-id",
                        shard_id,
                        "--blob-uri",
                        blob_uri,
                        "--consensus-uri",
                        consensus_uri,
                        "--mode",
                        "snapshot",
                    ],
                    stdout=f,
                    stderr=subprocess.PIPE,
                    timeout=600,
                )
            stderr_text = (
                result.stderr.decode("utf-8", errors="replace") if result.stderr else ""
            )
            if stderr_text:
                print(stderr_text, file=sys.stderr, end="")
            if result.returncode != 0:
                print(
                    f"  Warning: persistcli failed for {fqn}",
                    file=sys.stderr,
                )
                if os.path.exists(host_path):
                    os.remove(host_path)
                continue
            # Parse the as_of from stderr: "Reading shard ... at as_of=<N> ..."
            m = re.search(r"as_of=(\d+)", stderr_text)
            if m:
                as_of_map[shard_id] = int(m.group(1))
            # Remove empty files (shard had no surviving data).
            if os.path.exists(host_path) and os.path.getsize(host_path) == 0:
                os.remove(host_path)
        except subprocess.TimeoutExpired:
            print(f"  Warning: persistcli timed out for {fqn}", file=sys.stderr)
            if os.path.exists(host_path):
                os.remove(host_path)
        except Exception as e:
            print(f"  Warning: error capturing {fqn}: {e}", file=sys.stderr)

    return as_of_map


def _capture_continuous_data_persist(
    jobs_image: str,
    blob_uri: str,
    consensus_uri: str,
    objects: list[dict[str, Any]],
    shard_map: dict[str, str],
    output_dir: str,
    as_of_map: dict[str, int],
    container: str | None = None,
) -> None:
    """Capture continuous data via persistcli export --mode subscribe.

    Uses subscribe mode starting from the initial snapshot's as_of timestamp
    to capture only changes that occurred after the initial snapshot.
    Streams Parquet output directly from persistcli stdout.
    """
    os.makedirs(output_dir, exist_ok=True)

    for obj in objects:
        fqn = obj["fqn"]
        obj_id = obj.get("id")
        shard_id = shard_map.get(obj_id, "") if obj_id else ""

        if not shard_id:
            print(
                f"  Warning: no shard found for {fqn}, skipping",
                file=sys.stderr,
            )
            continue

        as_of = as_of_map.get(shard_id)
        if as_of is None:
            print(
                f"  Warning: no as_of for {fqn} (shard {shard_id}), skipping",
                file=sys.stderr,
            )
            continue

        output_path = os.path.join(output_dir, f"{fqn}.parquet")

        try:
            print(
                f"  {fqn} (shard {shard_id})",
                file=sys.stderr,
                flush=True,
            )
            with open(output_path, "wb") as f:
                result = subprocess.run(
                    _persistcli_cmd(jobs_image, container)
                    + [
                        "export",
                        "--shard-id",
                        shard_id,
                        "--blob-uri",
                        blob_uri,
                        "--consensus-uri",
                        consensus_uri,
                        "--mode",
                        "subscribe",
                        "--as-of",
                        str(as_of),
                    ],
                    stdout=f,
                    stderr=subprocess.PIPE,
                    timeout=600,
                )
            stderr_text = (
                result.stderr.decode("utf-8", errors="replace") if result.stderr else ""
            )
            if stderr_text:
                print(stderr_text, file=sys.stderr, end="")
            if result.returncode != 0:
                print(
                    f"  Warning: persistcli failed for {fqn}",
                    file=sys.stderr,
                )
                if os.path.exists(output_path):
                    os.remove(output_path)
                continue
            # Remove empty files (no changes since initial snapshot).
            if os.path.exists(output_path) and os.path.getsize(output_path) == 0:
                os.remove(output_path)
                print(f"  {fqn}: no changes", file=sys.stderr)
            else:
                print(f"  {fqn}: captured changes", file=sys.stderr)
        except subprocess.TimeoutExpired:
            print(
                f"  Warning: persistcli timed out for {fqn}",
                file=sys.stderr,
            )
            if os.path.exists(output_path):
                os.remove(output_path)
        except Exception as e:
            print(
                f"  Warning: error capturing {fqn}: {e}",
                file=sys.stderr,
            )

    print("  Continuous data capture complete", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="mz-workload-capture",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Records a workload profile from a running Materialize instance (without actual data)",
    )

    parser.add_argument("mz_url", type=str)
    # Default cluster to use
    parser.add_argument("--cluster", type=str, default="mz_catalog_server")
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="Path to write the workload (YAML file or directory when --capture-data)",
        default=f"workload_{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%S')}",
    )
    parser.add_argument(
        "--time",
        type=int,
        help="How long of a query/data ingestion history to capture, in seconds",
        default=360,
    )
    parser.add_argument(
        "--capture-data",
        action=argparse.BooleanOptionalAction,
        help="Capture actual data from persist storage into Parquet files",
    )
    parser.add_argument(
        "--blob-uri",
        type=str,
        help="Blob URI for persist (required with --capture-data)",
    )
    parser.add_argument(
        "--consensus-uri",
        type=str,
        help="Consensus URI for persist (required with --capture-data)",
    )
    parser.add_argument(
        "--docker-container",
        type=str,
        help="Share volumes/network from this container when running persistcli (for file:// blobs or Unix-socket consensus)",
    )
    # Currently too slow to always enable, not used yet.
    parser.add_argument("--avg-column-size", action=argparse.BooleanOptionalAction)
    parser.add_argument("-v", "--verbose", action=argparse.BooleanOptionalAction)

    args = parser.parse_args()

    global VERBOSE
    VERBOSE = args.verbose

    conn = psycopg.connect(args.mz_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        try:
            cur.execute(SQL("SET CLUSTER = {}").format(Identifier(args.cluster)))
        except Exception:
            # Requested cluster doesn't exist; find a user cluster or fall back.
            user_clusters = [
                row[0]
                for row in cur.execute(
                    "SELECT name FROM mz_clusters WHERE id LIKE 'u%' LIMIT 1"
                ).fetchall()
            ]
            cluster = user_clusters[0] if user_clusters else "mz_catalog_server"
            print(f"Cluster '{args.cluster}' not found, using '{cluster}' instead")
            cur.execute(SQL("SET CLUSTER = {}").format(Identifier(cluster)))

    workload = {
        "databases": {},
        "clusters": {},
        "queries": [],
        "mz_workload_version": "1.0.0",
    }

    with timed("Fetching configuration"):
        configuration = {}
        for row in query(conn, "SHOW ALL"):
            configuration[row[0]] = row[1]
        workload["configuration"] = configuration

    with timed("Fetching clusters"):
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

    with timed("Fetching databases"):
        for (db,) in query(conn, "SELECT name FROM mz_databases"):
            workload["databases"][db] = {}

    with timed("Fetching schemas"):
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

    with timed("Fetching data types"):
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

    with timed("Fetching connections"):
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

    with timed("Fetching sources"):
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

    with timed("Fetching subsources"):
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
            columns = []
            obj = {
                "create_sql": create_sql,
                "name": subsource,
                "schema": schema,
                "database": db,
                "id": subsource_id,
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

            if columns:
                obj["columns"] = columns

            workload["databases"][source_db][source_schema]["sources"][
                source
            ].setdefault("children", {})[f"{db}.{schema}.{subsource}"] = obj

    with timed("Fetching tables"):
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
            columns = []
            for column, typ, nullable, default in query(
                conn,
                SQL(
                    "SELECT c.name, c.type, c.nullable, c.default from mz_internal.mz_object_fully_qualified_names AS ofqn JOIN mz_columns AS c ON c.id = ofqn.id WHERE ofqn.name = {} and ofqn.schema_name = {} and ofqn.database_name = {} ORDER BY c.position"
                ).format(Literal(table), Literal(schema), Literal(db)),
            ):
                columns.append(
                    {
                        "name": column,
                        "type": typ,
                        "nullable": nullable,
                        "default": default,
                    }
                )

            obj = {
                "create_sql": create_sql,
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
                try:
                    obj["rows"] = query(
                        conn,
                        SQL("SELECT count(*) FROM {}.{}.{}").format(
                            Identifier(db), Identifier(schema), Identifier(table)
                        ),
                    )[0][0]
                except Exception:
                    obj["rows"] = 0
                workload["databases"][db][schema]["tables"][table] = obj

    with timed("Fetching views"):
        for view_id, view, schema, db in query(
            conn,
            "SELECT mz_views.id, mz_views.name, mz_schemas.name, mz_databases.name FROM mz_views JOIN mz_schemas ON mz_views.schema_id = mz_schemas.id JOIN mz_databases ON mz_schemas.database_id = mz_databases.id",
        ):
            create_sql = query(
                conn,
                SQL("SELECT create_sql FROM (SHOW CREATE VIEW {}.{}.{})").format(
                    Identifier(db), Identifier(schema), Identifier(view)
                ),
            )[0][0]

            obj = {"create_sql": create_sql}

            columns = []
            for column, nullable, column_type, default in query(
                conn,
                SQL(
                    "SELECT name, nullable, type, default FROM mz_columns WHERE id = {} ORDER BY position"
                ).format(Literal(view_id)),
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

            workload["databases"][db][schema]["views"][view] = obj

    with timed("Fetching materialized views"):
        for mv_id, mv, schema, db in query(
            conn,
            "SELECT mz_materialized_views.id, mz_materialized_views.name, mz_schemas.name, mz_databases.name FROM mz_materialized_views JOIN mz_schemas ON mz_materialized_views.schema_id = mz_schemas.id JOIN mz_databases ON mz_schemas.database_id = mz_databases.id",
        ):
            create_sql = query(
                conn,
                SQL(
                    "SELECT create_sql FROM (SHOW CREATE MATERIALIZED VIEW {}.{}.{})"
                ).format(Identifier(db), Identifier(schema), Identifier(mv)),
            )[0][0]
            obj = {"create_sql": create_sql}

            columns = []
            for column, nullable, column_type, default in query(
                conn,
                SQL(
                    "SELECT name, nullable, type, default FROM mz_columns WHERE id = {} ORDER BY position"
                ).format(Literal(mv_id)),
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

            workload["databases"][db][schema]["materialized_views"][mv] = obj

    with timed("Fetching sinks"):
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

    with timed("Fetching indexes"):
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

    end_time = time.time()
    start_time = end_time - args.time

    with timed("Fetching queries"):
        for (
            sql,
            cluster,
            database,
            search_path,
            statement_type,
            finished_status,
            params,
            transaction_isolation,
            session_id,
            transaction_id,
            began_at,
            duration,
            result_size,
        ) in query(
            conn,
            SQL(
                "SELECT sql, cluster_name, database_name, search_path, statement_type, finished_status, params, transaction_isolation, session_id, transaction_id, began_at, finished_at - began_at, result_size FROM mz_internal.mz_recent_activity_log WHERE began_at > {} ORDER BY began_at ASC"
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
                    "session_id": str(session_id),
                    "transaction_id": int(transaction_id),
                    "began_at": began_at,
                    "duration": (
                        duration.total_seconds() if duration is not None else None
                    ),
                    "result_size": result_size,
                }
            )

    if args.avg_column_size:
        with timed("Fetching average column sizes"):
            for db_name, schemas in workload["databases"].items():
                for schema_name, items in schemas.items():
                    for table_name, table in items["tables"].items():
                        attach_avg_column_sizes(
                            conn, db_name, schema_name, table_name, table
                        )
                    for source_name, source in items["sources"].items():
                        if source["type"] == "load-generator":
                            continue
                        attach_avg_column_sizes(
                            conn, db_name, schema_name, source_name, source
                        )
                        for child in source.get("children", {}).values():
                            attach_avg_column_sizes(
                                conn,
                                child["database"],
                                child["schema"],
                                child["name"],
                                child,
                            )

    if not args.capture_data:
        with timed("Fetching source/subsource/table statistics"):
            for schemas in workload["databases"].values():
                for items in schemas.values():
                    for source_name, source in items["sources"].items():
                        attach_source_statistics(
                            conn, source_name, source, start_time, end_time
                        )
                        for child_name, child in source.get("children", {}).items():
                            attach_source_statistics(
                                conn, child_name, child, start_time, end_time
                            )

    if args.capture_data:
        container = args.docker_container
        blob_uri = args.blob_uri
        consensus_uri = args.consensus_uri

        # Auto-detect persist URIs from the container's environmentd process.
        # The entrypoint script sets these env vars, but they're only visible
        # in the environmentd process, not in new `docker exec` sessions.
        if container and (not blob_uri or not consensus_uri):
            with timed("Reading persist URIs from container"):
                # Find the environmentd PID and read its environment.
                result = subprocess.run(
                    [
                        "docker",
                        "exec",
                        container,
                        "bash",
                        "-c",
                        "cat /proc/$(pgrep -x environmentd)/environ | tr '\\0' '\\n'",
                    ],
                    capture_output=True,
                    text=True,
                )
                if result.returncode != 0:
                    print(
                        f"Error: failed to read environmentd env from container '{container}': {result.stderr.strip()}",
                        file=sys.stderr,
                    )
                    return 1
                env_vars = dict(
                    line.split("=", 1)
                    for line in result.stdout.splitlines()
                    if "=" in line
                )
                if not blob_uri:
                    blob_uri = env_vars.get("MZ_PERSIST_BLOB_URL", "")
                if not consensus_uri:
                    consensus_uri = env_vars.get("MZ_PERSIST_CONSENSUS_URL", "")

        if not blob_uri or not consensus_uri:
            print(
                "Error: --blob-uri and --consensus-uri are required with --capture-data "
                "(or use --docker-container to auto-detect them)",
                file=sys.stderr,
            )
            return 1

        print(f"Blob URI: {blob_uri}", file=sys.stderr)
        print(f"Consensus URI: {consensus_uri}", file=sys.stderr)

        with timed("Acquiring persistcli (jobs) image"):
            jobs_image = _acquire_persistcli_image()
        print(f"Using image: {jobs_image}", file=sys.stderr)

        output_dir = args.output
        if output_dir.endswith(".yml"):
            output_dir = output_dir[:-4]

        os.makedirs(output_dir, exist_ok=True)

        # Look up shard IDs for all objects.
        with timed("Looking up shard IDs"):
            shard_map = _lookup_shard_ids(conn)
        print(f"Found {len(shard_map)} shards", file=sys.stderr)

        # Build capturable objects (tables + source children).
        capturable_objects = _build_capturable_objects(workload)
        # Data comes from persist, so replay loads directly into MZ tables.
        for obj in capturable_objects:
            obj["source_type"] = "table"
        print(f"Found {len(capturable_objects)} capturable objects", file=sys.stderr)

        with timed("Capturing initial data from persist"):
            initial_data_dir = os.path.join(output_dir, "initial_data")
            as_of_map = _capture_initial_data_persist(
                jobs_image,
                blob_uri,
                consensus_uri,
                capturable_objects,
                shard_map,
                initial_data_dir,
                container=container,
            )

        capture_start_ts = time.time()

        # Wait for new data to flow into persist before capturing changes.
        print(f"Waiting {args.time}s for new data...", file=sys.stderr)
        time.sleep(args.time)

        with timed("Capturing continuous data from persist"):
            continuous_data_dir = os.path.join(output_dir, "continuous_data")
            _capture_continuous_data_persist(
                jobs_image,
                blob_uri,
                consensus_uri,
                capturable_objects,
                shard_map,
                continuous_data_dir,
                as_of_map=as_of_map,
                container=container,
            )

        capture_end_ts = time.time()

        workload["capture_method"] = "persist"
        workload["capture_start_ts"] = capture_start_ts
        workload["capture_end_ts"] = capture_end_ts

        # Build capture_meta.yml keyed by FQN.
        capture_meta: dict[str, Any] = {}
        for obj in capturable_objects:
            fqn = obj["fqn"]
            meta = _build_object_meta(obj)
            capture_meta[fqn] = meta

        with timed(f"Writing {output_dir}/capture_meta.yml"):
            with open(os.path.join(output_dir, "capture_meta.yml"), "w") as f:
                yaml.dump(capture_meta, f, Dumper=yaml.CSafeDumper)

        queries = workload.pop("queries")
        with timed(f"Writing {output_dir}/objects.yml"):
            with open(os.path.join(output_dir, "objects.yml"), "w") as f:
                yaml.dump(workload, f, Dumper=yaml.CSafeDumper)

        with timed(f"Writing {output_dir}/queries.yml"):
            with open(os.path.join(output_dir, "queries.yml"), "w") as f:
                yaml.dump({"queries": queries}, f, Dumper=yaml.CSafeDumper)
    elif args.output == "-":
        yaml.dump(workload, sys.stdout, Dumper=yaml.CSafeDumper)
    else:
        output_path = args.output
        if not output_path.endswith(".yml"):
            output_path += ".yml"
        with timed(f"Writing workload to {output_path}"):
            with open(output_path, "w") as f:
                yaml.dump(workload, f, Dumper=yaml.CSafeDumper)

    return 0


if __name__ == "__main__":
    sys.exit(main())
