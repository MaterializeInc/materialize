# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import re
import sys
from typing import Any

import yaml

from materialize import MZ_ROOT


def keywords() -> set[str]:
    with open(MZ_ROOT / "src" / "sql-lexer" / "src" / "keywords.txt") as f:
        return set(
            line.strip().lower()
            for line in f.readlines()
            if not line.startswith("#") and len(line.strip()) > 0
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="mz-workload-anonymize",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Anonymize identifiers in a workload capture file",
    )

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="Path to write the workload.yml, overrides the input file if not specified",
    )

    parser.add_argument(
        "file",
        type=str,
        help="Input workload.yml",
    )

    args = parser.parse_args()

    with open(args.file) as f:
        workload = yaml.load(f, Loader=yaml.CSafeLoader)

    # Don't rename keywords, it makes the text-based replacements go wrong
    kws = keywords()

    new = {
        "databases": {},
        "clusters": {},
        "queries": [],
        "mz_workload_version": "1.0.0",
    }
    mapping: dict[str, str] = {}

    count = {
        "schemas": 0,
        "tables": 0,
        "connections": 0,
        "sources": 0,
        "children": 0,
        "views": 0,
        "mvs": 0,
        "indexes": 0,
        "sinks": 0,
        "types": 0,
        "columns": 0,
    }

    def set_name(name: str, new_name: str) -> str:
        if name.lower() in kws:
            new_name = name
        mapping[name] = new_name
        return new_name

    for i, (name, cluster) in enumerate(workload["clusters"].items()):
        new_name = set_name(name, f"cluster_{i}")
        new["clusters"][new_name] = cluster

    for i, (db_name, database) in enumerate(workload["databases"].items()):
        new_db_name = set_name(db_name, f"db_{i}")
        new_database = {}
        for schema_name, schema in database.items():
            count["schemas"] += 1
            new_schema_name = set_name(schema_name, f"schema_{count['schemas']}")
            new_schema = {
                "tables": {},
                "views": {},
                "materialized_views": {},
                "indexes": {},
                "types": {},
                "connections": {},
                "sources": {},
                "sinks": {},
            }

            for type_name, typ in schema["types"].items():
                count["types"] += 1
                new_type_name = set_name(type_name, f"type_{count['types']}")
                new_schema["types"][new_type_name] = typ

            for table_name, table in schema["tables"].items():
                count["tables"] += 1
                new_table_name = set_name(table_name, f"table_{count['tables']}")
                old_columns = table["columns"]
                table["columns"] = {}
                for column in old_columns:
                    count["columns"] += 1
                    new_column_name = set_name(
                        column["name"], f"column_{count['columns']}"
                    )
                    column["name"] = new_column_name
                    table["columns"].append(column)
                new_schema["tables"][new_table_name] = table

            for conn_name, conn in schema["connections"].items():
                count["connections"] += 1
                new_conn_name = set_name(conn_name, f"conn_{count['connections']}")
                new_schema["connections"][new_conn_name] = conn

            for source_name, source in schema["sources"].items():
                count["sources"] += 1
                new_source_name = set_name(source_name, f"source_{count['sources']}")
                if "columns" in source:
                    old_columns = source["columns"]
                    source["columns"] = []
                    for column in old_columns:
                        count["columns"] += 1
                        new_column_name = set_name(
                            column["name"], f"column_{count['columns']}"
                        )
                        column["name"] = new_column_name
                        source["columns"].append(column)
                if "children" in source:
                    old_children = source["children"]
                    source["children"] = {}
                    for child_name_full, child in old_children.items():
                        count["children"] += 1
                        new_child_name = set_name(
                            child["name"], f"child_{count['children']}"
                        )
                        child["name"] = new_child_name
                        old_columns = child["columns"]
                        child["columns"] = []
                        for column in old_columns:
                            count["columns"] += 1
                            new_column_name = set_name(
                                column["name"], f"column_{count['columns']}"
                            )
                            column["name"] = new_column_name
                            child["columns"].append(column)
                        source["children"][
                            f"{child['database']}.{child['schema']}.{child['name']}"
                        ] = child
                new_schema["sources"][new_source_name] = source

            for view_name, view in schema["views"].items():
                count["views"] += 1
                new_view_name = set_name(view_name, f"view_{count['views']}")
                old_columns = view["columns"]
                view["columns"] = []
                for column in old_columns:
                    count["columns"] += 1
                    new_column_name = set_name(
                        column["name"], f"column_{count['columns']}"
                    )
                    column["name"] = new_column_name
                    view["columns"].append(column)
                new_schema["views"][new_view_name] = view

            for mv_name, mv in schema["materialized_views"].items():
                count["mvs"] += 1
                new_mv_name = set_name(mv_name, f"mv_{count['mvs']}")
                old_columns = mv["columns"]
                mv["columns"] = []
                for column in old_columns:
                    count["columns"] += 1
                    new_column_name = set_name(
                        column["name"], f"column_{count['columns']}"
                    )
                    column["name"] = new_column_name
                    mv["columns"].append(column)
                new_schema["materialized_views"][new_mv_name] = mv

            for index_name, index in schema["indexes"].items():
                count["indexes"] += 1
                new_index_name = set_name(index_name, f"index_{count['indexes']}")
                new_schema["indexes"][new_index_name] = index

            for sink_name, sink in schema["sinks"].items():
                count["sinks"] += 1
                new_sink_name = set_name(sink_name, f"sink_{count['sinks']}")
                new_schema["sinks"][new_sink_name] = sink

            new_database[new_schema_name] = new_schema
        new["databases"][new_db_name] = new_database

    pattern = re.compile(
        "|".join(map(re.escape, sorted(mapping, key=len, reverse=True)))
    )

    def replace_substr(d: dict[str, Any], entry: str) -> None:
        d[entry] = pattern.sub(lambda m: mapping[m.group(0)], d[entry])

    # TODO: The create_sql replacements are more of a heuristic because we might overwrite identifiers and same name existing twice. There are two alternatives:
    # 1. Wrap the Mz parser in Python, parse the SQL, resolve what they map to, rename, and reserialize.
    # 2. Spin up Materialize, RENAME everything. Doesn't work for column names? Then take a new recording
    for cluster in new["clusters"].values():
        replace_substr(cluster, "create_sql")
    for db in new["databases"].values():
        for schema in db.values():
            for table in schema["tables"].values():
                for column in table["columns"]:
                    if column["type"] in mapping:
                        column["type"] = mapping[column["type"]]
            for typ in schema["types"].values():
                replace_substr(typ, "create_sql")
            for conn in schema["connections"].values():
                replace_substr(conn, "create_sql")
            for source in schema["sources"].values():
                for column in source.get("columns", []):
                    if column["type"] in mapping:
                        column["type"] = mapping[column["type"]]
                for child in source.get("children", {}).values():
                    child["schema"] = mapping[child["schema"]]
                    child["database"] = mapping[child["database"]]
                    for column in child["columns"]:
                        if column["type"] in mapping:
                            column["type"] = mapping[column["type"]]
            for view in schema["views"].values():
                replace_substr(view, "create_sql")
            for mv in schema["materialized_views"].values():
                replace_substr(mv, "create_sql")
            for index in schema["indexes"].values():
                replace_substr(index, "create_sql")
            for sink in schema["sinks"].values():
                replace_substr(sink, "create_sql")
    for query in workload["queries"]:
        query["cluster"] = mapping[query["cluster"]]
        query["database"] = mapping[query["database"]]
        query["search_path"] = [mapping[schema] for schema in query["search_path"]]
        replace_substr(query, "sql")
        new["queries"].append(query)
    # TODO: Anonymize literals in queries?

    with open(args.output or args.file, "w") as f:
        yaml.dump(new, f, Dumper=yaml.CSafeDumper)

    return 0


if __name__ == "__main__":
    sys.exit(main())
