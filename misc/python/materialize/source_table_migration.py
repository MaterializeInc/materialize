# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Utilities for testing the source table migration"""
from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import Composition
from materialize.version_list import get_published_minor_mz_versions


def verify_sources_after_source_table_migration(
    c: Composition, file: str, fail: bool = False
) -> None:
    source_names_rows = c.sql_query(
        "SELECT sm.name || '.' || src.name FROM mz_sources src INNER JOIN mz_schemas sm ON src.schema_id = sm.id WHERE src.id LIKE 'u%';"
    )
    source_names = [row[0] for row in source_names_rows]

    print(f"Sources created in {file} are: {source_names}")

    c.sql("SET statement_timeout = '20s'")

    for source_name in source_names:
        _verify_source(c, file, source_name, fail=fail)


def _verify_source(
    c: Composition, file: str, source_name: str, fail: bool = False
) -> None:
    try:
        print(f"Checking source: {source_name}")

        # must not crash
        statement = f"SELECT count(*) FROM {source_name};"
        c.sql_query(statement)

        statement = f"SHOW CREATE SOURCE {source_name};"
        result = c.sql_query(statement)
        sql = result[0][1]
        assert "FOR TABLE" not in sql, f"FOR TABLE found in: {sql}"
        assert "FOR ALL TABLES" not in sql, f"FOR ALL TABLES found in: {sql}"

        if not source_name.endswith("_progress"):
            assert "CREATE SUBSOURCE" not in sql, f"CREATE SUBSOURCE found in: {sql}"

        print("OK.")
    except Exception as e:
        print(f"source-table-migration issue in {file}: {str(e)}")

        if fail:
            raise e


_last_version: MzVersion | None = None


def get_old_image_for_source_table_migration_test() -> str:
    global _last_version
    if _last_version is None:
        current_version = MzVersion.parse_cargo()
        minor_versions = [
            v
            for v in get_published_minor_mz_versions(
                limit=4, exclude_current_minor_version=True
            )
            if v < current_version
        ]
        _last_version = minor_versions[0]
    return f"materialize/materialized:{_last_version}"


def get_new_image_for_source_table_migration_test() -> str | None:
    return None
