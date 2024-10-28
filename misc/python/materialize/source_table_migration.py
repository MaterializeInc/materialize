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


def verify_sources_after_source_table_migration(
    c: Composition, file: str, fail: bool = False
) -> None:
    source_names_rows = c.sql_query(
        "SELECT sm.name || '.' || src.name FROM mz_sources src INNER JOIN mz_schemas sm ON src.schema_id = sm.id WHERE src.id LIKE 'u%';"
    )
    source_names = [row[0] for row in source_names_rows]

    print(f"Sources created in {file} are: {source_names}")

    for source_name in source_names:
        _verify_source(c, file, source_name, fail=fail)


def _verify_source(
    c: Composition, file: str, source_name: str, fail: bool = False
) -> None:
    try:
        print(f"Checking source: {source_name}")
        # must not crash
        c.sql("SET statement_timeout = '20s'")
        c.sql_query(f"SELECT count(*) FROM {source_name};")

        result = c.sql_query(f"SHOW CREATE SOURCE {source_name};")
        sql = result[0][1]
        assert "FOR TABLE" not in sql, f"FOR TABLE found in: {sql}"
        assert "FOR ALL TABLES" not in sql, f"FOR ALL TABLES found in: {sql}"
        assert "CREATE SUBSOURCE" not in sql, f"FOR ALL TABLES found in: {sql}"
        print("OK.")
    except Exception as e:
        print(f"source-table-migration issue in {file}: {str(e)}")

        if fail:
            raise e


def check_source_table_migration_test_sensible() -> None:
    assert MzVersion.parse_cargo() < MzVersion.parse_mz(
        "v0.130.0"
    ), "migration test probably no longer needed"


def get_old_image_for_source_table_migration_test() -> str:
    return "materialize/materialized:v0.122.0"


def get_new_image_for_source_table_migration_test() -> str | None:
    return None
