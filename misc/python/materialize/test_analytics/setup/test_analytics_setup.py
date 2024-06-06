# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from pg8000 import Cursor

from materialize import MZ_ROOT


def setup_structures(cursor: Cursor) -> None:
    if exist_structures(cursor):
        return

    directory = f"{MZ_ROOT}/misc/python/materialize/test_analytics/setup/structures"
    setup_files = os.listdir(directory)
    setup_files.sort()

    for file_name in setup_files:
        if not file_name.endswith(".sql"):
            continue

        file_handle = open(f"{directory}/{file_name}")
        content = file_handle.read()

        sql_commands = content.split(";")

        for command in sql_commands:
            print(f"> {command}")
            cursor.execute(command)


def exist_structures(cursor: Cursor) -> bool:
    table_name_to_test = "build"
    cursor.execute(
        f"SELECT exists(SELECT 1 FROM mz_tables WHERE name = '{table_name_to_test}');"
    )
    return cursor.fetchall()[0][0]


def drop_structures_and_data(cursor: Cursor) -> None:
    cursor.execute("DROP DATABASE test_analytics;")
