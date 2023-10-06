# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import logging
import ssl
import subprocess
from collections.abc import Generator
from pathlib import Path
from typing import Any, cast

import pg8000
import sqlparse
from pg8000.native import literal

from materialize.mzexplore.common import resource_path

DictGenerator = Generator[dict[Any, Any], None, None]


class Database:
    """An API to the database under exploration."""

    def __init__(
        self,
        port: int,
        host: str,
        user: str,
        password: str | None,
        database: str | None,
        require_ssl: bool,
    ) -> None:
        logging.debug(f"Initialize Database with host={host} port={port}, user={user}")

        if require_ssl:
            # verify_mode=ssl.CERT_REQUIRED is the default
            ssl_context = ssl.create_default_context()
        else:
            ssl_context = None

        self.conn = pg8000.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            ssl_context=ssl_context,
        )
        self.conn.autocommit = True

    def close(self) -> None:
        self.conn.close()

    def query_one(self, query: str) -> dict[Any, Any]:
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            cols = [d[0].lower() for d in cursor.description]
            row = {key: val for key, val in zip(cols, cursor.fetchone())}
            return cast(dict[Any, Any], row)

    def query_all(self, query: str) -> DictGenerator:
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            cols = [d[0].lower() for d in cursor.description]
            for row in cursor.fetchall():
                yield {key: val for key, val in zip(cols, row)}

    def catalog_items(
        self,
        database: str | None = None,
        schema: str | None = None,
        name: str | None = None,
    ) -> DictGenerator:
        # Warning: this is not sanitizing the input!
        q = parse_query(resource_path("catalog/items.sql"))
        yield from self.query_all(
            q.format(
                database="'%'" if database is None else literal(database),
                schema="'%'" if schema is None else literal(schema),
                name="'%'" if name is None else literal(name),
            )
        )


# Utility functions
# -----------------


def parse_from_file(path: Path) -> list[str]:
    """Parses a *.sql file to a list of queries."""
    return sqlparse.split(path.read_text())


def parse_query(path: Path) -> str:
    """Parses a *.sql file to a list of queries."""
    queries = parse_from_file(path)
    assert len(queries) == 1, f"Exactly one query expected in {path}"
    return queries[0]


def try_mzfmt(sql: str) -> str:
    sql = sql.rstrip().rstrip(";")

    result = subprocess.run(
        ["mzfmt"],
        shell=True,
        input=sql.encode("utf-8"),
        capture_output=True,
    )

    if result.returncode == 0:
        return result.stdout.decode("utf-8").rstrip()
    else:
        return sql.rstrip().rstrip(";")
