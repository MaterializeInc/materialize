# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import contextlib
import functools
import logging
import ssl
import subprocess
from collections.abc import Generator
from pathlib import Path
from typing import Any, cast

import pg8000
import sqlparse
from pg8000.exceptions import InterfaceError
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

    def execute(self, statement: str) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute(statement)

    def catalog_items(
        self,
        database: str | None = None,
        schema: str | None = None,
        name: str | None = None,
        system: bool = False,
    ) -> DictGenerator:
        p = resource_path("catalog/s_items.sql" if system else "catalog/u_items.sql")
        q = parse_query(p)
        yield from self.query_all(
            q.format(
                database="'%'" if database is None else literal(database),
                schema="'%'" if schema is None else literal(schema),
                name="'%'" if name is None else literal(name),
            )
        )

    def object_clusters(
        self,
        object_ids: list[str],
    ) -> DictGenerator:
        p = resource_path("catalog/u_object_clusters.sql")
        q = parse_query(p)
        yield from self.query_all(
            q.format(object_ids=", ".join(map(literal, object_ids)))
        )

    def clone_dependencies(
        self,
        source_ids: list[str],
        cluster_id: str,
    ) -> DictGenerator:
        p = resource_path("catalog/u_clone_dependencies.sql")
        q = parse_query(p)
        yield from self.query_all(
            q.format(
                source_ids=", ".join(map(literal, source_ids)),
                cluster_id=literal(cluster_id),
            )
        )

    def arrangement_sizes(self, id: str) -> DictGenerator:
        p = resource_path("catalog/u_arrangement_sizes.sql")
        q = parse_query(p)
        yield from self.query_all(q.format(id=literal(id)))


@contextlib.contextmanager
def update_environment(
    db: Database, env: dict[str, str]
) -> Generator[Database, None, None]:
    original = dict()
    for e in db.query_all("SHOW ALL"):
        key, old_value = e["name"], e["setting"]
        if key in env:
            original[key] = old_value
            new_value = env[key]
            db.execute(f"SET {identifier(key)} = {literal(new_value)}")
    yield db
    for key, old_value in original.items():
        db.execute(f"SET {identifier(key)} = {literal(old_value)}")


# Utility functions
# -----------------


def parse_from_file(path: Path) -> list[str]:
    """Parses a *.sql file to a list of queries."""
    return sqlparse.split(path.read_text())


def parse_query(path: Path) -> str:
    """Parses a *.sql file to a single query."""
    queries = parse_from_file(path)
    assert len(queries) == 1, f"Exactly one query expected in {path}"
    return queries[0]


def try_mzfmt(sql: str) -> str:
    sql = sql.rstrip().rstrip(";")

    result = subprocess.run(
        ["mzfmt"],
        shell=True,
        input=sql.encode(),
        capture_output=True,
    )

    if result.returncode == 0:
        return result.stdout.decode("utf-8").rstrip()
    else:
        return sql.rstrip().rstrip(";")


def identifier(s: str, force_quote: bool = False) -> str:
    """
    A version of pg8000.native.identifier (1) that is _ACTUALLY_ compatible with
    the Postgres code (2).

    1. https://github.com/tlocke/pg8000/blob/017959e97751c35a3d58bc8bd5722cee5c10b656/pg8000/converters.py#L739-L761
    2. https://github.com/postgres/postgres/blob/b0f7dd915bca6243f3daf52a81b8d0682a38ee3b/src/backend/utils/adt/ruleutils.c#L11968-L12050
    """
    if not isinstance(s, str):
        raise InterfaceError("identifier must be a str")

    if len(s) == 0:
        raise InterfaceError("identifier must be > 0 characters in length")

    # Look for characters that require quotation.
    def is_alpha(c: str) -> bool:
        return ord(c) >= ord("a") and ord(c) <= ord("z") or c == "_"

    def is_alphanum(c: str) -> bool:
        return is_alpha(c) or ord(c) >= ord("0") and ord(c) <= ord("9")

    quote = not (is_alpha(s[0]))

    for c in s[1:]:
        if not (is_alphanum(c)):
            if c == "\u0000":
                raise InterfaceError(
                    "identifier cannot contain the code zero character"
                )
            quote = True

        if quote:
            break

    # Even if no speciall characters can be found we still want to quote
    # keywords.
    if s.upper() in keywords():
        quote = True

    if quote or force_quote:
        s = s.replace('"', '""')
        return f'"{s}"'
    else:
        return s


@functools.lru_cache(maxsize=1)
def keywords() -> set[str]:
    """
    Return a list of keywords reserved by Materialize.
    """
    with resource_path("sql/keywords.txt").open() as f:
        return set(
            line.strip().upper()
            for line in f.readlines()
            if not line.startswith("#") and len(line.strip()) > 0
        )
