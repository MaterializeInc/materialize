# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import logging
import re
import ssl
from enum import Enum
from importlib.abc import Traversable
from typing import Any, Dict, List, Optional, cast

import numpy as np
import pg8000
import sqlparse

from . import Scenario, util


class Dialect(Enum):
    PG = 0
    MZ = 1


class Query:
    """An API for manipulating workload queries."""

    def __init__(self, query: str) -> None:
        self.query = query

    def __str__(self) -> str:
        return self.query

    def name(self) -> str:
        """Extracts and returns the name of this query from a '-- name: {name}' comment.
        Returns 'anonymous' if the name is not set."""
        p = r"-- name\: (?P<name>.+)"
        m = re.search(p, self.query, re.MULTILINE)
        return m.group("name") if m else "anonoymous"

    def explain(self, timing: bool, dialect: Dialect = Dialect.MZ) -> str:
        """Prepends 'EXPLAIN ...' to the query respecting the given dialect."""

        if dialect == Dialect.PG:
            if timing:
                return "\n".join(["EXPLAIN (ANALYZE, TIMING TRUE)", self.query])
            else:
                return "\n".join(["EXPLAIN", self.query])
        else:
            if timing:
                return "\n".join(["EXPLAIN WITH(timing)", self.query])
            else:
                return "\n".join(["EXPLAIN", self.query])


class ExplainOutput:
    """An API for manipulating 'EXPLAIN ... PLAN FOR' results."""

    def __init__(self, output: str) -> None:
        self.output = output

    def __str__(self) -> str:
        return self.output

    def optimization_time(self) -> Optional[np.timedelta64]:
        """Optionally, returns the optimization_time time for an 'EXPLAIN' output."""
        p = r"(Optimization time|Planning Time)\: (?P<time>[0-9]+(\.[0-9]+)?\s?\S+)"
        m = re.search(p, self.output, re.MULTILINE)
        return util.duration_to_timedelta(m["time"]) if m else None


class Database:
    """An API to the database under test."""

    def __init__(
        self,
        port: int,
        host: str,
        user: str,
        password: Optional[str],
        database: Optional[str],
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
        self.dialect = Dialect.MZ if "Materialize" in self.version() else Dialect.PG

    def close(self) -> None:
        self.conn.close()

    def version(self) -> str:
        result = self.query_one("SELECT version()")
        return cast(str, result[0])

    def mz_version(self) -> Optional[str]:
        if self.dialect == Dialect.MZ:
            result = self.query_one("SELECT mz_version()")
            return cast(str, result[0])
        else:
            return None

    def drop_database(self, scenario: Scenario) -> None:
        logging.debug(f'Drop database "{scenario}"')
        self.execute(f"DROP DATABASE IF EXISTS {scenario}")

    def create_database(self, scenario: Scenario) -> None:
        logging.debug(f'Create database "{scenario}"')
        self.execute(f"CREATE DATABASE {scenario}")

    def explain(self, query: Query, timing: bool) -> "ExplainOutput":
        result = self.query_all(query.explain(timing, self.dialect))
        return ExplainOutput("\n".join([col for line in result for col in line]))

    def execute(self, statement: str) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute(statement)

    def execute_all(self, statements: List[str]) -> None:
        with self.conn.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)

    def query_one(self, query: str) -> Dict[Any, Any]:
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            return cast(Dict[Any, Any], cursor.fetchone())

    def query_all(self, query: str) -> Dict[Any, Any]:
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            return cast(Dict[Any, Any], cursor.fetchall())


# Utility functions
# -----------------


def parse_from_file(path: Traversable) -> List[str]:
    """Parses a *.sql file to a list of queries."""
    return sqlparse.split(path.read_text())
