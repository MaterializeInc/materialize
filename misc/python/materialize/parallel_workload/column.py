# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from typing import TYPE_CHECKING

from pg8000.native import identifier

from materialize.data_ingest.data_type import (
    DataType,
)
from materialize.util import naughty_strings

if TYPE_CHECKING:
    from materialize.parallel_workload.database import DBObject


NAUGHTY_IDENTIFIERS = False


def naughtify(name: str) -> str:
    """Makes a string into a naughty identifier, always returns the same
    identifier when called with the same input."""
    global NAUGHTY_IDENTIFIERS

    if not NAUGHTY_IDENTIFIERS:
        return name

    strings = naughty_strings()
    # This rng is just to get a more interesting integer for the name
    index = sum([10**i * c for i, c in enumerate(name.encode())]) % len(strings)
    # Keep them short so we can combine later with other identifiers, 255 char limit
    return f"{name}_{strings[index].encode('utf-8')[:16].decode('utf-8', 'ignore')}"


class Column:
    column_id: int
    data_type: type[DataType]
    db_object: "DBObject"
    nullable: bool
    default: str | None
    raw_name: str

    def __init__(
        self,
        rng: random.Random,
        column_id: int,
        data_type: type[DataType],
        db_object: "DBObject",
    ):
        self.column_id = column_id
        self.data_type = data_type
        self.db_object = db_object
        self.nullable = rng.choice([True, False])
        self.default = rng.choice(
            [None, str(data_type.random_value(rng, in_query=True))]
        )
        self.raw_name = f"c-{self.column_id}-{self.data_type.name()}"

    def name(self, in_query: bool = False) -> str:
        return (
            identifier(naughtify(self.raw_name))
            if in_query
            else naughtify(self.raw_name)
        )

    def __str__(self) -> str:
        return f"{self.db_object}.{self.name(True)}"

    def value(self, rng: random.Random, in_query: bool = False) -> str:
        return str(self.data_type.random_value(rng, in_query=in_query))

    def create(self) -> str:
        result = f"{self.name(True)} {self.data_type.name()}"
        if self.default:
            result += f" DEFAULT {self.default}"
        if not self.nullable:
            result += " NOT NULL"
        return result


class WebhookColumn(Column):
    def __init__(
        self,
        name: str,
        data_type: type[DataType],
        nullable: bool,
        db_object: "DBObject",
    ):
        self.raw_name = name
        self.data_type = data_type
        self.nullable = nullable
        self.db_object = db_object

    def name(self, in_query: bool = False) -> str:
        return identifier(self.raw_name) if in_query else self.raw_name


class KafkaColumn(Column):
    def __init__(
        self,
        name: str,
        data_type: type[DataType],
        nullable: bool,
        db_object: "DBObject",
    ):
        self.raw_name = name
        self.data_type = data_type
        self.nullable = nullable
        self.db_object = db_object

    def name(self, in_query: bool = False) -> str:
        return identifier(self.raw_name) if in_query else self.raw_name


class MySqlColumn(Column):
    def __init__(
        self,
        name: str,
        data_type: type[DataType],
        nullable: bool,
        db_object: "DBObject",
    ):
        self.raw_name = name
        self.data_type = data_type
        self.nullable = nullable
        self.db_object = db_object

    def name(self, in_query: bool = False) -> str:
        return identifier(self.raw_name) if in_query else self.raw_name


class PostgresColumn(Column):
    def __init__(
        self,
        name: str,
        data_type: type[DataType],
        nullable: bool,
        db_object: "DBObject",
    ):
        self.raw_name = name
        self.data_type = data_type
        self.nullable = nullable
        self.db_object = db_object

    def name(self, in_query: bool = False) -> str:
        return identifier(self.raw_name) if in_query else self.raw_name


class SqlServerColumn(Column):
    def __init__(
        self,
        name: str,
        data_type: type[DataType],
        nullable: bool,
        db_object: "DBObject",
    ):
        self.raw_name = name
        self.data_type = data_type
        self.nullable = nullable
        self.db_object = db_object

    def name(self, in_query: bool = False) -> str:
        return identifier(self.raw_name) if in_query else self.raw_name
