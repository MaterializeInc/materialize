# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass, replace
from enum import Enum, auto
from importlib import resources
from pathlib import Path
from typing import cast

import click


class ExplaineeType(Enum):
    CREATE_STATEMENT = 1
    CATALOG_ITEM = 2
    ALL = 3

    def __str__(self):
        return self.name.lower()

    def contains(self, other: "ExplaineeType") -> bool:
        return (self.value & other.value) > 0


class ExplainFormat(Enum):
    TEXT = "TEXT"
    JSON = "JSON"

    def __str__(self):
        return self.name

    def ext(self):
        if self == ExplainFormat.JSON:
            return "json"

        return "txt"


class ExplainStage(str, Enum):
    RAW_PLAN = "RAW PLAN"
    DECORRELATED_PLAN = "DECORRELATED PLAN"
    OPTIMIZED_PLAN = "OPTIMIZED PLAN"
    PHYSICAL_PLAN = "PHYSICAL PLAN"
    OPTIMIZER_TRACE = "OPTIMIZER TRACE"

    def __str__(self):
        return self.value


class ExplainFlag(Enum):
    # Show the number of columns.
    ARITY = auto()
    # Render implemented MIR `Join` nodes in a way which reflects the implementation.
    JOIN_IMPLS = auto()
    # Show the sets of unique keys.
    KEYS = auto()
    # Restrict output trees to linear chains. Ignored if `raw_plans` is set.
    LINEAR_CHAINS = auto()
    # Show the `non_negative` in the explanation if it is supported by the backing IR.
    NON_NEGATIVE = auto()
    # Show the slow path plan even if a fast path plan was created. Useful for debugging.
    # Enforced if `timing` is set.
    NO_FAST_PATH = auto()
    # Don't normalize plans before explaining them.
    RAW_PLANS = auto()
    # Disable virtual syntax in the explanation.
    RAW_SYNTAX = auto()
    # Show the `subtree_size` attribute in the explanation if it is supported by the backing IR.
    SUBTREE_SIZE = auto()
    # Print optimization timings.
    TIMING = auto()
    # Show the `type` attribute in the explanation.
    TYPES = auto()
    # Show MFP pushdown information.
    FILTER_PUSHDOWN = auto()
    # Show cardinality information.
    CARDINALITY = auto()
    # Show inferred column names.
    COLUMN_NAMES = auto()
    # Use inferred column names when rendering scalar and aggregate expressions.
    HUMANIZED_EXPRS = auto()
    # Enable outer join lowering implemented in #22343.
    ENABLE_NEW_OUTER_JOIN_LOWERING = auto()
    # Enable eager delta joins implemented in #23318.
    ENABLE_EAGER_DELTA_JOINS = auto()

    def __str__(self):
        return self.name.lower()


class ItemType(str, Enum):
    CONNECTION = "connection"
    INDEX = "index"
    MATERIALIZED_VIEW = "materialized-view"
    SECRET = "secret"
    SINK = "sink"
    SOURCE = "source"
    TABLE = "table"
    VIEW = "view"
    TYPE = "type"

    def sql(self) -> str:
        """Return the SQL string corresponding to this item type."""
        return self.replace("-", " ").upper()


@dataclass(frozen=True)
class CreateFile:
    database: str
    schema: str
    name: str
    item_type: ItemType

    def file_name(self) -> str:
        return f"{self.name}.sql"

    def folder(self) -> Path:
        return Path(self.item_type.value, self.database, self.schema)

    def path(self) -> Path:
        return self.folder() / self.file_name()

    def __str__(self) -> str:
        return str(self.path())

    def skip(self) -> bool:
        if self.item_type == ItemType.SOURCE:
            return self.name.endswith("_progress")
        else:
            return False


@dataclass(frozen=True)
class ExplainFile:
    database: str
    schema: str
    name: str
    suffix: str | None
    item_type: ItemType
    explainee_type: ExplaineeType
    stage: ExplainStage
    ext: str

    def file_name(self) -> str:
        return ".".join(
            str(part)
            for part in [
                self.name,
                self.explainee_type,
                self.stage.name.lower(),
                self.suffix,
                "json" if self.stage == ExplainStage.OPTIMIZER_TRACE else self.ext,
            ]
            if part
        )

    def folder(self) -> Path:
        return Path(self.item_type.value, self.database, self.schema)

    def path(self) -> Path:
        return self.folder() / self.file_name()

    def __str__(self) -> str:
        return str(self.path())


def explain_file(path: Path) -> ExplainFile | None:
    filename = path.name.split(".")

    if len(filename) == 5:
        ext = filename.pop()
        suffix = filename.pop()
        stage = filename.pop()
        explainee_type = filename.pop()
        name = filename.pop()
    elif len(filename) == 4:
        ext = filename.pop()
        suffix = None
        stage = filename.pop()
        explainee_type = filename.pop()
        name = filename.pop()
    else:
        return None

    parents = list(path.parents)
    parents.reverse()
    schema = parents.pop().name
    database = parents.pop().name
    item_type = parents.pop().name

    try:
        return ExplainFile(
            database=database,
            schema=schema,
            name=name,
            suffix=suffix,
            item_type=ItemType(item_type.lower()),
            explainee_type=ExplaineeType[explainee_type.upper()],
            stage=ExplainStage[stage.upper()],
            ext=ext,
        )
    except (KeyError, ValueError):
        return None


def explain_diff(base: ExplainFile, diff_suffix: str) -> ExplainFile:
    return replace(base, suffix=diff_suffix)


def resource_path(name: str) -> Path:
    # NOTE: we have to do this cast because pyright is not comfortable with the
    # Traversable protocol.
    return cast(Path, resources.files(__package__)) / name


def info(msg: str, fg: str = "green") -> None:
    click.secho(msg, fg=fg)


def warn(msg: str, fg: str = "yellow") -> None:
    click.secho(msg, fg=fg, err=True)


def err(msg: str, fg: str = "red") -> None:
    click.secho(msg, fg=fg, err=True)
