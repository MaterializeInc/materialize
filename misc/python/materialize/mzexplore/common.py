# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re
from dataclasses import dataclass, replace
from enum import Enum
from importlib import resources
from pathlib import Path
from typing import cast

import click
from pg8000.native import literal


class ExplaineeType(Enum):
    CREATE_STATEMENT = 1
    CATALOG_ITEM = 2
    REPLAN_ITEM = 4
    ALL = 7

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
    LOCAL_PLAN = "LOCALLY OPTIMIZED PLAN"
    OPTIMIZED_PLAN = "OPTIMIZED PLAN"
    PHYSICAL_PLAN = "PHYSICAL PLAN"
    OPTIMIZER_TRACE = "OPTIMIZER TRACE"

    def __str__(self):
        return self.value


@dataclass(frozen=True)
class ExplainOption:
    key: str
    val: str | bool | int | None = None

    def __str__(self):
        key = self.key.replace("_", " ").upper()
        if self.val is None:
            return f"{key}"
        else:
            return f"{key} = {literal(self.val)}"  # type: ignore

    def affects_pipeline(self) -> bool:
        """
        Returns true iff the `EXPLAIN` feature flag might affect the output of
        the optimizer pipeline.
        """
        return any(
            (
                self.key.lower() == "reoptimize_imported_views",
                self.key.lower().startswith("enable_"),
            )
        )


class ExplainOptionType(click.ParamType):
    name = "ExplainOption"

    pattern = re.compile(
        r"\s*(?P<key>[a-z0-9_]+)\s*(=\s*(?P<val>[a-z0-9_]+))?",
        re.IGNORECASE,
    )

    def convert(self, value, param, ctx):  # type: ignore
        m = ExplainOptionType.pattern.match(value)
        try:
            if m is None:
                raise ValueError(f"bad {self.name} format")

            key = str(m.group("key"))
            val = str(m.group("val")) if m.group("val") else None

            if val is None:
                return ExplainOption(
                    key=key,
                    val=None,
                )

            try:  # try converting to bool
                bool_values = dict(
                    on=True,
                    true=True,
                    yes=True,
                    y=True,
                    off=False,
                    false=False,
                    no=False,
                    n=False,
                )
                return ExplainOption(
                    key=key.lower(),
                    val=bool_values[val.lower()],
                )
            except KeyError:
                pass

            try:  # try converting to integer
                return ExplainOption(
                    key=key,
                    val=int(val),
                )
            except ValueError:
                pass

            # cannot convert: use a single-quoted string
            return ExplainOption(
                key=key,
                val=val,
            )
        except Exception as e:
            raise ValueError(f"Bad explain option: {value}: {e!r}") from e


class ItemType(str, Enum):
    CONNECTION = "connection"
    FUNCTION = "function"
    INDEX = "index"
    MATERIALIZED_VIEW = "materialized-view"
    SECRET = "secret"
    SINK = "sink"
    SOURCE = "source"
    TABLE = "table"
    TYPE = "type"
    VIEW = "view"

    def sql(self) -> str:
        """Return the SQL string corresponding to this item type."""
        return self.replace("-", " ").upper()

    def show_create(self, fqname: str) -> str | None:
        """
        Return a show create query for an item of the given type identified by
        the given `fqname` or `None` for item types that are currently not
        supported.
        """
        if self in [
            ItemType.INDEX,
            ItemType.MATERIALIZED_VIEW,
            ItemType.SOURCE,
            ItemType.TABLE,
            ItemType.VIEW,
        ]:
            return f"SHOW CREATE {self.sql()} {fqname}"
        else:  # unsupported item type
            return None


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
        # Skip _progress sources (they don't have a DDL)
        if self.item_type == ItemType.SOURCE:
            return self.name.endswith("_progress")
        # Skip items with database, schema, or item names that contain a `/`
        # (not a valid UNIX folder character).
        elif "/" in self.database:
            warn(f"Skip processing of item with bad database name: `{self.database}`")
            return True
        elif "/" in self.schema:
            warn(f"Skip processing of item with bad schema name: `{self.schema}`")
            return True
        elif "/" in self.name:
            warn(f"Skip processing of item with bad item name: `{self.name}`")
            return True
        # All good!
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

    def skip(self) -> bool:
        # Skip items with database, schema, or item names that contain a `/`
        # (not a valid UNIX folder character).
        if "/" in self.database:
            warn(f"Skip processing of item with bad database name: `{self.database}`")
            return True
        elif "/" in self.schema:
            warn(f"Skip processing of item with bad schema name: `{self.schema}`")
            return True
        elif "/" in self.name:
            warn(f"Skip processing of item with bad item name: `{self.name}`")
            return True
        # All good!
        else:
            return False


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
    return replace(base, explainee_type=ExplaineeType.REPLAN_ITEM, suffix=diff_suffix)


@dataclass(frozen=True)
class ClonedItem:
    database: str
    schema: str
    name: str
    id: str
    item_type: ItemType

    def name_old(self) -> str:
        from materialize.mzexplore import sql

        return f"{sql.identifier(self.name, True)}"

    def name_new(self) -> str:
        from materialize.mzexplore import sql

        if self.item_type == ItemType.INDEX:
            name = f"{self.name}_{self.id}"
        else:
            name = f"{self.database}_{self.schema}_{self.name}_{self.id}"

        return f"{sql.identifier(name, True)}"

    def fqname_old(self) -> str:
        from materialize.mzexplore import sql

        item_database = sql.identifier(self.database, True)
        item_schema = sql.identifier(self.schema, True)
        item_name = sql.identifier(self.name, True)
        return f"{item_database}.{item_schema}.{item_name}"

    def fqname_new(self, database: str, schema: str) -> str:
        return f"{database}.{schema}.{self.name_new()}"

    def create_name_old(self) -> str:
        if self.item_type == ItemType.INDEX:
            return self.name_old()
        else:
            return self.fqname_old()

    def create_name_new(self, database: str, schema: str) -> str:
        if self.item_type == ItemType.INDEX:
            return self.name_new()
        else:
            return self.fqname_new(database, schema)

    def aliased_ref_old(self) -> str:
        return f"{self.fqname_old()} AS"

    def aliased_ref_new(self, database: str, schema: str) -> str:
        return f"{self.fqname_new(database, schema)} AS"

    def index_on_ref_old(self) -> str:
        return f"ON {self.fqname_old()}"

    def index_on_ref_new(self, database: str, schema: str) -> str:
        return f"ON {self.fqname_new(database, schema)}"

    def simple_ref_old(self) -> str:
        return self.fqname_old()

    def simple_ref_new(self, database: str, schema: str) -> str:
        return f"{self.fqname_new(database, schema)} AS {self.name_old()}"


@dataclass(frozen=True)
class ArrangementSizesFile:
    database: str
    schema: str
    name: str
    item_type: ItemType
    ext: str = "csv"

    def file_name(self) -> str:
        return f"{self.name}.arrangement-sizes.{self.ext}"

    def folder(self) -> Path:
        return Path(self.item_type.value, self.database, self.schema)

    def path(self) -> Path:
        return self.folder() / self.file_name()

    def __str__(self) -> str:
        return str(self.path())

    def skip(self) -> bool:
        # Skip items with database, schema, or item names that contain a `/`
        # (not a valid UNIX folder character).
        if "/" in self.database:
            warn(f"Skip processing of item with bad database name: `{self.database}`")
            return True
        elif "/" in self.schema:
            warn(f"Skip processing of item with bad schema name: `{self.schema}`")
            return True
        elif "/" in self.name:
            warn(f"Skip processing of item with bad item name: `{self.name}`")
            return True
        else:
            # Arrangements only exists for the following item types
            return self.item_type not in {ItemType.MATERIALIZED_VIEW, ItemType.INDEX}


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
