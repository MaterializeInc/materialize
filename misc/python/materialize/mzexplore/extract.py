# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Utilities to extract data from a Materialize catalog for exploration purposes.
"""

import json
import string
import textwrap
from contextlib import closing
from pathlib import Path

from pg8000.dbapi import DatabaseError

from materialize.mzexplore import sql
from materialize.mzexplore.common import (
    ExplaineeType,
    ExplainFlag,
    ExplainFormat,
    ExplainStage,
    ItemType,
    info,
    warn,
)


def defs(
    target: Path,
    database: str,
    schema: str,
    name: str,
    db_port: int,
    db_host: str,
    db_user: str,
    db_pass: str | None,
    db_require_ssl: bool,
    mzfmt: bool,
) -> None:
    """
    Extract CREATE statements for selected catalog items.

    Processes only items that match the ILIKE pattern defined by the parameter
    triple (DATABASE, SCHEMA, NAME).
    """

    # Ensure that the target dir exists.
    target.mkdir(parents=True, exist_ok=True)

    with closing(
        sql.Database(
            port=db_port,
            host=db_host,
            user=db_user,
            database=None,
            password=db_pass,
            require_ssl=db_require_ssl,
        )
    ) as db:
        output_template = string.Template(
            textwrap.dedent(
                """
                -- id: $id
                -- oid: $oid
                $create_sql
                """
            ).lstrip()
        )

        # Extract materialized view definitions
        # -------------------------------------

        for item in db.catalog_items(database, schema, name):
            item_database = sql.identifier(item["database"])
            item_schema = sql.identifier(item["schema"])
            item_name = sql.identifier(item["name"])
            fqname = f"{item_database}.{item_schema}.{item_name}"

            if item["type"] == "index":
                show_create_query = f"SHOW CREATE INDEX {fqname}"
            elif item["type"] == "materialized-view":
                show_create_query = f"SHOW CREATE MATERIALIZED VIEW {fqname}"
            elif item["type"] == "source" and not item["name"].endswith("_progress"):
                show_create_query = f"SHOW CREATE SOURCE {fqname}"
            elif item["type"] == "table":
                show_create_query = f"SHOW CREATE TABLE {fqname}"
            elif item["type"] == "view":
                show_create_query = f"SHOW CREATE VIEW {fqname}"
            else:  # "connection", "secret"
                continue

            try:
                create_sql = db.query_one(show_create_query)["create_sql"]
                item["create_sql"] = sql.try_mzfmt(create_sql) if mzfmt else create_sql

                base = Path(item["type"], item["database"], item["schema"])
                path = base.joinpath(f"{item['name']}.sql")

                (target / base).mkdir(parents=True, exist_ok=True)

                # Save definitions in files.
                info(f"Extracting {item['type']} def in `{path}`")
                with (target / path).open("w") as file:
                    file.write(output_template.substitute(item))
            except DatabaseError as e:
                warn(f"Cannot export def {fqname}: {e}")


def plans(
    target: Path,
    database: str,
    schema: str,
    name: str,
    db_port: int,
    db_host: str,
    db_user: str,
    db_pass: str | None,
    db_require_ssl: bool,
    explainee_type: ExplaineeType,
    explain_flags: list[ExplainFlag],
    explain_stages: set[ExplainStage],
    explain_format: ExplainFormat,
    suffix: str | None = None,
) -> None:
    """
    Extract EXPLAIN plans for selected catalog items.

    Processes only items that match the ILIKE pattern defined by the parameter
    triple (DATABASE, SCHEMA, NAME).
    """

    # Click doesn't deduplicate, so we need to convert explain_stages (which is
    # actually a list) into a set explicitly.
    explain_stages = set(explain_stages)

    if not explain_flags:
        # We should have at least arity for good measure.
        explain_flags = [ExplainFlag.ARITY]

    with closing(
        sql.Database(
            port=db_port,
            host=db_host,
            user=db_user,
            database=None,
            password=db_pass,
            require_ssl=db_require_ssl,
        )
    ) as db:
        for item in db.catalog_items(database, schema, name):
            item_database = sql.identifier(item["database"])
            item_schema = sql.identifier(item["schema"])
            item_name = sql.identifier(item["name"])
            fqname = f"{item_database}.{item_schema}.{item_name}"

            try:
                item_type = ItemType(item["type"])
            except ValueError:
                warn(f"Unsupported item type {item['type']} for {fqname}")
                continue

            base = Path(item["type"], item["database"], item["schema"])

            plans: dict[str, str] = {}

            if ExplaineeType.CATALOG_ITEM.contains(explainee_type):
                # If the item can be explained, explain the DDL
                explainee = explain_item(item_type, fqname)
                if explainee is not None:
                    supported_stages = supported_explain_stages(item_type, create=False)
                    for stage in explain_stages:
                        if stage not in supported_stages:
                            continue

                        file_name = ".".join(
                            str(part)
                            for part in [
                                item["name"],
                                ExplaineeType.CATALOG_ITEM,
                                stage.name.lower(),
                                suffix,
                                explain_format.suffix(),
                            ]
                            if part is not None and part != ""
                        )
                        info(f"Explaining {stage} for {explainee} in `{file_name}`")
                        try:
                            plans[file_name] = explain(
                                db,
                                stage,
                                explainee,
                                explain_flags,
                                explain_format,
                            )
                        except DatabaseError as e:
                            warn(f"Cannot explain {stage} for {explainee}: {e}")
                            continue

            if ExplaineeType.CREATE_STATEMENT.contains(explainee_type):
                # If the DDL for the plan exists, explain it as well
                supported_stages = supported_explain_stages(item_type, create=True)

                create_path = base.joinpath("{name}.sql".format(**item))
                if not (target / create_path).is_file():
                    if set.intersection(supported_stages, explain_stages):
                        # No CREATE file, but a supported stage is requested
                        info(
                            f"WARNING: Skipping EXPLAIN CREATE for {fqname}: "
                            f"CREATE statement path `{create_path}` does not exist."
                        )
                    continue

                explainee = (target / create_path).read_text()

                for stage in explain_stages:
                    if stage not in supported_stages:
                        continue

                    file_name = ".".join(
                        str(part)
                        for part in [
                            item["name"],
                            ExplaineeType.CREATE_STATEMENT,
                            stage.name.lower(),
                            suffix,
                            "txt",
                        ]
                        if part is not None
                    )
                    info(f"Explaining {stage} for CREATE {fqname} in `{file_name}`")
                    try:
                        plans[file_name] = explain(
                            db,
                            stage,
                            explainee,
                            explain_flags,
                            explain_format,
                        )
                    except DatabaseError as e:
                        warn(f"Cannot explain {stage} for CREATE {fqname}: {e}")
                        continue

            for file_name, plan in plans.items():
                explain_path = base.joinpath(file_name)
                with (target / explain_path).open("w") as file:
                    file.write(plan)


# Utility methods
# ---------------


def explain(
    db: sql.Database,
    explain_stage: ExplainStage,
    explainee: str,
    explain_flags: list[ExplainFlag],
    explain_format: ExplainFormat,
) -> str:
    explain_query = "\n".join(
        line
        for line in [
            f"EXPLAIN {explain_stage}",
            f"WITH({', '.join(map(str, explain_flags))})" if explain_flags else "",
            f"AS {explain_format} FOR",
            explainee,
        ]
        if line != ""
    )
    if explain_stage == ExplainStage.OPTIMIZER_TRACE:
        return json.dumps(
            {
                "explainee": {"query": explainee},
                "list": [
                    {"id": id, **entry}
                    for (id, entry) in enumerate(db.query_all(explain_query))
                ],
            },
            indent=4,
        )
    else:
        return next(iter(db.query_one(explain_query).values()))


def explain_item(item_type: ItemType, fqname: str) -> str | None:
    if item_type == ItemType.MATERIALIZED_VIEW:
        return f"MATERIALIZED VIEW {fqname}"
    if item_type == ItemType.INDEX:
        return f"INDEX {fqname}"
    else:
        return None


def supported_explain_stages(item_type: ItemType, create: bool) -> set[ExplainStage]:
    if item_type in {ItemType.MATERIALIZED_VIEW, ItemType.INDEX}:
        if create:
            return set(ExplainStage)
        else:
            return set([ExplainStage.OPTIMIZED_PLAN, ExplainStage.PHYSICAL_PLAN])
    else:
        return set()
