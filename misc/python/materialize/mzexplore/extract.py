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
    CreateFile,
    ExplaineeType,
    ExplainFile,
    ExplainFlag,
    ExplainFormat,
    ExplainOption,
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

        for item in db.catalog_items(database, schema, name, system=False):
            item_database = sql.identifier(item["database"])
            item_schema = sql.identifier(item["schema"])
            item_name = sql.identifier(item["name"])
            fqname = f"{item_database}.{item_schema}.{item_name}"

            try:
                item_type = ItemType(item["type"])
            except ValueError:
                warn(f"Unsupported item type `{item['type']}` for {fqname}")
                continue

            create_file = CreateFile(
                database=item["database"],
                schema=item["schema"],
                name=item["name"],
                item_type=item_type,
            )

            if create_file.skip():
                continue

            show_create_query = item_type.show_create(fqname)
            if show_create_query is None:
                continue

            try:
                info(f"Extracting {item['type']} def in `{create_file.path()}`")
                create_sql = db.query_one(show_create_query)["create_sql"]
                item["create_sql"] = sql.try_mzfmt(create_sql) if mzfmt else create_sql

                # Ensure that the parent folder exists.
                (target / create_file.folder()).mkdir(parents=True, exist_ok=True)

                # Write the definition into the file.
                with (target / create_file.path()).open("w") as file:
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
    explain_options: list[ExplainFlag] | list[ExplainOption],
    explain_stages: set[ExplainStage],
    explain_format: ExplainFormat,
    suffix: str | None = None,
    system: bool = False,
) -> None:
    """
    Extract EXPLAIN plans for selected catalog items.

    Processes only items that match the ILIKE pattern defined by the parameter
    triple (DATABASE, SCHEMA, NAME).
    """

    # Click doesn't deduplicate, so we need to convert explain_stages (which is
    # actually a list) into a set explicitly.
    explain_stages = set(explain_stages)

    explain_options = [
        (
            ExplainOption(key=flag_or_opt.name)
            if isinstance(flag_or_opt, ExplainFlag)
            else flag_or_opt  # already an ExplainOption
        )
        for flag_or_opt in explain_options
    ]

    if not explain_options:
        # We should have at least arity for good measure.
        explain_options = [ExplainOption(key=ExplainFlag.ARITY.name)]

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
        for item in db.catalog_items(database, schema, name, system):
            item_database = sql.identifier(item["database"])
            item_schema = sql.identifier(item["schema"])
            item_name = sql.identifier(item["name"])
            if item["database"] == "mz":  # don't prepend pseudo-database `mz`
                fqname = f"{item_schema}.{item_name}"
            else:
                fqname = f"{item_database}.{item_schema}.{item_name}"

            try:
                item_type = ItemType(item["type"])
            except ValueError:
                warn(f"Unsupported item type `{item['type']}` for {fqname}")
                continue

            plans: dict[ExplainFile, str] = {}

            if ExplaineeType.CATALOG_ITEM.contains(explainee_type):
                # If the item can be explained, explain the DDL
                explainee = explain_item(item_type, fqname, False)
                if explainee is not None:
                    supported_stages = supported_explain_stages(
                        item_type, optimize=False
                    )
                    for stage in explain_stages:
                        if stage not in supported_stages:
                            continue

                        explain_file = ExplainFile(
                            database=item["database"],
                            schema=item["schema"],
                            name=item["name"],
                            suffix=suffix,
                            item_type=item_type,
                            explainee_type=ExplaineeType.CATALOG_ITEM,
                            stage=stage,
                            ext=explain_format.ext(),
                        )
                        info(f"Explaining {stage} for {explainee} in `{explain_file}`")
                        try:
                            plans[explain_file] = explain(
                                db,
                                stage,
                                explainee,
                                explain_options,
                                explain_format,
                            )
                        except DatabaseError as e:
                            warn(f"Cannot explain {stage} for {explainee}: {e}")
                            continue

            if ExplaineeType.CREATE_STATEMENT.contains(explainee_type):
                # If the DDL for the plan exists, explain it as well
                supported_stages = supported_explain_stages(item_type, optimize=True)

                create_file = CreateFile(
                    database=item["database"],
                    schema=item["schema"],
                    name=item["name"],
                    item_type=item_type,
                )
                if not (target / create_file.path()).is_file():
                    if set.intersection(supported_stages, explain_stages):
                        # No CREATE file, but a supported stage is requested
                        info(
                            f"WARNING: Skipping EXPLAIN CREATE for {fqname}: "
                            f"CREATE statement path `{target / create_file.path()}` does not exist."
                        )
                    continue

                explainee = (target / create_file.path()).read_text()

                for stage in explain_stages:
                    if stage not in supported_stages:
                        continue

                    explain_file = ExplainFile(
                        database=item["database"],
                        schema=item["schema"],
                        name=item["name"],
                        suffix=suffix,
                        item_type=item_type,
                        explainee_type=ExplaineeType.CREATE_STATEMENT,
                        stage=stage,
                        ext=explain_format.ext(),
                    )
                    info(f"Explaining {stage} for CREATE {fqname} in `{explain_file}`")
                    try:
                        plans[explain_file] = explain(
                            db,
                            stage,
                            explainee,
                            explain_options,
                            explain_format,
                        )
                    except DatabaseError as e:
                        warn(f"Cannot explain {stage} for CREATE {fqname}: {e}")
                        continue

            if ExplaineeType.REPLAN_ITEM.contains(explainee_type):
                # If the item can be explained, explain the DDL
                explainee = explain_item(item_type, fqname, True)
                if explainee is not None:
                    supported_stages = supported_explain_stages(item_type, True)
                    for stage in explain_stages:
                        if stage not in supported_stages:
                            continue

                        explain_file = ExplainFile(
                            database=item["database"],
                            schema=item["schema"],
                            name=item["name"],
                            suffix=suffix,
                            item_type=item_type,
                            explainee_type=ExplaineeType.REPLAN_ITEM,
                            stage=stage,
                            ext=explain_format.ext(),
                        )
                        info(f"Explaining {stage} for {explainee} in `{explain_file}`")
                        try:
                            plans[explain_file] = explain(
                                db,
                                stage,
                                explainee,
                                explain_options,
                                explain_format,
                            )
                        except DatabaseError as e:
                            warn(f"Cannot explain {stage} for {explainee}: {e}")
                            continue

            for explain_file, plan in plans.items():
                # Ensure that the parent folder exists.
                (target / explain_file.folder()).mkdir(parents=True, exist_ok=True)

                # Write the plan into the file.
                with (target / explain_file.path()).open("w") as file:
                    file.write(plan)


# Utility methods
# ---------------


def explain(
    db: sql.Database,
    explain_stage: ExplainStage,
    explainee: str,
    explain_options: list[ExplainOption],
    explain_format: ExplainFormat,
) -> str:
    explain_query = "\n".join(
        line
        for line in [
            f"EXPLAIN {explain_stage}",
            f"WITH({', '.join(map(str, explain_options))})" if explain_options else "",
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


def explain_item(item_type: ItemType, fqname: str, replan: bool) -> str | None:
    prefix = "REPLAN" if replan else ""
    if item_type == ItemType.MATERIALIZED_VIEW:
        return " ".join((prefix, "MATERIALIZED VIEW", fqname)).strip()
    if item_type == ItemType.INDEX:
        return " ".join((prefix, "INDEX", fqname)).strip()
    else:
        return None


def supported_explain_stages(item_type: ItemType, optimize: bool) -> set[ExplainStage]:
    if item_type in {ItemType.MATERIALIZED_VIEW, ItemType.INDEX}:
        if optimize:
            return set(ExplainStage)
        else:
            return set([ExplainStage.OPTIMIZED_PLAN, ExplainStage.PHYSICAL_PLAN])
    else:
        return set()
