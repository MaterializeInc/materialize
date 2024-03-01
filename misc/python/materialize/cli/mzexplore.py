# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path
from typing import Any

import click

import materialize.mzexplore as api

# import logging
# logging.basicConfig(encoding='utf-8', level=logging.DEBUG)

# Click CLI Application
# ---------------------


@click.group()
def app() -> None:
    pass


class Arg:
    repository = dict(
        type=click.Path(
            exists=False,
            file_okay=False,
            dir_okay=True,
            writable=True,
            readable=True,
            resolve_path=True,
        ),
        callback=lambda ctx, param, value: Path(value),
    )


class Opt:
    db_port = dict(
        default=6877,
        help="DB connection port.",
        envvar="PGPORT",
    )

    db_host = dict(
        default="localhost",
        help="DB connection host.",
        envvar="PGHOST",
    )

    db_user = dict(
        default="mz_support",
        help="DB connection user.",
        envvar="PGUSER",
    )

    db_pass = dict(
        default=None,
        help="DB connection password.",
        envvar="PGPASSWORD",
    )

    db_require_ssl = dict(
        is_flag=True,
        help="DB connection requires SSL.",
        envvar="PGREQUIRESSL",
    )

    mzfmt = dict(
        default=True,
        help="Format SQL statements with `mzfmt` if present.",
    )

    explainee_type = dict(
        type=click.Choice([v.name.lower() for v in list(api.ExplaineeType)]),
        default="all",  # We should have at least arity for good measure.
        callback=lambda ctx, param, v: api.ExplaineeType[v.upper()],
        help="EXPLAIN mode.",
        metavar="MODE",
    )

    explain_flags = dict(
        type=click.Choice([v.name.lower() for v in list(api.ExplainFlag)]),
        multiple=True,
        default=["arity"],  # We should have at least arity for good measure.
        callback=lambda ctx, param, vals: [api.ExplainFlag[v.upper()] for v in vals],
        help="WITH flag to pass to the EXPLAIN command.",
        metavar="FLAG",
    )

    explain_stage = dict(
        type=click.Choice([str(v.name.lower()) for v in list(api.ExplainStage)]),
        multiple=True,
        default=["optimized_plan"],  # Most often we'll only the optimized plan.
        callback=lambda ctx, param, vals: [api.ExplainStage[v.upper()] for v in vals],
        help="EXPLAIN stage to export.",
        metavar="STAGE",
    )

    explain_suffix = dict(
        type=str,
        default="",
        help="A suffix to append to the EXPLAIN output files.",
        metavar="SUFFIX",
    )

    explain_format = dict(
        type=click.Choice([str(v.name.lower()) for v in list(api.ExplainFormat)]),
        default=["TEXT"],
        callback=lambda ctx, param, v: api.ExplainFormat[v.upper()],
        help="AS [FORMAT] clause to pass to the EXPLAIN command.",
        metavar="FORMAT",
    )


def is_documented_by(original: Any) -> Any:
    def wrapper(target):
        target.__doc__ = original.__doc__
        return target

    return wrapper


@app.group()
@is_documented_by(api.extract)
def extract() -> None:
    pass


@extract.command()
@click.argument("target", **Arg.repository)
@click.argument("database", type=str)
@click.argument("schema", type=str)
@click.argument("name", type=str)
@click.option("--db-port", **Opt.db_port)
@click.option("--db-host", **Opt.db_host)
@click.option("--db-user", **Opt.db_user)
@click.option("--db-pass", **Opt.db_pass)
@click.option("--db-require-ssl", **Opt.db_require_ssl)
@click.option("--mzfmt/--no-mzfmt", **Opt.mzfmt)
@is_documented_by(api.extract.defs)
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
    try:
        api.extract.defs(
            target,
            database,
            schema,
            name,
            db_port,
            db_host,
            db_user,
            db_pass,
            db_require_ssl,
            mzfmt,
        )
    except Exception as e:
        raise click.ClickException(f"extract defs command failed: {e=}, {type(e)=}")


@extract.command()
@click.argument("target", **Arg.repository)
@click.argument("database", type=str)
@click.argument("schema", type=str)
@click.argument("name", type=str)
@click.option("--db-port", **Opt.db_port)
@click.option("--db-host", **Opt.db_host)
@click.option("--db-user", **Opt.db_user)
@click.option("--db-pass", **Opt.db_pass)
@click.option("--db-require-ssl", **Opt.db_require_ssl)
@click.option("--explainee-type", "-t", **Opt.explainee_type)
@click.option("--with", "-w", "explain_flags", **Opt.explain_flags)
@click.option("--stage", "-s", "explain_stages", **Opt.explain_stage)
@click.option("--format", "-f", "explain_format", **Opt.explain_format)
@click.option("--suffix", **Opt.explain_suffix)
@is_documented_by(api.extract.plans)
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
    explainee_type: api.ExplaineeType,
    explain_flags: list[api.ExplainFlag],
    explain_stages: set[api.ExplainStage],
    explain_format: api.ExplainFormat,
    suffix: str | None = None,
) -> None:
    try:
        api.extract.plans(
            target,
            database,
            schema,
            name,
            db_port,
            db_host,
            db_user,
            db_pass,
            db_require_ssl,
            explainee_type,
            explain_flags,
            explain_stages,
            explain_format,
            suffix,
        )
    except Exception as e:
        raise click.ClickException(f"extract plans command failed: {e=}, {type(e)=}")


# Entrypoint
# ----------

if __name__ == "__main__":
    app()
