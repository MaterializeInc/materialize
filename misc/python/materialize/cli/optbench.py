# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import csv
import re
import tempfile
from pathlib import Path
from typing import cast

import click
import numpy as np
import pandas as pd  # type: ignore

from ..optbench import Scenario, scenarios, sql, util

# import logging
# logging.basicConfig(encoding='utf-8', level=logging.DEBUG)

# Typer CLI Application
# ---------------------


@click.group()
def app() -> None:
    pass


class Arg:
    scenario = dict(
        type=click.Choice(scenarios()),
        callback=lambda ctx, param, value: Scenario(value),
    )

    base = dict(
        type=click.Path(
            exists=True,
            file_okay=True,
            dir_okay=False,
            writable=False,
            readable=True,
            resolve_path=True,
        ),
        callback=lambda ctx, param, value: Path(value),
    )

    diff = dict(
        type=click.Path(
            exists=True,
            file_okay=True,
            dir_okay=False,
            writable=False,
            readable=True,
            resolve_path=True,
        ),
        callback=lambda ctx, param, value: Path(value),
    )


class Opt:
    repository = dict(
        default=Path(tempfile.gettempdir()),
        type=click.Path(
            exists=True,
            file_okay=False,
            dir_okay=True,
            writable=True,
            readable=True,
            resolve_path=True,
        ),
        help="Experiment results folder.",
        callback=lambda ctx, param, value: Path(value),
    )

    samples = dict(default=11, help="Samples per query.")

    print_results = dict(default=False, help="Print the experiment results.")

    no_indexes = dict(default=False, help="Skip CREATE [DEFAULT]/DROP INDEX DDL.")

    db_port = dict(default=6875, help="DB connection port.")

    db_host = dict(default="localhost", help="DB connection host.")

    db_user = dict(default="materialize", help="DB connection user.")


@app.command()
@click.argument("scenario", **Arg.scenario)
@click.option("--no-indexes", **Opt.no_indexes)
@click.option("--db-port", **Opt.db_port)
@click.option("--db-host", **Opt.db_host)
@click.option("--db-user", **Opt.db_user)
def init(
    scenario: Scenario,
    no_indexes: bool,
    db_port: int,
    db_host: str,
    db_user: str,
) -> None:
    """Initialize the DB under test for the given scenario."""

    info(f'Initializing "{scenario}" as the DB under test')

    try:
        db = sql.Database(port=db_port, host=db_host, user=db_user)

        db.drop_database(scenario)
        db.create_database(scenario)

        db.set_database(scenario)

        statements = sql.parse_from_file(scenario.schema_path())
        if no_indexes:
            idx_re = re.compile(r"(create|create\s+default|drop)\s+index\s+")
            statements = [
                statement
                for statement in statements
                if not idx_re.match(statement.lower())
            ]
        db.execute_all(statements)
    except Exception as e:
        raise click.ClickException(f"init command failed: {e}")


@app.command()
@click.argument("scenario", **Arg.scenario)
@click.option("--samples", **Opt.samples)
@click.option("--repository", **Opt.repository)
@click.option("--print-results", **Opt.print_results)
@click.option("--db-port", **Opt.db_port)
@click.option("--db-host", **Opt.db_host)
@click.option("--db-user", **Opt.db_user)
def run(
    scenario: Scenario,
    samples: int,
    repository: Path,
    print_results: bool,
    db_port: int,
    db_host: str,
    db_user: str,
) -> None:
    """Run benchmark in the DB under test for a given scenario."""

    info(f'Running "{scenario}" scenario')

    try:
        db = sql.Database(port=db_port, host=db_host, user=db_user)
        db.set_database(scenario)

        df = pd.DataFrame(
            data={
                query.name(): np.array(
                    [
                        cast(
                            np.timedelta64,
                            db.explain(query, timing=True).optimization_time(),
                        ).astype(int)
                        for _ in range(samples)
                    ]
                )
                for query in [
                    sql.Query(query)
                    for query in sql.parse_from_file(scenario.workload_path())
                ]
            }
        )

        if print_results:
            print(df.to_string())

        results_path = util.results_path(repository, scenario, db.mz_version())
        info(f'Writing results to "{results_path}"')
        df.to_csv(results_path, index=False, quoting=csv.QUOTE_MINIMAL)
    except Exception as e:
        raise click.ClickException(f"run command failed: {e}")


@app.command()
@click.argument("base", **Arg.base)
@click.argument("diff", **Arg.diff)
def compare(
    base: Path,
    diff: Path,
) -> None:
    """Compare the results of a base and diff benchmark runs."""

    info(f'Compare experiment results between "{base}" and "{diff}"')

    try:
        base_df = pd.read_csv(base, quoting=csv.QUOTE_MINIMAL).agg(
            [np.min, np.median, np.max]
        )

        diff_df = pd.read_csv(diff, quoting=csv.QUOTE_MINIMAL).agg(
            [np.min, np.median, np.max]
        )

        # compute diff/base quotient for all (metric, query) pairs
        quot_df = diff_df / base_df
        # append average quotient across all queries for each metric
        quot_df.insert(0, "Avg", quot_df.mean(axis=1))

        # TODO: use styler to color-code the cells
        print("base times")
        print("----------")
        print(base_df.to_string())
        print("")
        print("diff times")
        print("----------")
        print(diff_df.to_string())
        print("")
        print("diff/base ratio")
        print("---------------")
        print(quot_df.to_string())
    except Exception as e:
        raise click.ClickException(f"compare command failed: {e}")


# Utility methods
# ---------------


def print_df(df: pd.DataFrame) -> None:
    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        print(df)


def info(msg: str, fg: str = "green") -> None:
    click.secho(msg, fg=fg)


def err(msg: str, fg: str = "red") -> None:
    click.secho(msg, fg=fg, err=True)


if __name__ == "__main__":
    app()
