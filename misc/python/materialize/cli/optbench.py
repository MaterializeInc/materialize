# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import csv
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import typer

from ..optbench import Scenario, sql, util

# import logging
# logging.basicConfig(encoding='utf-8', level=logging.DEBUG)

# Typer CLI Application
# ---------------------

app = typer.Typer()


class Arg:
    scenario: Scenario = typer.Argument(..., help="Scenario to use.")

    base: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
        help="Results of the base run",
    )

    diff: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
        help="Results of the diff run",
    )


class Opt:
    repository: Path = typer.Option(
        Path(tempfile.gettempdir()),
        exists=True,
        file_okay=False,
        dir_okay=True,
        writable=True,
        readable=True,
        resolve_path=True,
        help="Experiment results folder.",
    )

    samples: int = typer.Option(11, help="Samples per query.")

    print_results: bool = typer.Option(False, help="Print the experiment results.")

    db_port: int = typer.Option(6875, help="DB connection port.")

    db_host: str = typer.Option("localhost", help="DB connection host.")

    db_user: str = typer.Option("materialize", help="DB connection user.")


@app.command()
def init(
    scenario: Scenario = Arg.scenario,
    db_port: int = Opt.db_port,
    db_host: str = Opt.db_host,
    db_user: str = Opt.db_user,
) -> None:
    """Initialize the DB under test for the given `scenario`."""

    info(f'Initializing "{scenario}" as the DB under test')

    try:
        db = sql.Database(port=db_port, host=db_host, user=db_user)

        db.drop_database(scenario)
        db.create_database(scenario)

        db.set_database(scenario)
        db.execute_all(statements=sql.parse_from_file(scenario.schema_path()))
    except Exception as e:
        err(f"DB initialization failed: {e}")
        raise typer.Exit()


@app.command()
def run(
    scenario: Scenario = Arg.scenario,
    samples: int = Opt.samples,
    repository: Path = Opt.repository,
    print_results: bool = Opt.print_results,
    db_port: int = Opt.db_port,
    db_host: str = Opt.db_host,
    db_user: str = Opt.db_user,
) -> None:
    """Run benchmark in the DB under test"""

    info(f'Running "{scenario}" scenario')

    try:
        db = sql.Database(port=db_port, host=db_host, user=db_user)
        db.set_database(scenario)

        df = pd.DataFrame(
            data={
                query.name(): np.array(
                    [
                        db.explain(query, timing=True).optimization_time().astype(int)
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
        err(f"run command failed: {e}")
        raise typer.Exit()


@app.command()
def compare(
    base: Path = Arg.base,
    diff: Path = Arg.diff,
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
        err(f"compare command failed: {e}")
        raise typer.Exit()


# Utility methods
# ---------------


def print_df(df: pd.DataFrame) -> None:
    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        print(df)


def info(msg: str, fg: str = typer.colors.GREEN) -> None:
    typer.secho(msg, fg=fg)


def err(msg: str, fg: str = typer.colors.RED) -> None:
    typer.secho(msg, fg=fg, err=True)



if __name__ == "__main__":
    app()