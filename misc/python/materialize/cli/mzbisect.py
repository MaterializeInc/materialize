# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""mzbisect: localize data corruption by bisecting an object's dependency tree.

Point it at an environment's internal SQL port (typically through a Teleport
app proxy) and it creates an unbilled scratch cluster, walks the dependency
closure of the object you name, and probes every relation in it for
consistency violations: read errors, non-positive multiplicities, and index
arrangements that diverge from a fresh read of the persisted inputs.
"""

import contextlib
import sys
from collections.abc import Generator
from dataclasses import replace
from typing import Any

import click

from materialize.mzbisect import teleport
from materialize.mzbisect.db import ConnParams
from materialize.mzbisect.run import Options, run_bisect, run_scan


class Opt:
    db_host: dict[str, Any] = dict(
        default="localhost",
        help="DB connection host.",
        envvar="PGHOST",
    )
    db_port: dict[str, Any] = dict(
        default=6877,
        help="DB connection port (internal SQL port).",
        envvar="PGPORT",
    )
    db_user: dict[str, Any] = dict(
        default="mz_system",
        help="DB connection user. Creating the scratch cluster needs mz_system.",
        envvar="PGUSER",
    )
    db_pass: dict[str, Any] = dict(
        default=None,
        help="DB connection password.",
        envvar="PGPASSWORD",
    )
    db_name: dict[str, Any] = dict(
        default="materialize",
        help="DB connection database.",
        envvar="PGDATABASE",
    )
    db_require_ssl: dict[str, Any] = dict(
        is_flag=True,
        default=False,
        help="DB connection requires SSL.",
        envvar="PGREQUIRESSL",
    )
    teleport_env: dict[str, Any] = dict(
        default=None,
        help="Teleport database resource of the environment (e.g."
        " aws-us-east-1-<hash>-0). Spawns `tsh proxy db --tunnel` and connects"
        " through it, overriding --db-host and --db-port.",
    )
    teleport_request_id: dict[str, Any] = dict(
        default=None,
        help="Approved Teleport access request ID to `tsh login` with before"
        " connecting. Required for mz_system on production environments.",
    )


def conn_options(fn):  # type: ignore[no-untyped-def]
    fn = click.option("--db-host", **Opt.db_host)(fn)
    fn = click.option("--db-port", type=int, **Opt.db_port)(fn)
    fn = click.option("--db-user", **Opt.db_user)(fn)
    fn = click.option("--db-pass", **Opt.db_pass)(fn)
    fn = click.option("--db-name", **Opt.db_name)(fn)
    fn = click.option("--db-require-ssl", **Opt.db_require_ssl)(fn)
    fn = click.option("--teleport-env", **Opt.teleport_env)(fn)
    fn = click.option("--teleport-request-id", **Opt.teleport_request_id)(fn)
    return fn


@contextlib.contextmanager
def connect_params(
    db_host: str,
    db_port: int,
    db_user: str,
    db_pass: str | None,
    db_name: str,
    db_require_ssl: bool,
    teleport_env: str | None,
    teleport_request_id: str | None,
) -> Generator[ConnParams, None, None]:
    """Resolve connection parameters, holding a Teleport tunnel if requested."""
    params = ConnParams(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_pass,
        database=db_name,
        require_ssl=db_require_ssl,
    )
    try:
        if teleport_request_id is not None:
            teleport.login(teleport_request_id)
        if teleport_env is None:
            yield params
            return
        with teleport.tunnel(
            teleport_env, params.user, params.database or "materialize"
        ) as (host, port):
            # The tunnel terminates TLS locally, so the client side is plain.
            yield replace(params, host=host, port=port, require_ssl=False)
    except RuntimeError as e:
        print(f"error: {e}")
        sys.exit(2)


@click.group()
def app() -> None:
    pass


@app.command()
@conn_options
def scan(**conn: Any) -> None:
    """List dataflows currently reporting errors, as bisection candidates."""
    with connect_params(**conn) as params:
        sys.exit(run_scan(params))


@app.command()
@click.argument("object", metavar="OBJECT")
@click.option(
    "--size",
    default="50cc",
    help="Size of the scratch cluster to create.",
)
@click.option(
    "--cluster",
    default=None,
    help="Reuse this existing cluster instead of creating (and dropping) one.",
)
@click.option(
    "--keep",
    is_flag=True,
    default=False,
    help="Do not drop the created scratch cluster on exit.",
)
@click.option(
    "--timeout",
    default=900,
    help="Per-probe statement timeout in seconds.",
)
@click.option(
    "--sample",
    default=10,
    help="How many offending rows a potato probe reports.",
)
@click.option(
    "--skip",
    multiple=True,
    help="Object (name or ID) to leave out of the walk. Repeatable.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print the dependency closure and exit without touching the environment.",
)
@click.option(
    "--fingerprint/--no-fingerprint",
    default=True,
    help="Compare index arrangements against persist. This is the only probe"
    " that runs queries on the customer's own clusters; --no-fingerprint"
    " confines all reads to the scratch cluster.",
)
@click.option(
    "--durable-only",
    is_flag=True,
    default=False,
    help="Probe only relations that hold state (tables, sources, materialized"
    " views, indexed views, and the seed). Unindexed views are walked through"
    " but not probed. Use this on deep view stacks, where probing every view"
    " recomputes the same inputs over and over.",
)
@conn_options
def run(
    object: str,
    size: str,
    cluster: str | None,
    keep: bool,
    timeout: int,
    sample: int,
    skip: tuple[str, ...],
    dry_run: bool,
    fingerprint: bool,
    durable_only: bool,
    **conn: Any,
) -> None:
    """Bisect the dependency closure of OBJECT (name, ID, or index).

    Exits 0 if everything probed clean, 1 if corruption was found, 2 on
    usage errors.
    """
    opts = Options(
        scratch_size=size,
        scratch_cluster=cluster,
        keep=keep,
        timeout_secs=timeout,
        sample=sample,
        dry_run=dry_run,
        skip=skip,
        fingerprint=fingerprint,
        durable_only=durable_only,
    )
    with connect_params(**conn) as params:
        sys.exit(run_bisect(params, object, opts))


if __name__ == "__main__":
    app()
