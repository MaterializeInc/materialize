# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import sys

from materialize import MZ_ROOT
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import Service as ServiceDefinition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Redpanda(),
    Postgres(),
    MySql(),
    SqlServer(),
    Testdrive(),
    Materialized(system_parameter_defaults={"enable_rbac_checks": "false"}),
    ServiceDefinition(
        name="console-runner",
        config={
            "image": "node:22-alpine",
            "volumes": [
                f"{MZ_ROOT}:/workdir",
                "/var/run/docker.sock:/var/run/docker.sock",
            ],
            "working_dir": "/workdir/console",
            "init": True,
        },
    ),
]

# UID/GID of the host user, used to run yarn commands so they don't create
# root-owned files in the bind-mounted volume.
_HOST_UID_GID = f"{os.getuid()}:{os.getgid()}"


def workflow_default(c: Composition) -> None:
    """
    Run all console test workflows.
    """
    for name in c.workflows:
        if name in ("default", "list-versions", "start-version"):
            continue

        with c.test_case(name):
            c.workflow(name)


def workflow_sql_tests(c: Composition) -> None:
    """
    Run console SQL tests against a running Materialize instance.

    Starts Materialized and supporting services, then runs yarn test:sql
    inside a Node.js container on the composition network so it can reach
    materialized and other services by hostname.
    """
    c.up(
        "redpanda",
        "postgres",
        "sql-server",
        "mysql",
        "materialized",
        Service("testdrive", idle=True),
        Service("console-runner", idle=True),
    )

    # These commands require root (default user in node:22-alpine).
    c.exec("console-runner", "apk", "add", "--no-cache", "docker-cli")
    c.exec(
        "console-runner",
        "sh",
        "-c",
        "corepack enable",
        env_extra={"COREPACK_ENABLE_DOWNLOAD_PROMPT": "0"},
    )

    # Run yarn as the host user so it doesn't create root-owned files
    # in the bind-mounted node_modules directory.
    _env = {
        "MZ_INTERNAL_SQL_HOST": "materialized",
        "MZ_INTERNAL_SQL_PORT": "6877",
        "MZ_INTERNAL_HTTP_HOST": "materialized",
        "MZ_INTERNAL_HTTP_PORT": "6878",
        "COREPACK_ENABLE_DOWNLOAD_PROMPT": "0",
        "HOME": "/tmp",
    }
    c.invoke(
        "exec",
        "--user",
        _HOST_UID_GID,
        *(f"-e{k}={v}" for k, v in _env.items()),
        "-T",
        "console-runner",
        "sh",
        "-c",
        " && ".join(
            [
                "yarn install --immutable --network-timeout 30000",
                "yarn test:sql --run",
            ]
        ),
    )


def workflow_list_versions(c: Composition) -> None:
    """
    Print the version matrix for console testing.

    This outputs a JSON object that Console CI can consume to determine
    which Materialize versions to test against.

    Usage:
        ./mzcompose run list-versions
    """
    import json

    from materialize.console_version_matrix import get_console_test_versions

    versions = get_console_test_versions()

    # Convert to JSON-friendly format (None -> null, MzVersion -> string)
    output = {
        name: str(version) if version else None for name, version in versions.items()
    }

    print(json.dumps(output, indent=2))


def workflow_start_version(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Start Materialize services with a specific version from the version matrix.

    This workflow is designed to be called by the console repo to start services
    before running console SQL tests. The console repo should call this, then run
    its own yarn test:sql, then call ./mzcompose down.

    Usage:
        ./mzcompose run start-version latest
        ./mzcompose run start-version self-managed
        ./mzcompose run start-version  # defaults to latest

    Arguments:
        version_alias: One of: latest, previous, older, self-managed
                      Or a direct version like: v26.1.1
    """
    from materialize.console_version_matrix import get_console_test_versions

    parser.add_argument(
        "version_alias",
        nargs="?",
        default="latest",
        help="Version alias (latest, previous, older, self-managed) or direct version (v26.1.1)",
    )
    args = parser.parse_args()
    version_alias = args.version_alias

    # Check if it's a direct version string (starts with 'v')
    if version_alias.startswith("v"):
        version_str = version_alias
        print(f"Using direct version: {version_str}")
    else:
        # Resolve from version matrix
        versions = get_console_test_versions()

        if version_alias not in versions:
            print(f"❌ Unknown version alias: {version_alias}")
            print(f"Available aliases: {', '.join(versions.keys())}")
            print("Or provide a direct version like: v26.1.1")
            sys.exit(1)

        version = versions[version_alias]
        version_str = str(version) if version else None
    print(f"Starting services for version: {version_alias}")

    print(f"Docker image: materialize/materialized:{version_str}")
    mz_service = Materialized(
        image=f"materialize/materialized:{version_str}" if version_str else None,
        system_parameter_defaults={"enable_rbac_checks": "false"},
    )

    with c.override(mz_service):
        c.up(
            "redpanda",
            "postgres",
            "sql-server",
            "mysql",
            "materialized",
            Service("testdrive", idle=True),
        )

    print("\n✅ Services started successfully")
    print("You can now run console SQL tests from the console repo:")
    print("  cd ../../../console && yarn test:sql")
