# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sys

from materialize.mzcompose.composition import Composition, Service, WorkflowArgumentParser
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
]


def workflow_default(c: Composition) -> None:
    c.up(
        "redpanda",
        "postgres",
        "mysql",
        "sql-server",
        "materialized",
        Service("testdrive", idle=True),
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
        name: str(version) if version else None
        for name, version in versions.items()
    }
    
    print(json.dumps(output, indent=2))


def workflow_start_version(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Start Materialize services with a specific version from the version matrix.
    
    This workflow is designed to be called by the console repo to start services
    before running console SQL tests. The console repo should call this, then run
    its own yarn test:sql, then call ./mzcompose down.
    
    Usage:
        ./mzcompose run start-version cloud-backward
        ./mzcompose run start-version sm-lts
        ./mzcompose run start-version  # defaults to cloud-current
    
    Arguments:
        version_alias: One of: cloud-backward, cloud-current, cloud-forward, sm-lts
                      Or a direct version like: v0.26.1.1
    """
    from materialize.console_version_matrix import get_console_test_versions
    
    parser.add_argument(
        "version_alias",
        nargs="?",
        default="cloud-current",
        help="Version alias (cloud-backward, cloud-current, cloud-forward, sm-lts) or direct version (v0.147.18)",
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
            print(f"Or provide a direct version like: v0.26.1.1")
            sys.exit(1)
        
        version = versions[version_alias]
        version_str = str(version) if version else None
    print(f"Starting services for version: {version_alias}")

    if version_str:
        print(f"Docker image: materialize/materialized:{version_str}")
        c.compose["services"]["materialized"]["image"] = f"materialize/materialized:{version_str}"
    else:
        print("Using current source build")

    # Start services
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
