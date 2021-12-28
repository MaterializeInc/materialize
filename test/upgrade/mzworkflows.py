# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
from typing import List
from unittest.mock import patch

from semver import Version

from materialize import util
from materialize.mzcompose import (
    Kafka,
    Materialized,
    Postgres,
    SchemaRegistry,
    Testdrive,
    Workflow,
    WorkflowArgumentParser,
    Zookeeper,
)

# All released Materialize versions, in order from most to least recent.
all_versions = util.known_materialize_versions()

# The `materialized` options that are valid only at or above a certain version.
mz_options = {Version.parse("0.9.2"): "--persistent-user-tables"}

prerequisites = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Postgres(),
]

services = [
    *prerequisites,
    *(
        Materialized(
            name=f"materialized_v{version}",
            image=f"materialize/materialized:v{version}",
            hostname="materialized",
            options=" ".join(
                option
                for starting_version, option in mz_options.items()
                if version >= starting_version
            ),
        )
        for version in all_versions
    ),
    Materialized(
        name="materialized_current_source",
        hostname="materialized",
        options=" ".join(mz_options.values()),
    ),
    # N.B.: we need to use `validate_catalog=False` because testdrive uses HEAD
    # to load the catalog from disk but does *not* run migrations. There is no
    # guarantee that HEAD can load an old catalog without running migrations.
    #
    # When testdrive is targeting a HEAD materialized, we re-enable catalog
    # validation below by manually passing the `--validate-catalog` flag.
    #
    # Disabling catalog validation is preferable to using a versioned testdrive
    # because that would involve maintaining backwards compatibility for all
    # testdrive commands.
    Testdrive(validate_catalog=False),
]


def workflow_upgrade(w: Workflow, args: List[str]):
    """Test upgrades from various versions."""

    parser = WorkflowArgumentParser(w)
    parser.add_argument(
        "--min-version",
        metavar="VERSION",
        type=Version.parse,
        default=Version.parse("0.8.0"),
        help="the minimum version to test from",
    )
    parser.add_argument(
        "--most-recent",
        metavar="N",
        type=int,
        help="limit testing to the N most recent versions",
    )
    parser.add_argument(
        "filter", nargs="?", default="*", help="limit to only the files matching filter"
    )
    args = parser.parse_args(args)

    tested_versions = [v for v in all_versions if v >= args.min_version]
    if args.most_recent is not None:
        tested_versions = tested_versions[: args.most_recent]
    tested_versions.reverse()

    w.start_and_wait_for_tcp(services=prerequisites)

    for version in tested_versions:
        priors = [f"v{v}" for v in all_versions if v < version]
        test_upgrade_from_version(w, f"v{version}", priors, filter=args.filter)

    test_upgrade_from_version(w, "current_source", priors=["*"], filter=args.filter)


def test_upgrade_from_version(
    w: Workflow, from_version: str, priors: List[str], filter: str
):
    print(f"===>>> Testing upgrade from Materialize {from_version} to current_source.")

    version_glob = "|".join(["any_version", *priors, from_version])
    print(">>> Version glob pattern: " + version_glob)

    mz_from = f"materialized_{from_version}"
    w.start_services(services=[mz_from])
    w.wait_for_mz(service=mz_from)

    temp_dir = f"--temp-dir=/share/tmp/upgrade-from-{from_version}"
    with patch.dict(os.environ, {"UPGRADE_FROM_VERSION": from_version}):
        w.run_service(
            service="testdrive-svc",
            command=f"--seed=1 --no-reset {temp_dir} create-in-@({version_glob})-{filter}.td",
        )

    w.kill_services(services=[mz_from])
    w.remove_services(services=[mz_from, "testdrive-svc"])

    mz_to = "materialized_current_source"
    w.start_services(services=[mz_to])
    w.wait_for_mz(service=mz_to)

    with patch.dict(os.environ, {"UPGRADE_FROM_VERSION": from_version}):
        w.run_service(
            service="testdrive-svc",
            command=f"--seed=1 --no-reset {temp_dir} --validate-catalog=/share/mzdata/catalog check-from-@({version_glob})-{filter}.td",
        )

    w.kill_services(services=[mz_to])
    w.remove_services(services=[mz_to, "testdrive-svc"])
    w.remove_volumes(volumes=["mzdata", "tmp"])
