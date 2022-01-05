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

from semver import Version

from materialize import util
from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    Postgres,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

# All released Materialize versions, in order from most to least recent.
all_versions = util.known_materialize_versions()

# The `materialized` options that are valid only at or above a certain version.
mz_options = {Version.parse("0.9.2"): "--persistent-user-tables"}

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Postgres(),
    Materialized(options=" ".join(mz_options.values())),
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
    Testdrive(
        validate_catalog=False,
        depends_on=["kafka", "schema-registry", "postgres", "materialized"],
    ),
]


def workflow_upgrade(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Test upgrades from various versions."""

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
    args = parser.parse_args()

    tested_versions = [v for v in all_versions if v >= args.min_version]
    if args.most_recent is not None:
        tested_versions = tested_versions[: args.most_recent]
    tested_versions.reverse()

    for version in tested_versions:
        priors = [f"v{v}" for v in all_versions if v < version]
        test_upgrade_from_version(c, f"v{version}", priors, filter=args.filter)

    test_upgrade_from_version(c, "current_source", priors=["*"], filter=args.filter)


def test_upgrade_from_version(
    c: Composition, from_version: str, priors: List[str], filter: str
) -> None:
    print(f"===>>> Testing upgrade from Materialize {from_version} to current_source.")

    version_glob = "|".join(["any_version", *priors, from_version])
    print(">>> Version glob pattern: " + version_glob)

    overrides = []
    if from_version != "current_source":
        overrides.append(
            Materialized(
                image=f"materialize/materialized:{from_version}",
                options=" ".join(
                    opt
                    for start_version, opt in mz_options.items()
                    if from_version[1:] >= start_version
                ),
            )
        )

    temp_dir = f"--temp-dir=/share/tmp/upgrade-from-{from_version}"

    with c.override(*overrides):
        c.run(
            "testdrive-svc",
            "--seed=1",
            "--no-reset",
            temp_dir,
            f"create-in-@({version_glob})-{filter}.td",
            env={"UPGRADE_FROM_VERSION": from_version},
        )

    c.rm("materialized", "testdrive-svc")

    c.run(
        "testdrive-svc",
        "--seed=1",
        "--no-reset",
        temp_dir,
        "--validate-catalog=/share/mzdata/catalog",
        f"check-from-@({version_glob})-{filter}.td",
        env={"UPGRADE_FROM_VERSION": from_version},
    )

    c.rm("materialized", "testdrive-svc")
    c.rm_volumes("mzdata", "tmp")
