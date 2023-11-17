# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Verifies that objects created in previous versions of Materialize are still
operational after an upgrade.
"""

import random

from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.version_list import VersionsFromDocs

version_list = VersionsFromDocs()
all_versions = version_list.all_versions()

mz_options: dict[MzVersion, str] = {}

SERVICES = [
    TestCerts(),
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Postgres(),
    Cockroach(setup_materialize=True),
    Materialized(
        options=list(mz_options.values()),
        volumes_extra=["secrets:/share/secrets"],
        external_cockroach=True,
        catalog_store="stash",
    ),
    # N.B.: we need to use `validate_postgres_stash=False` because testdrive uses
    # HEAD to load the catalog from disk but does *not* run migrations. There
    # is no guarantee that HEAD can load an old catalog without running
    # migrations.
    #
    # When testdrive is targeting a HEAD materialized, we re-enable catalog
    # validation.
    #
    # Disabling catalog validation is preferable to using a versioned testdrive
    # because that would involve maintaining backwards compatibility for all
    # testdrive commands.
    Testdrive(
        validate_postgres_stash=None,
        volumes_extra=["secrets:/share/secrets"],
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Test upgrades from previous versions."""

    parser.add_argument(
        "filter", nargs="?", default="*", help="limit to only the files matching filter"
    )
    args = parser.parse_args()

    tested_versions = version_list.minor_versions()[-2:]

    for version in tested_versions:
        priors = [v for v in all_versions if v <= version]
        test_upgrade_from_version(c, f"{version}", priors, filter=args.filter)

    test_upgrade_from_version(c, "current_source", priors=[], filter=args.filter)


def test_upgrade_from_version(
    c: Composition,
    from_version: str,
    priors: list[MzVersion],
    filter: str,
) -> None:
    print(f"+++ Testing upgrade from Materialize {from_version} to current_source.")

    # If we are testing vX.Y.Z, the glob should include all patch versions 0 to Z
    prior_patch_versions = []
    for prior in priors:
        for prior_patch_version in range(0, prior.patch):
            prior_patch_versions.append(
                MzVersion(
                    major=prior.major, minor=prior.minor, patch=prior_patch_version
                )
            )

    # We need this to be a new variable binding otherwise `pyright` complains of
    # a type mismatch
    prior_strings = sorted(str(p) for p in priors + prior_patch_versions)

    if len(priors) == 0:
        prior_strings = ["*"]

    version_glob = "{" + ",".join(["any_version", *prior_strings, from_version]) + "}"
    print(">>> Version glob pattern: " + version_glob)

    c.down(destroy_volumes=True)
    c.up("zookeeper", "kafka", "schema-registry", "postgres")

    if from_version != "current_source":
        mz_from = Materialized(
            image=f"materialize/materialized:{from_version}",
            options=[
                opt
                for start_version, opt in mz_options.items()
                if MzVersion.parse_mz(from_version) >= start_version
            ],
            volumes_extra=["secrets:/share/secrets"],
            external_cockroach=True,
            catalog_store="stash",
        )
        with c.override(mz_from):
            c.up("materialized")
    else:
        c.up("materialized")

    temp_dir = f"--temp-dir=/share/tmp/upgrade-from-{from_version}"
    seed = f"--seed={random.getrandbits(32)}"
    c.run(
        "testdrive",
        "--no-reset",
        f"--var=upgrade-from-version={from_version}",
        temp_dir,
        seed,
        f"create-in-{version_glob}-{filter}.td",
    )

    c.kill("materialized")
    c.rm("materialized", "testdrive")

    c.up("materialized")

    # Restart once more, just in case
    c.kill("materialized")
    c.rm("materialized")
    c.up("materialized")

    with c.override(
        Testdrive(
            validate_postgres_stash="cockroach",
            volumes_extra=["secrets:/share/secrets"],
        )
    ):
        c.run(
            "testdrive",
            "--no-reset",
            f"--var=upgrade-from-version={from_version}",
            f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
            temp_dir,
            seed,
            f"check-from-{version_glob}-{filter}.td",
        )
