# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Verifies that objects created in previous versions of Materialize are still
operational after an upgrade. See also the newer platform-checks' upgrade scenarios.
"""

import random

from materialize import buildkite
from materialize.mz_version import MzVersion
from materialize.mzcompose import get_default_system_parameters
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import DeploymentStatus, Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.version_list import (
    VersionsFromDocs,
    get_all_published_mz_versions,
    get_published_minor_mz_versions,
)

mz_options: dict[MzVersion, str] = {}

SERVICES = [
    TestCerts(),
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Postgres(),
    MySql(),
    Cockroach(setup_materialize=True),
    # Overridden below
    Materialized(),
    Materialized(name="materialized2"),
    # N.B.: we need to use `validate_catalog_store=False` because testdrive uses
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
        external_metadata_store=True,
        validate_catalog_store=False,
        volumes_extra=["secrets:/share/secrets", "mzdata:/mzdata"],
        metadata_store="cockroach",
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Test upgrades from previous versions."""

    parser.add_argument(
        "filter", nargs="?", default="*", help="limit to only the files matching filter"
    )
    parser.add_argument(
        "--versions-source",
        default="docs",
        choices=["docs", "git"],
        help="from what source to fetch the versions",
    )
    args = parser.parse_args()

    parallelism_index = buildkite.get_parallelism_index()
    parallelism_count = buildkite.get_parallelism_count()

    assert parallelism_count in [
        1,
        2,
    ], "Special cased parallelism, only allows values 1 or 2"

    all_versions, tested_versions = get_all_and_latest_two_minor_mz_versions(
        use_versions_from_docs=args.versions_source == "docs"
    )

    current_version = MzVersion.parse_cargo()
    min_upgradable_version = MzVersion.create(
        current_version.major, current_version.minor - 1, 0
    )

    for version in tested_versions:
        priors = [
            v for v in all_versions if v <= version and v >= min_upgradable_version
        ]

        if len(priors) == 0:
            # this may happen when versions are marked as invalid
            print("No versions to test!")
        else:
            if parallelism_count == 1 or parallelism_index == 0:
                test_upgrade_from_version(
                    c,
                    f"{version}",
                    priors,
                    filter=args.filter,
                    zero_downtime=True,
                    force_source_table_syntax=False,
                )
            if parallelism_count == 1 or parallelism_index == 1:
                test_upgrade_from_version(
                    c,
                    f"{version}",
                    priors,
                    filter=args.filter,
                    zero_downtime=False,
                    force_source_table_syntax=False,
                )
                test_upgrade_from_version(
                    c,
                    f"{version}",
                    priors,
                    filter=args.filter,
                    zero_downtime=False,
                    force_source_table_syntax=True,
                )

    if parallelism_count == 1 or parallelism_index == 0:
        test_upgrade_from_version(
            c,
            "current_source",
            priors=[],
            filter=args.filter,
            zero_downtime=True,
            force_source_table_syntax=False,
        )
    if parallelism_count == 1 or parallelism_index == 1:
        test_upgrade_from_version(
            c,
            "current_source",
            priors=[],
            filter=args.filter,
            zero_downtime=False,
            force_source_table_syntax=False,
        )
        test_upgrade_from_version(
            c,
            "current_source",
            priors=[],
            filter=args.filter,
            zero_downtime=False,
            force_source_table_syntax=True,
        )


def get_all_and_latest_two_minor_mz_versions(
    use_versions_from_docs: bool,
) -> tuple[list[MzVersion], list[MzVersion]]:
    if use_versions_from_docs:
        version_list = VersionsFromDocs(respect_released_tag=False)
        all_versions = version_list.all_versions()
        tested_versions = version_list.minor_versions()[-2:]
    else:
        tested_versions = get_published_minor_mz_versions(limit=2)
        all_versions = get_all_published_mz_versions(newest_first=False)
    return all_versions, tested_versions


def test_upgrade_from_version(
    c: Composition,
    from_version: str,
    priors: list[MzVersion],
    filter: str,
    zero_downtime: bool,
    force_source_table_syntax: bool,
) -> None:
    print(
        f"+++ Testing {'0dt upgrade' if zero_downtime else 'regular upgrade'} from Materialize {from_version} to current_source."
    )

    system_parameter_defaults = get_default_system_parameters(
        zero_downtime=zero_downtime,
    )
    deploy_generation = 0

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
    c.up("zookeeper", "kafka", "schema-registry", "postgres", "mysql")

    mz_service = "materialized"

    if from_version != "current_source":
        mz_from = Materialized(
            name=mz_service,
            image=f"materialize/materialized:{from_version}",
            options=[
                opt
                for start_version, opt in mz_options.items()
                if MzVersion.parse_mz(from_version) >= start_version
            ],
            volumes_extra=["secrets:/share/secrets"],
            external_metadata_store=True,
            system_parameter_defaults=system_parameter_defaults,
            deploy_generation=deploy_generation,
            restart="on-failure",
            sanity_restart=False,
            metadata_store="cockroach",
        )
        with c.override(mz_from):
            c.up(mz_service)
    else:
        mz_from = Materialized(
            name=mz_service,
            options=list(mz_options.values()),
            volumes_extra=["secrets:/share/secrets"],
            external_metadata_store=True,
            system_parameter_defaults=system_parameter_defaults,
            restart="on-failure",
            sanity_restart=False,
            metadata_store="cockroach",
        )
        with c.override(mz_from):
            c.up(mz_service)

    if from_version == "current_source" or MzVersion.parse_mz(
        from_version
    ) >= MzVersion.parse_mz("v0.82.0-dev"):
        created_cluster = "quickstart"
    else:
        created_cluster = "default"
    temp_dir = f"--temp-dir=/share/tmp/upgrade-from-{from_version}"
    seed = f"--seed={random.getrandbits(32)}"
    c.run_testdrive_files(
        "--no-reset",
        f"--var=upgrade-from-version={from_version}",
        f"--var=created-cluster={created_cluster}",
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        "--var=mysql-user-password=us3rp4ssw0rd",
        temp_dir,
        seed,
        f"create-in-{version_glob}-{filter}.td",
        mz_service=mz_service,
    )
    if zero_downtime:
        mz_service = "materialized2"
        deploy_generation += 1
        c.rm("testdrive")
    else:
        c.kill(mz_service)
        c.rm(mz_service, "testdrive")

    if from_version != "current_source":
        # We can't skip in-between minor versions anymore, so go through all of them
        for version in get_published_minor_mz_versions(newest_first=False):
            if version <= from_version:
                continue
            if version >= MzVersion.parse_cargo():
                continue
            if version <= MzVersion.parse_mz("v0.87.0"):
                # Old versions didn't care about upgrading the catalog one version at a time, so save some time
                continue

            print(
                f"'{'0dt-' if zero_downtime else ''}Upgrading to in-between version {version}"
            )
            with c.override(
                Materialized(
                    name=mz_service,
                    image=f"materialize/materialized:{version}",
                    options=[
                        opt
                        for start_version, opt in mz_options.items()
                        if version >= start_version
                    ],
                    volumes_extra=["secrets:/share/secrets"],
                    external_metadata_store=True,
                    system_parameter_defaults=system_parameter_defaults,
                    deploy_generation=deploy_generation,
                    restart="on-failure",
                    sanity_restart=False,
                    metadata_store="cockroach",
                )
            ):
                c.up(mz_service)
                if zero_downtime:
                    c.await_mz_deployment_status(
                        DeploymentStatus.READY_TO_PROMOTE, mz_service
                    )
                    c.promote_mz(mz_service)
                    c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, mz_service)
                    mz_service = (
                        "materialized2"
                        if mz_service == "materialized"
                        else "materialized"
                    )
                    deploy_generation += 1
                else:
                    c.kill(mz_service)
                    c.rm(mz_service)

    print(f"{'0dt-' if zero_downtime else ''}Upgrading to final version")
    system_parameter_defaults = get_default_system_parameters(
        zero_downtime=zero_downtime,
        # We can only force the syntax on the final version so that the migration to convert
        # sources to the new model can be applied without preventing sources from being
        # created in the old syntax on the older version.
        force_source_table_syntax=force_source_table_syntax,
    )
    mz_to = Materialized(
        name=mz_service,
        options=list(mz_options.values()),
        volumes_extra=["secrets:/share/secrets"],
        external_metadata_store=True,
        system_parameter_defaults=system_parameter_defaults,
        deploy_generation=deploy_generation,
        restart="on-failure",
        sanity_restart=False,
        metadata_store="cockroach",
    )
    with c.override(mz_to):
        c.up(mz_service)
        if zero_downtime:
            c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, mz_service)
            c.promote_mz(mz_service)
            c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, mz_service)
        else:
            # Restart once more, just in case
            c.kill(mz_service)
            c.rm(mz_service)
            c.up(mz_service)

    with c.override(
        Testdrive(
            external_metadata_store=True,
            validate_catalog_store=True,
            volumes_extra=["secrets:/share/secrets", "mzdata:/mzdata"],
            metadata_store="cockroach",
        )
    ):
        c.run_testdrive_files(
            "--no-reset",
            f"--var=upgrade-from-version={from_version}",
            f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
            f"--var=created-cluster={created_cluster}",
            temp_dir,
            seed,
            f"check-from-{version_glob}-{filter}.td",
            mz_service=mz_service,
        )
