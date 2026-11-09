# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Runs the current-source object-preservation case and reports historical
pre-native-to-native upgrade cases as unsupported.
"""

import random
from unittest import SkipTest

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
from materialize.version_list import (
    VersionsFromDocs,
    get_published_minor_mz_versions,
)

SERVICES = [
    TestCerts(),
    Kafka(),
    SchemaRegistry(),
    Postgres(volumes=["secrets:/certs:ro"]),
    MySql(),
    Cockroach(setup_materialize=True, in_memory=True),
    # Overridden below
    Materialized(),
    Materialized(name="materialized2"),
    Testdrive(
        external_metadata_store=True,
        validate_catalog_store=True,
        volumes_extra=["secrets:/share/secrets", "mzdata:/mzdata"],
        metadata_store="cockroach",
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Report unsupported historical upgrades and run current-source preservation."""

    parser.add_argument(
        "filter", nargs="?", default="*", help="limit to only the files matching filter"
    )
    parser.add_argument(
        "--versions-source",
        default="docs",
        choices=["docs", "git"],
        help="from what source to fetch the versions",
    )
    # Keep existing CI invocations parseable so they receive an unsupported
    # result, not an unrelated argument error. Neither flag enables conversion.
    parser.add_argument("--ignore-missing-version", action="store_true")
    parser.add_argument("--self-managed-upgrade", action="store_true")
    args = parser.parse_args()

    parallelism_index = buildkite.get_parallelism_index()
    parallelism_count = buildkite.get_parallelism_count()

    assert parallelism_count in [
        1,
        2,
    ], "Special cased parallelism, only allows values 1 or 2"

    tested_versions = get_released_upgrade_versions(
        use_versions_from_docs=args.versions_source == "docs"
    )
    if parallelism_count == 1 or parallelism_index == 0:
        for version in tested_versions:
            with c.test_case(str(version)):
                test_upgrade_from_version(c, str(version), args.filter)
        with c.test_case("current_source"):
            test_upgrade_from_version(c, "current_source", args.filter)


def get_released_upgrade_versions(use_versions_from_docs: bool) -> list[MzVersion]:
    if use_versions_from_docs:
        version_list = VersionsFromDocs(respect_released_tag=False, skip_rc=True)
        return version_list.minor_versions()[-2:]
    return [
        version
        for version in get_published_minor_mz_versions()
        if version < MzVersion.parse_cargo()
    ]


def test_upgrade_from_version(
    c: Composition,
    from_version: str,
    filter: str,
) -> None:
    if from_version != "current_source":
        raise SkipTest(
            f"Unsupported: pre-native environmentd {from_version} -> native "
            "controllerd/adapterd upgrade. No catalog conversion or reset workaround."
        )

    print(f"+++ Testing upgrade from Materialize {from_version} to current_source.")

    deploy_generation = 0

    version_glob = "{any_version,*,current_source}"
    print(">>> Version glob pattern: " + version_glob)

    c.down(destroy_volumes=True)
    c.up("kafka", "schema-registry", "postgres", "mysql")

    mz_service = "materialized"

    system_parameter_defaults = get_default_system_parameters(
        version=MzVersion.parse_cargo(),
    )
    mz_from = Materialized(
        name=mz_service,
        volumes_extra=["secrets:/share/secrets"],
        external_metadata_store=True,
        system_parameter_defaults=system_parameter_defaults,
        restart="on-failure",
        sanity_restart=False,
        metadata_store="cockroach",
    )
    with c.override(mz_from):
        c.up(mz_service)

    temp_dir = f"--temp-dir=/share/tmp/upgrade-from-{from_version}"
    seed = f"--seed={random.getrandbits(32)}"
    c.run_testdrive_files(
        "--no-reset",
        f"--var=upgrade-from-version={from_version}",
        "--var=created-cluster=quickstart",
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        "--var=mysql-user-password=us3rp4ssw0rd",
        temp_dir,
        seed,
        f"create-in-{version_glob}-{filter}.td",
        mz_service=mz_service,
    )

    mz_service = "materialized2"
    deploy_generation += 1
    c.rm("testdrive")

    print("Upgrading to final version")
    system_parameter_defaults = get_default_system_parameters()
    mz_to = Materialized(
        name=mz_service,
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
        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, mz_service)
        c.promote_mz(
            mz_service,
            retire_mz_service="materialized",
        )

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
            "--var=created-cluster=quickstart",
            temp_dir,
            seed,
            f"check-from-{version_glob}-{filter}.td",
            mz_service=mz_service,
        )
