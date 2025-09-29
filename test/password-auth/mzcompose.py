# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Tests for self-managed password authentication.
"""

from textwrap import dedent

from materialize import MZ_ROOT
from materialize.git import MzVersion
from materialize.mzcompose.composition import Composition, Service
from materialize.mzcompose.services.materialized import (
    DeploymentStatus,
    Materialized,
)
from materialize.mzcompose.services.postgres import (
    CockroachOrPostgresMetadata,
    Postgres,
)
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.version_list import fetch_self_managed_versions

MATERIALIZED_ENVIRONMENT_EXTRA = [
    "MZ_EXTERNAL_LOGIN_PASSWORD_MZ_SYSTEM=password",
]

ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS = {
    "enable_password_auth": "true",
}

# We use a different listener config for the Self Managed Materialize because it doesn't support SASL like the new
# materialized listener config.
# We also enable an listener for our internal HTTP endpoints to allow 0dt upgrades and
# and a listener for the internal SQL port for testdrive.
listeners_config_path = (
    f"{MZ_ROOT}/test/password-auth/self_managed_listener_config.json"
)

PASSWORD_AUTH_MATERIALIZED_ARGS = {
    "additional_system_parameter_defaults": ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS,
    "environment_extra": MATERIALIZED_ENVIRONMENT_EXTRA,
    "listeners_config_path": listeners_config_path,
    "external_metadata_store": True,
    "restart": "on-failure",
}


SERVICES = [
    Postgres(),
    CockroachOrPostgresMetadata(),
    Materialized(name="mz_old", **PASSWORD_AUTH_MATERIALIZED_ARGS),
    Materialized(name="mz_new", **PASSWORD_AUTH_MATERIALIZED_ARGS),
    Testdrive(
        materialize_url="postgres://mz_system:password@mz_old:6875",
        mz_service="mz_old",
        no_reset=True,
    ),
]


def workflow_default(c: Composition) -> None:
    workflow_self_managed_v_25_2_upgrade_oldest_to_latest_patch_release(c)


def verify_password_auth(c: Composition) -> None:
    c.testdrive(
        dedent(
            """
            > SELECT * FROM db1.schema1.t1
            c
            ----
            """
        )
    )


def test_upgrade_then_rbac_check(
    c: Composition, previous_version: MzVersion, new_version: MzVersion
) -> None:
    c.down(destroy_volumes=True)
    # Setup roles and rbac
    with c.override(
        Materialized(
            name="mz_old",
            image=f"materialize/materialized:{previous_version}",
            **PASSWORD_AUTH_MATERIALIZED_ARGS,
        ),
        Testdrive(
            materialize_url="postgres://mz_system:password@mz_old:6875",
            materialize_url_internal="postgres://materialize@mz_old:6877",
            mz_service="mz_old",
            no_reset=True,
        ),
    ):
        c.up(
            "mz_old",
            Service("testdrive", idle=True),
        )

        c.testdrive(
            dedent(
                """
            > CREATE DATABASE db1;
            > SET DATABASE = db1;
            > CREATE SCHEMA schema1;
            > SET SCHEMA = schema1;
            > CREATE ROLE user1 WITH LOGIN PASSWORD 'password';
            > CREATE TABLE t1 (c int);
            > GRANT USAGE ON SCHEMA schema1 TO user1;
            > GRANT SELECT ON TABLE t1 TO user1;
            """
            )
        )

    # Set testdrive to connect via the user1 role then verify password auth
    with c.override(
        Testdrive(
            materialize_url="postgres://user1:password@mz_old:6875",
            materialize_url_internal="postgres://materialize@mz_old:6877",
            mz_service="mz_old",
            no_reset=True,
        )
    ):
        c.up(Service("testdrive", idle=True))
        verify_password_auth(c)

    # Commence a 0dt upgrade
    with c.override(
        Materialized(
            name="mz_new",
            image=f"materialize/materialized:{new_version}",
            deploy_generation=1,
            **PASSWORD_AUTH_MATERIALIZED_ARGS,
        ),
        Testdrive(
            materialize_url="postgres://user1:password@mz_new:6875",
            materialize_url_internal="postgres://materialize@mz_new:6877",
            mz_service="mz_new",
            no_reset=True,
        ),
    ):
        c.up("mz_new", Service("testdrive", idle=True))
        c.await_mz_deployment_status(
            DeploymentStatus.READY_TO_PROMOTE,
            "mz_new",
        )
        c.promote_mz(
            "mz_new",
        )
        c.await_mz_deployment_status(
            DeploymentStatus.IS_LEADER,
            "mz_new",
        )

        # Verify that password auth continues to work
        verify_password_auth(c)


def workflow_self_managed_v_25_2_upgrade_oldest_to_latest_patch_release(
    c: Composition,
) -> None:
    """
    Test upgrading from previous v25.2 patch releases to the latest v25.2 patch release, verifying that password
    authentication continues to work properly throughout the upgrade process.
    """
    self_managed_versions = fetch_self_managed_versions()
    v25_2_versions = sorted(
        [
            v.version
            for v in self_managed_versions
            if v.helm_version.major == 25 and v.helm_version.minor == 2
        ]
    )

    latest_v25_2 = v25_2_versions[-1]
    # Get all versions up to latest v25.2
    previous_patch_releases = v25_2_versions[0:-2]

    # Test upgrades through each version
    for patch_release in previous_patch_releases:
        print(f"Testing upgrade from {patch_release} to {latest_v25_2}")
        test_upgrade_then_rbac_check(c, patch_release, latest_v25_2)
