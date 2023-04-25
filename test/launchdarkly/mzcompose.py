# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from itertools import chain
from os import environ
from textwrap import dedent
from time import sleep
from typing import Any, List, Optional
from uuid import uuid1

import launchdarkly_api  # type: ignore
from launchdarkly_api.api import feature_flags_api  # type: ignore
from launchdarkly_api.model.client_side_availability_post import (  # type: ignore
    ClientSideAvailabilityPost,
)
from launchdarkly_api.model.defaults import Defaults  # type: ignore
from launchdarkly_api.model.feature_flag_body import FeatureFlagBody  # type: ignore
from launchdarkly_api.model.json_patch import JSONPatch  # type: ignore
from launchdarkly_api.model.patch_operation import PatchOperation  # type: ignore
from launchdarkly_api.model.patch_with_comment import PatchWithComment  # type: ignore
from launchdarkly_api.model.variation import Variation  # type: ignore

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    DEFAULT_MZ_ENVIRONMENT_ID,
    Materialized,
    Testdrive,
)
from materialize.ui import UIError

# Access keys required for interacting with LaunchDarkly.
LAUNCHDARKLY_API_TOKEN = environ.get("LAUNCHDARKLY_API_TOKEN")
LAUNCHDARKLY_SDK_KEY = environ.get("LAUNCHDARKLY_SDK_KEY")

# We need those to derive feature flag name that guarantees that we won't have
# collisions between runs.
BUILDKITE_JOB_ID = environ.get("BUILDKITE_JOB_ID", uuid1())
BUILDKITE_PULL_REQUEST = environ.get("BUILDKITE_PULL_REQUEST")

# This should always coincide with the MZ_ENVIRONMENT_ID value passed to the
# Materialize service.
LD_CONTEXT_KEY = DEFAULT_MZ_ENVIRONMENT_ID
# A unique feature flag key to use for this test.
LD_FEATURE_FLAG_KEY = f"ci-test-{BUILDKITE_JOB_ID}"

SERVICES = [
    Materialized(
        environment_extra=[
            f"MZ_LAUNCHDARKLY_SDK_KEY={LAUNCHDARKLY_SDK_KEY}",
            f"MZ_LAUNCHDARKLY_KEY_MAP=max_result_size={LD_FEATURE_FLAG_KEY}",
            "MZ_LOG_FILTER=mz_adapter::catalog=debug,mz_adapter::config=debug",
            "MZ_CONFIG_SYNC_LOOP_INTERVAL=1s",
        ]
    ),
    Testdrive(no_reset=True, seed=1),
]


def workflow_default(c: Composition) -> None:
    if LAUNCHDARKLY_API_TOKEN is None:
        raise UIError("Missing LAUNCHDARKLY_API_TOKEN environment variable")
    if LAUNCHDARKLY_SDK_KEY is None:
        raise UIError("Missing LAUNCHDARKLY_SDK_KEY environment variable")

    # Create a LaunchDarkly client that simulates somebody interacting
    # with the LaunchDarkly frontend.
    ld_client = LaunchDarklyClient(
        configuration=launchdarkly_api.Configuration(
            api_key=dict(ApiKey=LAUNCHDARKLY_API_TOKEN),
        ),
        project_key="default",
        environment_key="ci-cd",
    )

    try:
        c.up("testdrive", persistent=True)

        # Assert that the default max_result_size is served when sync is disabled.
        with c.override(Materialized()):
            c.up("materialized")
            c.testdrive("\n".join(["> SHOW max_result_size", "1073741824"]))
            c.stop("materialized")

        # Create a test feature flag unique for this test run. Based on the
        # MZ_LAUNCHDARKLY_KEY_MAP value, the test feature will be mapped to the
        # max_result_size system parameter.
        ld_client.create_flag(
            LD_FEATURE_FLAG_KEY,
            tags=["ci-test", f"gh-{BUILDKITE_PULL_REQUEST}"]
            if BUILDKITE_PULL_REQUEST
            else ["ci-test"],
        )
        # Turn on targeting. The default rule will now serve 2GiB for the test
        # feature.
        ld_client.update_targeting(
            LD_FEATURE_FLAG_KEY,
            on=True,
        )

        # 3 seconds should be enough to avoid race conditions between the update
        # above and the query below.
        sleep(3)

        # Assert that the value is as expected after the initial parameter sync.
        with c.override(
            Materialized(
                environment_extra=[
                    f"MZ_LAUNCHDARKLY_SDK_KEY={LAUNCHDARKLY_SDK_KEY}",
                    f"MZ_LAUNCHDARKLY_KEY_MAP=max_result_size={LD_FEATURE_FLAG_KEY}",
                    "MZ_LOG_FILTER=mz_adapter::catalog=debug,mz_adapter::config=debug",
                ]
            )
        ):
            c.up("materialized")
            c.testdrive("\n".join(["> SHOW max_result_size", "2147483648"]))
            c.stop("materialized")

        # Assert that the last value is persisted and available upon restart,
        # even if the parameter sync loop is not running.
        with c.override(Materialized()):
            c.up("materialized")
            c.testdrive("\n".join(["> SHOW max_result_size", "2147483648"]))
            c.stop("materialized")

        # Restart Materialized with the parameter sync loop running.
        c.up("materialized")

        # Add a rule that targets the current user with the 3GiB variant.
        ld_client.update_targeting(
            LD_FEATURE_FLAG_KEY,
            contextTargets=[
                {
                    "contextKind": "environment",
                    "values": [LD_CONTEXT_KEY],
                    "variation": 2,
                }
            ],
        )

        # Assert that max_result_size is 3 GiB.
        c.testdrive("\n".join(["> SHOW max_result_size", "3221225472"]))

        # Add a rule that targets the current user with the 4GiB - 1 byte variant.
        ld_client.update_targeting(
            LD_FEATURE_FLAG_KEY,
            contextTargets=[
                {
                    "contextKind": "environment",
                    "values": [LD_CONTEXT_KEY],
                    "variation": 3,
                }
            ],
        )

        # Assert that max_result_size is 4 GiB - 1 byte.
        c.testdrive("\n".join(["> SHOW max_result_size", "4294967295"]))

        def sys(command: str):
            c.testdrive(
                "\n".join(
                    [
                        "$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}",
                        "$ postgres-execute connection=mz_system",
                        command,
                    ]
                )
            )

        # Assert that we can turn off synchronization
        sys("ALTER SYSTEM SET enable_launchdarkly=off")
        sys("ALTER SYSTEM SET max_result_size=1234")
        # The new value should not be replaced, even after 15 seconds
        sleep(15)
        c.testdrive("\n".join(["> SHOW max_result_size", "1234"]))
        # The value should be reset after we turn the kill switch back off
        sys("ALTER SYSTEM SET enable_launchdarkly=on")
        c.testdrive("\n".join(["> SHOW max_result_size", "4294967295"]))

        # Remove custom targeting.
        ld_client.update_targeting(
            LD_FEATURE_FLAG_KEY,
            contextTargets=[],
        )

        # Assert that max_result_size is 2 GiB (the default when targeting is
        # turned on).
        c.testdrive("\n".join(["> SHOW max_result_size", "2147483648"]))

        # Disable targeting.
        ld_client.update_targeting(
            LD_FEATURE_FLAG_KEY,
            on=False,
        )

        # Assert that max_result_size is 1 GiB (the default when targeting is
        # turned off).
        c.testdrive("\n".join(["> SHOW max_result_size", "1073741824"]))
        c.stop("materialized")
    except launchdarkly_api.ApiException as e:
        raise UIError(
            dedent(
                f"""
                Error when calling the Launch Darkly API.
                - Status: {e.status},
                - Reason: {e.reason},
                """
            )
        )
    finally:
        try:
            ld_client.delete_flag(LD_FEATURE_FLAG_KEY)
        except:
            pass  # ignore exceptions on cleanup


class LaunchDarklyClient:
    """
    A test-specific LaunchDarkly client that simulates a client modifying
    a LaunchDarkly configuration.
    """

    def __init__(
        self,
        configuration: launchdarkly_api.Configuration,
        project_key: str,
        environment_key: str,
    ) -> None:
        self.configuration = configuration
        self.project_key = project_key
        self.environment_key = environment_key

    def create_flag(self, feature_flag_key: str, tags: List[str] = []) -> Any:
        with launchdarkly_api.ApiClient(self.configuration) as api_client:
            api = feature_flags_api.FeatureFlagsApi(api_client)
            return api.post_feature_flag(
                project_key=self.project_key,
                feature_flag_body=FeatureFlagBody(
                    name=feature_flag_key,
                    key=feature_flag_key,
                    client_side_availability=ClientSideAvailabilityPost(
                        using_environment_id=True,
                        using_mobile_key=True,
                    ),
                    variations=[
                        Variation(value=1073741824, name="1 GiB"),
                        Variation(value=2147483648, name="2 GiB"),
                        Variation(value=3221225472, name="3 GiB"),
                        Variation(value=4294967295, name="4 GiB - 1 (max size)"),
                    ],
                    temporary=False,
                    tags=tags,
                    defaults=Defaults(
                        off_variation=0,
                        on_variation=1,
                    ),
                ),
            )

    def update_targeting(
        self,
        feature_flag_key: str,
        on: Optional[bool] = None,
        contextTargets: Optional[List[Any]] = None,
    ) -> Any:
        with launchdarkly_api.ApiClient(self.configuration) as api_client:
            api = feature_flags_api.FeatureFlagsApi(api_client)
            return api.patch_feature_flag(
                project_key=self.project_key,
                feature_flag_key=feature_flag_key,
                patch_with_comment=PatchWithComment(
                    patch=JSONPatch(
                        list(
                            chain(
                                [
                                    PatchOperation(
                                        op="replace",
                                        path=f"/environments/{self.environment_key}/on",
                                        value=on,
                                    )
                                ]
                                if on is not None
                                else [],
                                [
                                    PatchOperation(
                                        op="replace",
                                        path=f"/environments/{self.environment_key}/contextTargets",
                                        value=contextTargets,
                                    ),
                                ]
                                if contextTargets is not None
                                else [],
                            )
                        )
                    )
                ),
            )

    def delete_flag(self, feature_flag_key: str) -> Any:
        with launchdarkly_api.ApiClient(self.configuration) as api_client:
            api = feature_flags_api.FeatureFlagsApi(api_client)
            return api.delete_feature_flag(self.project_key, feature_flag_key)
