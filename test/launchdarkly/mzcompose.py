# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Test the LaunchDarkly integration, get configuration flags from LD.
"""

from itertools import chain
from os import environ
from textwrap import dedent
from time import sleep
from typing import Any
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

from materialize.mzcompose import DEFAULT_MZ_ENVIRONMENT_ID, DEFAULT_ORG_ID
from materialize.mzcompose.composition import Composition, Service
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive
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
# Unique feature flag keys for the scoped (per-cluster / per-replica) cases.
LD_OPTIMIZER_FLAG_KEY = f"ci-test-optimizer-{BUILDKITE_JOB_ID}"
LD_LGALLOC_FLAG_KEY = f"ci-test-lgalloc-{BUILDKITE_JOB_ID}"
# Gate flag for the scoped feature. The gate is itself an LD-synced parameter,
# so the sync loop is authoritative: a boot-time default is overwritten on the
# first tick by LD's value. Enable it through LD instead, so the gate sticks.
LD_SCOPED_GATE_FLAG_KEY = f"ci-test-scoped-gate-{BUILDKITE_JOB_ID}"
# A cluster-coherent (optimizer) parameter and a replica-local parameter, both
# declared scoped in their definitions.
OPTIMIZER_PARAM = "enable_eager_delta_joins"
LGALLOC_PARAM = "enable_lgalloc"
# The feature gate that turns on scoped evaluation.
SCOPED_GATE_PARAM = "enable_scoped_system_parameters"


def context_rule(
    context_kind: str, attribute: str, values: list[str], variation: int
) -> dict[str, Any]:
    """Builds a LaunchDarkly targeting rule that serves `variation` to contexts
    of `context_kind` whose `attribute` is in `values`."""
    return {
        "variation": variation,
        "clauses": [
            {
                "contextKind": context_kind,
                "attribute": attribute,
                "op": "in",
                "values": values,
                "negate": False,
            }
        ],
    }


# Boolean flag variations: index 0 is `false`, index 1 is `true`.
BOOL_VARIATIONS: list[Any] = [
    Variation(value=False, name="false"),
    Variation(value=True, name="true"),
]

SERVICES = [
    Materialized(
        environment_extra=[
            f"MZ_LAUNCHDARKLY_SDK_KEY={LAUNCHDARKLY_SDK_KEY}",
            f"MZ_LAUNCHDARKLY_KEY_MAP=max_result_size={LD_FEATURE_FLAG_KEY}",
            "MZ_CONFIG_SYNC_LOOP_INTERVAL=1s",
        ],
        additional_system_parameter_defaults={
            "log_filter": "mz_adapter::catalog=debug,mz_adapter::config=debug",
        },
        external_metadata_store=True,
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
        project_key=environ.get("LAUNCHDARKLY_PROJECT_KEY", "default"),
        environment_key=environ.get("LAUNCHDARKLY_ENVIRONMENT_KEY", "ci-cd"),
    )

    try:
        c.up(Service("testdrive", idle=True))

        # Assert that the default max_result_size is served when sync is disabled.
        with c.override(Materialized(external_metadata_store=True)):
            c.up("materialized")
            c.testdrive("\n".join(["> SHOW max_result_size", "1GB"]))
            c.stop("materialized")

        # Create a test feature flag unique for this test run. Based on the
        # MZ_LAUNCHDARKLY_KEY_MAP value, the test feature will be mapped to the
        # max_result_size system parameter.
        ld_client.create_flag(
            LD_FEATURE_FLAG_KEY,
            tags=(
                ["ci-test", f"gh-{BUILDKITE_PULL_REQUEST}"]
                if BUILDKITE_PULL_REQUEST
                else ["ci-test"]
            ),
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
                ],
                additional_system_parameter_defaults={
                    "log_filter": "mz_adapter::catalog=debug,mz_adapter::config=debug",
                },
                external_metadata_store=True,
            )
        ):
            c.up("materialized")
            c.testdrive("\n".join(["> SHOW max_result_size", "2GB"]))
            c.stop("materialized")

        # Assert that the last value is persisted and available upon restart,
        # even if the parameter sync loop is not running.
        with c.override(Materialized(external_metadata_store=True)):
            c.up("materialized")
            c.testdrive("\n".join(["> SHOW max_result_size", "2GB"]))
            c.stop("materialized")

        # Restart Materialized with the parameter sync loop running.
        c.up("materialized")

        # Add a rule that targets the current environment with the 4GiB - 1 byte variant.
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
        c.testdrive("\n".join(["> SHOW max_result_size", "4294967295B"]))

        # Add a rule that targets the current organization with the 3GiB
        # variant. Even though we don't delete the above rule (replicated as
        # first entry in the contextTargets list below), the evaluation order is
        # based on the definition order of flag variants.
        ld_client.update_targeting(
            LD_FEATURE_FLAG_KEY,
            contextTargets=[
                {
                    "contextKind": "environment",
                    "values": [LD_CONTEXT_KEY],
                    "variation": 3,
                },
                {
                    "contextKind": "organization",
                    "values": [DEFAULT_ORG_ID],
                    "variation": 2,
                },
            ],
        )

        # Assert that max_result_size is 3 GiB.
        c.testdrive("\n".join(["> SHOW max_result_size", "3GB"]))

        # Assert that we can turn off synchronization
        def sys(command: str) -> None:
            c.testdrive(
                "\n".join(
                    [
                        "$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}",
                        "$ postgres-execute connection=mz_system",
                        command,
                    ]
                )
            )

        # (1) The logs should report that the frontend was not stopped until now
        logs = c.invoke("logs", "materialized", capture=True)
        assert "stopping system parameter frontend" not in logs.stdout
        # (2) Turn the kill switch on
        sys("ALTER SYSTEM SET enable_launchdarkly=off")
        sleep(10)
        # (3) The logs should report that the frontend was stopped at least once
        logs = c.invoke("logs", "materialized", capture=True)
        assert "stopping system parameter frontend" in logs.stdout
        # (4) After that, it should be safe to alter a value directly.
        #     The new value should not be replaced, even after 15 seconds
        sys("ALTER SYSTEM SET max_result_size=1234567")
        sleep(15)
        c.testdrive("\n".join(["> SHOW max_result_size", "1234567B"]))
        # (5) The value should be reset after we turn the kill switch back off
        sys("ALTER SYSTEM SET enable_launchdarkly=on")
        c.testdrive("\n".join(["> SHOW max_result_size", "3GB"]))

        # Remove custom targeting.
        ld_client.update_targeting(
            LD_FEATURE_FLAG_KEY,
            contextTargets=[],
        )

        # Assert that max_result_size is 2 GiB (the default when targeting is
        # turned on).
        c.testdrive("\n".join(["> SHOW max_result_size", "2GB"]))

        # Disable targeting.
        ld_client.update_targeting(
            LD_FEATURE_FLAG_KEY,
            on=False,
        )

        # Assert that max_result_size is 1 GiB (the default when targeting is
        # turned off).
        c.testdrive("\n".join(["> SHOW max_result_size", "1GB"]))
        c.stop("materialized")

        # Exercise the scoped (per-cluster / per-replica) feature flags.
        run_scoped_feature_flag_cases(c, ld_client)
    except launchdarkly_api.ApiException as e:
        raise UIError(dedent(f"""
                Error when calling the Launch Darkly API.
                - Status: {e.status},
                - Reason: {e.reason},
                """))
    finally:
        try:
            ld_client.delete_flag(LD_FEATURE_FLAG_KEY)
        except:
            pass  # ignore exceptions on cleanup


def run_scoped_feature_flag_cases(
    c: Composition, ld_client: "LaunchDarklyClient"
) -> None:
    """Demonstrates per-cluster (cluster-coherent) and per-replica
    (replica-local) LaunchDarkly overrides, plus their durability across a
    restart.

    Cluster case: a cluster-coherent optimizer feature is served `true` to the
    builtin clusters (e.g. `mz_catalog_server`) and `false` to user clusters,
    targeted by the `cluster` context's `is_builtin` attribute.

    Replica case: the replica-local `enable_lgalloc` flag is served `false` only
    to replicas whose `replica_size_family` is `legacy`, while modern (`cc`)
    replicas keep the environment-wide value, targeted by the `replica`
    context's `replica_size_family` attribute.
    """
    # A materialized that syncs the scoped flags. The key map ties the
    # cluster-coherent optimizer parameter and the replica-local lgalloc
    # parameter to their LD flags, alongside the original max_result_size flag.
    # MZ_LAUNCHDARKLY_KEY_MAP entries are `;`-separated (the CLI arg's value
    # delimiter), not comma-separated.
    key_map = ";".join(
        [
            f"max_result_size={LD_FEATURE_FLAG_KEY}",
            f"{OPTIMIZER_PARAM}={LD_OPTIMIZER_FLAG_KEY}",
            f"{LGALLOC_PARAM}={LD_LGALLOC_FLAG_KEY}",
            f"{SCOPED_GATE_PARAM}={LD_SCOPED_GATE_FLAG_KEY}",
        ]
    )
    scoped_mz = Materialized(
        environment_extra=[
            f"MZ_LAUNCHDARKLY_SDK_KEY={LAUNCHDARKLY_SDK_KEY}",
            f"MZ_LAUNCHDARKLY_KEY_MAP={key_map}",
            "MZ_CONFIG_SYNC_LOOP_INTERVAL=1s",
        ],
        additional_system_parameter_defaults={
            "log_filter": "mz_adapter::catalog=debug,mz_adapter::config=debug",
        },
        external_metadata_store=True,
    )

    try:
        # Cluster-coherent optimizer flag. Fallthrough serves the env-wide
        # `false`. The rules carve out per-cluster opinions:
        #   * builtin clusters (`is_builtin`) and a same-named user cluster
        #     (`ld_role`, by name) are served `true`, which differs from env-wide,
        #     so it is recorded.
        #   * `quickstart` is served the env-wide `false`. That rule *matches*
        #     but the served value does not differ from env-wide, so it must NOT
        #     record a row. This guards the recording rule: a bare LD match is
        #     not, by itself, a cluster opinion. Only a *differing* value is.
        optimizer_rules = [
            context_rule("cluster", "is_builtin", ["true"], 1),
            context_rule("cluster", "cluster_name", ["ld_role", "ld_sync"], 1),
            context_rule("cluster", "cluster_name", ["quickstart"], 0),
        ]
        ld_client.create_flag(
            LD_OPTIMIZER_FLAG_KEY,
            tags=["ci-test"],
            variations=BOOL_VARIATIONS,
            off_variation=0,
            on_variation=0,
        )
        ld_client.update_targeting(
            LD_OPTIMIZER_FLAG_KEY,
            on=True,
            rules=optimizer_rules,
        )

        # Replica-local lgalloc flag: fallthrough serves `true` (the env-wide
        # default), a rule serves `false` to the legacy size family.
        ld_client.create_flag(
            LD_LGALLOC_FLAG_KEY,
            tags=["ci-test"],
            variations=BOOL_VARIATIONS,
            off_variation=0,
            on_variation=1,
        )
        ld_client.update_targeting(
            LD_LGALLOC_FLAG_KEY,
            on=True,
            rules=[context_rule("replica", "replica_size_family", ["legacy"], 0)],
        )

        # The scoped feature gate. Off by default, so opt in by serving `true`
        # env-wide. Enabled through LD rather than a boot-time default because
        # the gate is itself synced: the sync loop would clobber a boot default
        # with LD's value on the first tick.
        ld_client.create_flag(
            LD_SCOPED_GATE_FLAG_KEY,
            tags=["ci-test"],
            variations=BOOL_VARIATIONS,
            off_variation=0,
            on_variation=1,
        )
        ld_client.update_targeting(LD_SCOPED_GATE_FLAG_KEY, on=True)

        with c.override(scoped_mz):
            c.up("materialized")

            # A user cluster with a legacy-family replica (to contrast with the
            # builtin / `cc` defaults), plus `ld_role`, targeted by name to
            # exercise recreate semantics below.
            c.testdrive(
                "\n".join(
                    [
                        "$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}",
                        "$ postgres-execute connection=mz_system",
                        "CREATE CLUSTER ld_legacy SIZE 'scale=1,workers=1,legacy'",
                        "CREATE CLUSTER ld_role SIZE 'scale=1,workers=1,legacy'",
                    ]
                )
            )

            # Allow a few sync ticks to evaluate the new clusters/replicas.
            sleep(5)

            # Cluster case + differs-from-env guard. Recorded rows are sparse:
            # `mz_catalog_server` (is_builtin rule) and `ld_role` (name rule)
            # differ from the env-wide `false` and are recorded. `quickstart`
            # (rule serves the env-wide `false`) and `ld_legacy` (fallthrough)
            # carry no differing value, so they have no row. If either appears,
            # the recording rule has regressed to keying on the LD match.
            c.testdrive(
                "\n".join(
                    [
                        f"> SELECT c.name, p.value FROM mz_internal.mz_cluster_system_parameters p JOIN mz_clusters c ON c.id = p.cluster_id WHERE p.name = '{OPTIMIZER_PARAM}' AND c.name IN ('mz_catalog_server', 'quickstart', 'ld_legacy', 'ld_role') ORDER BY c.name",
                        "ld_role true",
                        "mz_catalog_server true",
                    ]
                )
            )

            # Replica case: the legacy-family replica's `false` differs from the
            # env-wide `true` and is recorded. The `cc`-family quickstart replica
            # falls through to the env-wide value, so it has no override row.
            c.testdrive(
                "\n".join(
                    [
                        f"> SELECT cr.name, p.value FROM mz_internal.mz_replica_system_parameters p JOIN mz_cluster_replicas cr ON cr.id = p.replica_id JOIN mz_clusters c ON c.id = cr.cluster_id WHERE p.name = '{LGALLOC_PARAM}' AND c.name IN ('ld_legacy', 'quickstart') ORDER BY c.name",
                        "r1 false",
                    ]
                )
            )

            # Recreate semantics: a `cluster_name` rule is a *role* predicate, so
            # it must re-apply to a recreated same-named cluster (which gets a
            # fresh id, never the old one). Drop and recreate `ld_role`. The
            # override returns under the new id.
            c.testdrive(
                "\n".join(
                    [
                        "$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}",
                        "$ postgres-execute connection=mz_system",
                        "DROP CLUSTER ld_role",
                        "CREATE CLUSTER ld_role SIZE 'scale=1,workers=1,legacy'",
                    ]
                )
            )
            sleep(5)
            c.testdrive(
                "\n".join(
                    [
                        f"> SELECT c.name, p.value FROM mz_internal.mz_cluster_system_parameters p JOIN mz_clusters c ON c.id = p.cluster_id WHERE p.name = '{OPTIMIZER_PARAM}' AND c.name = 'ld_role'",
                        "ld_role true",
                    ]
                )
            )

            # Bidirectional reconcile: the sync loop is the sole writer and must
            # *remove* an override once LD no longer serves a differing value.
            # Clear the rules so every cluster falls through to env-wide. All
            # rows for the parameter disappear. Restoring the rules brings them
            # back, exercising propagation in both directions.
            ld_client.update_targeting(LD_OPTIMIZER_FLAG_KEY, rules=[])
            sleep(5)
            c.testdrive(
                "\n".join(
                    [
                        f"> SELECT count(*) FROM mz_internal.mz_cluster_system_parameters WHERE name = '{OPTIMIZER_PARAM}'",
                        "0",
                    ]
                )
            )
            ld_client.update_targeting(
                LD_OPTIMIZER_FLAG_KEY,
                rules=optimizer_rules,
            )
            sleep(5)
            c.testdrive(
                "\n".join(
                    [
                        f"> SELECT c.name, p.value FROM mz_internal.mz_cluster_system_parameters p JOIN mz_clusters c ON c.id = p.cluster_id WHERE p.name = '{OPTIMIZER_PARAM}' AND c.name IN ('mz_catalog_server', 'ld_role') ORDER BY c.name",
                        "ld_role true",
                        "mz_catalog_server true",
                    ]
                )
            )

            # Feature gate. Turning `enable_scoped_system_parameters` off makes
            # the sync loop evaluate no scoped contexts and clear what it
            # previously persisted, so both collections empty out and resolution
            # reverts to env-wide. Turning it back on re-evaluates and restores.
            # Toggle through LD, not `ALTER SYSTEM`: the gate is itself a synced
            # parameter, so the sync loop is authoritative and would revert a
            # local override on the next tick.
            ld_client.update_targeting(LD_SCOPED_GATE_FLAG_KEY, on=False)
            sleep(5)
            c.testdrive(
                "\n".join(
                    [
                        "> SELECT count(*) FROM mz_internal.mz_cluster_system_parameters",
                        "0",
                        "> SELECT count(*) FROM mz_internal.mz_replica_system_parameters",
                        "0",
                    ]
                )
            )
            ld_client.update_targeting(LD_SCOPED_GATE_FLAG_KEY, on=True)
            sleep(5)
            c.testdrive(
                "\n".join(
                    [
                        f"> SELECT c.name, p.value FROM mz_internal.mz_cluster_system_parameters p JOIN mz_clusters c ON c.id = p.cluster_id WHERE p.name = '{OPTIMIZER_PARAM}' AND c.name IN ('mz_catalog_server', 'ld_role') ORDER BY c.name",
                        "ld_role true",
                        "mz_catalog_server true",
                        f"> SELECT cr.name, p.value FROM mz_internal.mz_replica_system_parameters p JOIN mz_cluster_replicas cr ON cr.id = p.replica_id JOIN mz_clusters c ON c.id = cr.cluster_id WHERE p.name = '{LGALLOC_PARAM}' AND c.name = 'ld_legacy'",
                        "r1 false",
                    ]
                )
            )
            c.stop("materialized")

        # Durability: restart without the sync loop and assert the scoped values
        # are restored from the durable cache, including the recreated
        # `ld_role` cluster override and the legacy replica override.
        with c.override(Materialized(external_metadata_store=True)):
            c.up("materialized")
            c.testdrive(
                "\n".join(
                    [
                        f"> SELECT c.name, p.value FROM mz_internal.mz_cluster_system_parameters p JOIN mz_clusters c ON c.id = p.cluster_id WHERE p.name = '{OPTIMIZER_PARAM}' AND c.name IN ('mz_catalog_server', 'ld_role') ORDER BY c.name",
                        "ld_role true",
                        "mz_catalog_server true",
                        f"> SELECT cr.name, p.value FROM mz_internal.mz_replica_system_parameters p JOIN mz_cluster_replicas cr ON cr.id = p.replica_id JOIN mz_clusters c ON c.id = cr.cluster_id WHERE p.name = '{LGALLOC_PARAM}' AND c.name IN ('ld_legacy', 'quickstart') ORDER BY c.name",
                        "r1 false",
                    ]
                )
            )
            c.stop("materialized")

        # Create-time resolution: a cluster and its replicas created between sync
        # ticks must resolve their overrides synchronously, before the cluster's
        # first plan (cluster-coherent, optimizer features are baked into
        # immutable dataflows) and before the replica renders (replica-local,
        # render-frozen flags like the column-paged batcher). We isolate this from
        # the periodic loop by running with a very long sync interval: the
        # frontend is still installed on the first (immediate) tick, but no
        # further reconcile happens during the test, so a row that appears right
        # after `CREATE CLUSTER` can only come from create-time resolution.
        # (testdrive `>` retries, but the periodic loop cannot fire within the
        # retry window at this interval.)
        slow_mz = Materialized(
            environment_extra=[
                f"MZ_LAUNCHDARKLY_SDK_KEY={LAUNCHDARKLY_SDK_KEY}",
                f"MZ_LAUNCHDARKLY_KEY_MAP={key_map}",
                "MZ_CONFIG_SYNC_LOOP_INTERVAL=3600s",
            ],
            additional_system_parameter_defaults={
                "enable_scoped_system_parameters": "true",
            },
            external_metadata_store=True,
        )
        with c.override(slow_mz):
            c.up("materialized")
            c.testdrive(
                "\n".join(
                    [
                        "$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}",
                        "$ postgres-execute connection=mz_system",
                        "CREATE CLUSTER ld_sync SIZE 'scale=1,workers=1,legacy'",
                    ]
                )
            )
            c.testdrive(
                "\n".join(
                    [
                        f"> SELECT c.name, p.value FROM mz_internal.mz_cluster_system_parameters p JOIN mz_clusters c ON c.id = p.cluster_id WHERE p.name = '{OPTIMIZER_PARAM}' AND c.name = 'ld_sync'",
                        "ld_sync true",
                    ]
                )
            )
            # Create-time replica resolution: `ld_sync` is a legacy-family
            # cluster, so its replica is served `enable_lgalloc=false`, which
            # differs from the env-wide `true` and is recorded. The override is
            # folded into the create transaction, so the row appears immediately,
            # before the (disabled) sync loop could write it. This is the
            # replica-local counterpart to the cluster-coherent case above, and it
            # would not appear if the replica's override were resolved only after
            # the create transaction.
            c.testdrive(
                "\n".join(
                    [
                        f"> SELECT cr.name, p.value FROM mz_internal.mz_replica_system_parameters p JOIN mz_cluster_replicas cr ON cr.id = p.replica_id JOIN mz_clusters c ON c.id = cr.cluster_id WHERE p.name = '{LGALLOC_PARAM}' AND c.name = 'ld_sync'",
                        "r1 false",
                    ]
                )
            )
            c.stop("materialized")
    finally:
        for flag in (
            LD_OPTIMIZER_FLAG_KEY,
            LD_LGALLOC_FLAG_KEY,
            LD_SCOPED_GATE_FLAG_KEY,
        ):
            try:
                ld_client.delete_flag(flag)
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

    def create_flag(
        self,
        feature_flag_key: str,
        tags: list[str] = [],
        variations: list[Any] | None = None,
        off_variation: int = 0,
        on_variation: int = 1,
    ) -> Any:
        if variations is None:
            variations = [
                Variation(value=1073741824, name="1 GiB"),
                Variation(value=2147483648, name="2 GiB"),
                Variation(value=3221225472, name="3 GiB"),
                Variation(value=4294967295, name="4 GiB - 1 (max size)"),
            ]
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
                    variations=variations,
                    temporary=False,
                    tags=tags,
                    defaults=Defaults(
                        off_variation=off_variation,
                        on_variation=on_variation,
                    ),
                ),
            )

    def update_targeting(
        self,
        feature_flag_key: str,
        on: bool | None = None,
        contextTargets: list[Any] | None = None,
        rules: list[Any] | None = None,
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
                                (
                                    [
                                        PatchOperation(
                                            op="replace",
                                            path=f"/environments/{self.environment_key}/on",
                                            value=on,
                                        )
                                    ]
                                    if on is not None
                                    else []
                                ),
                                (
                                    [
                                        PatchOperation(
                                            op="replace",
                                            path=f"/environments/{self.environment_key}/contextTargets",
                                            value=contextTargets,
                                        ),
                                    ]
                                    if contextTargets is not None
                                    else []
                                ),
                                (
                                    [
                                        PatchOperation(
                                            op="replace",
                                            path=f"/environments/{self.environment_key}/rules",
                                            value=rules,
                                        ),
                                    ]
                                    if rules is not None
                                    else []
                                ),
                            )
                        )
                    )
                ),
            )

    def delete_flag(self, feature_flag_key: str) -> Any:
        with launchdarkly_api.ApiClient(self.configuration) as api_client:
            api = feature_flags_api.FeatureFlagsApi(api_client)
            return api.delete_feature_flag(self.project_key, feature_flag_key)
