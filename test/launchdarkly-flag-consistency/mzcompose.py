# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Verify that the LaunchDarkly project and the set of synchronized system
parameters in Materialize are kept in sync.

Every synchronized system parameter (system variables, dyncfgs and feature
flags -- everything returned by `SystemVars::iter_synced()`) is pulled from
LaunchDarkly at runtime by the `SystemParameterFrontend`, keyed by the
parameter name (see `src/adapter/src/config/frontend.rs`). If a parameter has
no corresponding LaunchDarkly flag, `client.variation` silently falls back to
the compiled-in default, which means the flag can never be controlled in
production. This check catches that situation early:

  * ERROR  -- a synchronized parameter exists in Materialize but has no flag in
              LaunchDarkly. This fails the build.
  * WARN   -- a flag exists in LaunchDarkly but is no longer a synchronized
              parameter in Materialize. We only warn (and never fail) because
              the flag might still be required by a deployed older version. To
              avoid false positives we consider both the current build and the
              last published release.

The set of synchronized parameters is obtained by booting Materialize and
running `SHOW ALL` as `mz_system`, then filtering out the per-session variables
(which are not synchronized) and `enable_launchdarkly` (the kill switch, which
is explicitly excluded from `iter_synced`).

Caveat: `SHOW ALL` hides a parameter that is gated behind a *disabled*
feature flag (`VarDefinition::require_feature_flag`). No system parameter uses
that today, but if one is ever added it would be silently omitted here and
should be enabled before running this check (or added to a small allowlist).
"""

import os
from collections.abc import Iterable

import launchdarkly_api  # type: ignore
from launchdarkly_api.api import feature_flags_api  # type: ignore

from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.ui import UIError
from materialize.version_list import get_latest_published_version

# Access token required for reading the LaunchDarkly configuration.
LAUNCHDARKLY_API_TOKEN = os.environ.get("LAUNCHDARKLY_API_TOKEN")

# The LaunchDarkly project and environment that mirror Materialize's system
# parameters. Overridable so the same check can be pointed at a staging project.
LAUNCHDARKLY_PROJECT = os.environ.get("LAUNCHDARKLY_PROJECT", "default")
LAUNCHDARKLY_ENVIRONMENT = os.environ.get("LAUNCHDARKLY_ENVIRONMENT", "production")

# The internal SQL port, on which we can connect as `mz_system`.
MZ_SYSTEM_PORT = 6877

SERVICES = [
    # Booted in unsafe mode so that internal and unsafe parameters are also
    # visible via `SHOW ALL` as `mz_system`. The image is overridden per run to
    # also collect parameters from the last published release.
    Materialized(),
]

# Per-session variables returned by `SHOW ALL` that are *not* synchronized from
# LaunchDarkly and therefore must not be expected to have a flag.
#
# Keep this in sync with the session variables defined in
# `src/sql/src/session/vars.rs`:
#   * the `vars` array in `SessionVars::new`,
#   * the `SESSION_SYSTEM_VARS` set, and
#   * the `mz_version` / `is_superuser` pseudo-variables added by
#     `SessionVars::iter`.
# Matched case-insensitively, so the historical capitalization of e.g.
# `DateStyle` does not matter.
SESSION_VARIABLES = {
    # SessionVars::new pure-session variables.
    "failpoints",
    "server_version",
    "server_version_num",
    "sql_safe_updates",
    "real_time_recency",
    "emit_plan_insights_notice",
    "emit_timestamp_notice",
    "emit_trace_id_notice",
    "auto_route_catalog_queries",
    "enable_session_rbac_checks",
    "restrict_to_user_objects",
    "enable_session_cardinality_estimates",
    "max_identifier_length",
    "statement_logging_sample_rate",
    "emit_introspection_query_notice",
    "unsafe_new_transaction_wall_time",
    "welcome_message",
    # SESSION_SYSTEM_VARS.
    "application_name",
    "client_encoding",
    "client_min_messages",
    "cluster",
    "cluster_replica",
    "default_cluster_replication_factor",
    "current_object_missing_warnings",
    "database",
    "datestyle",
    "extra_float_digits",
    "integer_datetimes",
    "intervalstyle",
    "real_time_recency_timeout",
    "search_path",
    "standard_conforming_strings",
    "statement_timeout",
    "idle_in_transaction_session_timeout",
    "timezone",
    "transaction_isolation",
    "max_query_result_size",
    # Pseudo-variables added by SessionVars::iter.
    "mz_version",
    "is_superuser",
    # The LaunchDarkly kill switch, explicitly excluded from iter_synced.
    "enable_launchdarkly",
}

# Tag used by `test/launchdarkly` and `ci/cleanup/launchdarkly.py` for the
# throwaway flags created during the integration test. These are never real
# system parameters, so we ignore them when warning about stale flags.
CI_TEST_TAG = "ci-test"

# Known synchronized parameters that intentionally have no LaunchDarkly flag.
# Should normally be empty; add a name here (with a comment explaining why) only
# if a parameter is deliberately not managed via LaunchDarkly.
IGNORED_MZ_PARAMETERS: set[str] = set()


def synced_parameters(c: Composition) -> set[str]:
    """Return the set of synchronized system parameter names of the running
    `materialized` service, derived from `SHOW ALL` as `mz_system`."""
    rows = c.sql_query("SHOW ALL", user="mz_system", port=MZ_SYSTEM_PORT)
    names = {row[0] for row in rows}
    return {
        name
        for name in names
        if name.lower() not in SESSION_VARIABLES and name not in IGNORED_MZ_PARAMETERS
    }


def collect_synced_parameters(
    c: Composition, image: str | None, label: str
) -> set[str] | None:
    """Boot `materialized` (optionally pinned to `image`) and collect its
    synchronized parameters. Returns `None` if the service could not be booted,
    which is treated as best-effort for older releases."""
    try:
        with c.override(Materialized(image=image, sanity_restart=False)):
            c.up("materialized")
            params = synced_parameters(c)
            c.stop("materialized")
        print(f"Collected {len(params)} synchronized parameters from {label}")
        return params
    except Exception as e:
        print(f"WARNING: could not collect parameters from {label}: {e}")
        return None


def launchdarkly_flags() -> dict[str, list[str]]:
    """Return all feature flags in the configured LaunchDarkly project as a
    mapping from flag key to its tags."""
    configuration = launchdarkly_api.Configuration(
        api_key=dict(ApiKey=LAUNCHDARKLY_API_TOKEN)
    )
    flags: dict[str, list[str]] = {}
    with launchdarkly_api.ApiClient(configuration) as api_client:
        api = feature_flags_api.FeatureFlagsApi(api_client)
        limit = 100
        offset = 0
        while True:
            page = api.get_feature_flags(
                LAUNCHDARKLY_PROJECT,
                env=LAUNCHDARKLY_ENVIRONMENT,
                summary=True,
                limit=limit,
                offset=offset,
            )
            items = page["items"]
            for flag in items:
                flags[flag["key"]] = list(flag.get("tags", []))
            if len(items) < limit:
                break
            offset += limit
    return flags


def report(title: str, names: Iterable[str]) -> None:
    print(f"--- {title}")
    for name in sorted(names):
        print(f"  {name}")


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--no-fail",
        action="store_true",
        help="Only warn (never fail) when a parameter is missing in LaunchDarkly.",
    )
    args = parser.parse_args()

    if LAUNCHDARKLY_API_TOKEN is None:
        raise UIError("Missing LAUNCHDARKLY_API_TOKEN environment variable")

    # 1. Synchronized parameters of the current build. Mandatory: this drives
    #    the error direction of the check.
    current = collect_synced_parameters(c, image=None, label="current build")
    if current is None:
        raise UIError("Could not boot the current build to collect parameters")

    # 2. Synchronized parameters of the last published release. Best-effort:
    #    used only to avoid warning about flags that a deployed older version
    #    still relies on.
    try:
        previous_version = get_latest_published_version()
        previous_image = f"materialize/materialized:v{previous_version}"
        previous = collect_synced_parameters(
            c, image=previous_image, label=f"release v{previous_version}"
        )
    except Exception as e:
        print(f"WARNING: could not determine the last published release: {e}")
        previous = None

    known = set(current)
    if previous is not None:
        known |= previous

    # 3. All flags currently defined in LaunchDarkly.
    try:
        ld = launchdarkly_flags()
    except launchdarkly_api.ApiException as e:
        raise UIError(
            f"Error calling the LaunchDarkly API (status={e.status}, reason={e.reason})"
        )
    print(
        f"Found {len(ld)} flags in LaunchDarkly project "
        f"'{LAUNCHDARKLY_PROJECT}' (env '{LAUNCHDARKLY_ENVIRONMENT}')"
    )

    ld_keys = set(ld)

    # ERROR: parameters in Materialize that are missing in LaunchDarkly.
    missing_in_ld = current - ld_keys

    # WARN: flags in LaunchDarkly that are not known to either the current build
    # or the last release (ignoring throwaway CI test flags).
    stale_in_ld = {key for key in ld_keys - known if CI_TEST_TAG not in ld[key]}

    if stale_in_ld:
        report(
            "WARNING: flags in LaunchDarkly that are not synchronized "
            "parameters in the current build or the last release",
            stale_in_ld,
        )
        print(
            "These flags are likely stale and could be archived in LaunchDarkly "
            "once no deployed version relies on them."
        )

    if missing_in_ld:
        report(
            "ERROR: synchronized parameters that are missing in LaunchDarkly",
            missing_in_ld,
        )
        message = (
            f"{len(missing_in_ld)} synchronized parameter(s) have no flag in "
            f"LaunchDarkly project '{LAUNCHDARKLY_PROJECT}'. They cannot be "
            "controlled in production until a flag is created."
        )
        if args.no_fail:
            print(f"WARNING: {message}")
        else:
            raise UIError(message)

    if not missing_in_ld and not stale_in_ld:
        print(
            "All synchronized parameters are defined in LaunchDarkly and no "
            "stale flags were found."
        )
