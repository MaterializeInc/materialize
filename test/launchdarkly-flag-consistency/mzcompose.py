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
production. This check reports three conditions:

  * ERROR  -- a synchronized parameter exists in Materialize but has no flag in
              LaunchDarkly. This fails the build (unless `--no-fail`).
  * WARN   -- a flag exists in LaunchDarkly but is no longer a synchronized
              parameter in Materialize. We only warn (and never fail) because
              the flag might still be required by a deployed older version. To
              avoid false positives we consider both the current build and the
              last published release.
  * WARN   -- a flag exists in both, but the value LaunchDarkly serves by
              default diverges from Materialize's compiled-in default. This is
              not necessarily a bug (it is how a default is intentionally
              overridden in production), but it is worth surfacing so that
              unintended drift is noticed.

The set of synchronized parameters (and their defaults) is obtained by booting
Materialize and running `SHOW ALL` as `mz_system`, then filtering out the
per-session variables (which are not synchronized) and `enable_launchdarkly`
(the kill switch, which is explicitly excluded from `iter_synced`).

Caveat: `SHOW ALL` hides a parameter that is gated behind a *disabled*
feature flag (`VarDefinition::require_feature_flag`). No system parameter uses
that today, but if one is ever added it would be silently omitted here and
should be enabled before running this check (or added to a small allowlist).
"""

import os
from collections.abc import Iterable
from typing import Any

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


class Flag:
    """A LaunchDarkly flag, reduced to what this check needs."""

    def __init__(self, tags: list[str], default: Any | None) -> None:
        self.tags = tags
        # The value LaunchDarkly serves to an unmatched context (targeting off
        # -> off variation, otherwise the fallthrough variation). `None` when it
        # could not be determined unambiguously (e.g. a percentage rollout).
        self.default = default


def synced_parameters(c: Composition) -> dict[str, str]:
    """Return the synchronized system parameters of the running `materialized`
    service as a mapping from name to its (default) value, derived from
    `SHOW ALL` as `mz_system`."""
    rows = c.sql_query("SHOW ALL", user="mz_system", port=MZ_SYSTEM_PORT)
    return {
        name: value
        for name, value, *_ in rows
        if name.lower() not in SESSION_VARIABLES and name not in IGNORED_MZ_PARAMETERS
    }


def collect_synced_parameters(
    c: Composition, image: str | None, label: str
) -> dict[str, str] | None:
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


def ld_served_default(flag: dict[str, Any]) -> Any | None:
    """Return the value LaunchDarkly serves to an unmatched context in the
    configured environment, or `None` if it cannot be determined (e.g. a
    percentage rollout). Mirrors what `client.variation` returns for a context
    that matches no specific targeting rule."""
    variations = flag.get("variations") or []
    env = (flag.get("environments") or {}).get(LAUNCHDARKLY_ENVIRONMENT)
    if env is None:
        return None
    if env.get("on"):
        index = (env.get("fallthrough") or {}).get("variation")
    else:
        index = env.get("off_variation")
    if index is None or not (0 <= index < len(variations)):
        return None
    return variations[index].get("value")


def launchdarkly_flags() -> dict[str, Flag]:
    """Return all feature flags in the configured LaunchDarkly project."""
    configuration = launchdarkly_api.Configuration(
        api_key=dict(ApiKey=LAUNCHDARKLY_API_TOKEN)
    )
    flags: dict[str, Flag] = {}
    with launchdarkly_api.ApiClient(configuration) as api_client:
        api = feature_flags_api.FeatureFlagsApi(api_client)
        limit = 100
        offset = 0
        while True:
            # `summary=False` so that per-environment targeting (needed to
            # determine the served default) is included.
            page = api.get_feature_flags(
                LAUNCHDARKLY_PROJECT,
                env=LAUNCHDARKLY_ENVIRONMENT,
                summary=False,
                limit=limit,
                offset=offset,
            ).to_dict()
            items = page.get("items") or []
            for flag in items:
                flags[flag["key"]] = Flag(
                    tags=list(flag.get("tags") or []),
                    default=ld_served_default(flag),
                )
            total = page.get("total_count")
            offset += len(items)
            if not items or (total is not None and offset >= total):
                break
    return flags


def values_diverge(mz_value: str, ld_value: Any) -> bool | None:
    """Return whether the Materialize default `mz_value` (a string, as printed
    by `SHOW`) diverges from the LaunchDarkly value `ld_value` (typed). Returns
    `None` when the two cannot be compared confidently, so that we never warn on
    a representation mismatch (e.g. `"1GB"` vs `1073741824`)."""
    if ld_value is None:
        return None
    if isinstance(ld_value, bool):
        truthy = {"on", "true", "t", "yes", "1"}
        falsy = {"off", "false", "f", "no", "0"}
        mz = mz_value.strip().lower()
        if mz in truthy:
            return ld_value is not True
        if mz in falsy:
            return ld_value is not False
        return None
    if isinstance(ld_value, int | float):
        try:
            return float(mz_value) != float(ld_value)
        except ValueError:
            # `mz_value` is not plainly numeric (e.g. has a unit); don't guess.
            return None
    if isinstance(ld_value, str):
        return ld_value.strip() != mz_value.strip()
    return None


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

    # 1. Synchronized parameters (and defaults) of the current build. Mandatory:
    #    this drives the error direction of the check.
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
        known |= set(previous)

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
    missing_in_ld = set(current) - ld_keys

    # WARN: flags in LaunchDarkly that are not known to either the current build
    # or the last release (ignoring throwaway CI test flags).
    stale_in_ld = {key for key in ld_keys - known if CI_TEST_TAG not in ld[key].tags}

    # WARN: flags present in both whose served default diverges from the
    # Materialize default.
    diverging_defaults = {}
    for name, mz_value in current.items():
        flag = ld.get(name)
        if flag is None or CI_TEST_TAG in flag.tags:
            continue
        if values_diverge(mz_value, flag.default):
            diverging_defaults[name] = (mz_value, flag.default)

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

    if diverging_defaults:
        print(
            "--- WARNING: flags whose LaunchDarkly default diverges from the "
            "Materialize default"
        )
        for name in sorted(diverging_defaults):
            mz_value, ld_value = diverging_defaults[name]
            print(f"  {name}: materialize={mz_value!r} launchdarkly={ld_value!r}")
        print(
            "A diverging default overrides the compiled-in value in production. "
            "Confirm this is intentional."
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

    if not missing_in_ld and not stale_in_ld and not diverging_defaults:
        print(
            "All synchronized parameters are defined in LaunchDarkly, with "
            "matching defaults, and no stale flags were found."
        )
