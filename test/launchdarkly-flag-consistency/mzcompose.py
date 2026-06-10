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
production. This check reports four conditions:

  * ERROR  -- a synchronized parameter exists in Materialize but has no flag in
              LaunchDarkly. This fails the build (unless `--no-fail`).
  * WARN   -- a flag exists in LaunchDarkly but is no longer a synchronized
              parameter in Materialize. We only warn (and never fail) because
              the flag might still be required by a deployed older version. To
              avoid false positives we consider both the current build and the
              last published release.
  * WARN   -- a flag exists in both, but the value LaunchDarkly serves by
              default in the production environment differs from Materialize's
              compiled-in default. Self-managed deployments have no LaunchDarkly
              and run on the compiled-in default, so this means cloud and
              self-managed behave differently (e.g. a feature enabled in cloud
              but still off by default in self-managed). Parameters that
              deliberately tune cloud-only infrastructure are listed in
              `INTENTIONAL_LD_OVERRIDES` and excluded.
  * WARN   -- a flag's served default differs *between* LaunchDarkly
              environments (e.g. production vs staging), which usually only
              happens during a staged rollout.

Note: not every synchronized parameter has (or needs) a LaunchDarkly flag --
many just ride their compiled-in default. The check therefore runs with
`--no-fail` in CI for now, so the missing-flag condition only warns; flip to a
hard failure once the existing backlog has been triaged.

The set of synchronized parameters (and their defaults) is obtained by booting
Materialize -- with an empty `system_parameter_defaults` and no LaunchDarkly SDK
key, so that `SHOW ALL` reports the compiled-in defaults rather than mzcompose's
CI overrides or LaunchDarkly-synced values -- and running `SHOW ALL` as
`mz_system`, then filtering out the per-session variables (which are not
synchronized) and `enable_launchdarkly` (the kill switch, which is explicitly
excluded from `iter_synced`).

Caveat: `SHOW ALL` hides a parameter that is gated behind a *disabled*
feature flag (`VarDefinition::require_feature_flag`). No system parameter uses
that today, but if one is ever added it would be silently omitted here and
should be enabled before running this check (or added to a small allowlist).
"""

import itertools
import math
import os
import re
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

# The LaunchDarkly project that mirrors Materialize's system parameters.
# Overridable so the same check can be pointed at a different project.
LAUNCHDARKLY_PROJECT = os.environ.get("LAUNCHDARKLY_PROJECT", "default")

# The LaunchDarkly environments to check default divergence against. Flag
# *existence* is project-level (identical across environments), so the
# missing/stale checks do not depend on the environment; only the served
# default value can differ per environment. The first environment is treated as
# the production / cloud baseline that is compared against the compiled-in
# default (i.e. what self-managed deployments use).
LAUNCHDARKLY_ENVIRONMENTS = [
    env.strip()
    for env in os.environ.get("LAUNCHDARKLY_ENVIRONMENTS", "production,staging").split(
        ","
    )
    if env.strip()
]
PRODUCTION_ENVIRONMENT = (
    LAUNCHDARKLY_ENVIRONMENTS[0] if LAUNCHDARKLY_ENVIRONMENTS else ""
)

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

# Parameters whose production LaunchDarkly value is *deliberately* different from
# the compiled-in default, because they tune cloud-specific infrastructure that
# does not apply to self-managed deployments. These are excluded from the
# "cloud vs self-managed" divergence warning (but still participate in the
# cross-environment prod-vs-staging check). Curate this list; everything else
# that diverges is surfaced so a feature accidentally left off in self-managed
# is noticed.
INTENTIONAL_LD_OVERRIDES: set[str] = {
    # Cloud replica expiration (confirmed cloud-only behavior).
    "compute_replica_expiration_offset",
    "enable_compute_replica_expiration",
    # Cloud resource quotas / available replica sizes.
    "allowed_cluster_replica_sizes",
    "max_clusters",
    "max_connections",
    "max_credit_consumption_rate",
    "max_aws_privatelink_connections",
    "max_materialized_views",
    "max_sources",
    "max_tables",
}


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
        # Start from a clean slate: the previous-release binary cannot read
        # persist/catalog state written by the newer current build (and vice
        # versa), so destroy any volumes left over from an earlier collection.
        c.down(destroy_volumes=True, sanity_restart_mz=False)
        with c.override(
            Materialized(
                image=image,
                sanity_restart=False,
                # Read Materialize's *compiled-in* defaults: pass an empty
                # `system_parameter_defaults` so mzcompose does not apply its CI
                # test overrides, and don't set an SDK key so the instance does
                # not sync from LaunchDarkly. `SHOW ALL` then reflects the value
                # environmentd falls back to when a flag is absent from
                # LaunchDarkly, which is what the divergence check compares
                # against.
                system_parameter_defaults={},
            )
        ):
            c.up("materialized")
            params = synced_parameters(c)
            c.stop("materialized")
        print(f"Collected {len(params)} synchronized parameters from {label}")
        return params
    except Exception as e:
        print(f"WARNING: could not collect parameters from {label}: {e}")
        return None


def ld_served_default(flag: dict[str, Any], environment: str) -> Any | None:
    """Return the value LaunchDarkly serves to an unmatched context in the given
    environment, or `None` if it cannot be determined (e.g. a percentage
    rollout). Mirrors what `client.variation` returns for a context that matches
    no specific targeting rule."""
    variations = flag.get("variations") or []
    env = (flag.get("environments") or {}).get(environment)
    if env is None:
        return None
    if env.get("on"):
        index = (env.get("fallthrough") or {}).get("variation")
    else:
        index = env.get("off_variation")
    if index is None or not (0 <= index < len(variations)):
        return None
    return variations[index].get("value")


def launchdarkly_flag_tags(api: Any) -> dict[str, list[str]]:
    """Return every flag in the configured LaunchDarkly project as a mapping
    from flag key to its tags. Uses the (summary) list endpoint, which is cheap
    but does not include per-environment targeting."""
    flags: dict[str, list[str]] = {}
    limit = 100
    offset = 0
    while True:
        page = api.get_feature_flags(
            LAUNCHDARKLY_PROJECT,
            summary=True,
            limit=limit,
            offset=offset,
        ).to_dict()
        items = page.get("items") or []
        for flag in items:
            flags[flag["key"]] = list(flag.get("tags") or [])
        total = page.get("total_count")
        offset += len(items)
        if not items or (total is not None and offset >= total):
            break
    return flags


def launchdarkly_served_defaults(
    api: Any, keys: Iterable[str]
) -> tuple[dict[str, dict[str, Any | None]], set[str]]:
    """Return, for each requested flag key, the value LaunchDarkly serves by
    default per checked environment. Fetches each flag individually because the
    list endpoint only returns summaries, not the per-environment targeting
    needed to determine the served default. Also returns the set of environment
    keys actually seen, so a misconfigured environment name can be detected."""
    defaults: dict[str, dict[str, Any | None]] = {}
    env_keys_seen: set[str] = set()
    for key in sorted(keys):
        try:
            flag = api.get_feature_flag(LAUNCHDARKLY_PROJECT, key).to_dict()
        except launchdarkly_api.ApiException as e:
            print(f"WARNING: could not fetch LaunchDarkly flag {key!r}: {e}")
            continue
        env_keys_seen.update((flag.get("environments") or {}).keys())
        defaults[key] = {
            environment: ld_served_default(flag, environment)
            for environment in LAUNCHDARKLY_ENVIRONMENTS
        }
    return defaults, env_keys_seen


# Time units accepted in duration values, expressed in seconds. Covers both the
# spaced form printed by Materialize's SHOW (e.g. "1 min", "30 d") and the
# humantime form stored in LaunchDarkly (e.g. "60s", "525600min", "1200ms").
_DURATION_UNITS_SECONDS = {
    "ns": 1e-9,
    "us": 1e-6,
    "µs": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "sec": 1.0,
    "secs": 1.0,
    "second": 1.0,
    "seconds": 1.0,
    "m": 60.0,
    "min": 60.0,
    "mins": 60.0,
    "minute": 60.0,
    "minutes": 60.0,
    "h": 3600.0,
    "hr": 3600.0,
    "hour": 3600.0,
    "hours": 3600.0,
    "d": 86400.0,
    "day": 86400.0,
    "days": 86400.0,
}

_DURATION_TOKEN = re.compile(r"(\d+(?:\.\d+)?)\s*([a-zµ]+)")


def parse_duration_seconds(
    value: str, assume_bare_millis: bool = False
) -> float | None:
    """Parse a duration string into seconds, or return `None` if it is not a
    duration. Handles Materialize's spaced form ("1 min"), humantime's compact
    form ("525600min", "1h30m"), and -- when `assume_bare_millis` is set -- a
    bare number, which Materialize interprets as milliseconds for duration
    parameters (this is how some durations are stored in LaunchDarkly, e.g.
    "600000" for 10 minutes)."""
    v = value.strip().lower()
    if not v:
        return None
    if re.fullmatch(r"\d+(?:\.\d+)?", v):
        return float(v) / 1000.0 if assume_bare_millis else None
    tokens = _DURATION_TOKEN.findall(v)
    # Require the whole string to be made of duration tokens, so that e.g. a
    # cluster size like "M.1-8xlarge" is not mistaken for a duration.
    if not tokens or "".join(f"{n}{u}" for n, u in tokens) != v.replace(" ", ""):
        return None
    total = 0.0
    for number, unit in tokens:
        if unit not in _DURATION_UNITS_SECONDS:
            return None
        total += float(number) * _DURATION_UNITS_SECONDS[unit]
    return total


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"on", "true", "t", "yes", "1"}:
        return True
    if s in {"off", "false", "f", "no", "0"}:
        return False
    return None


def values_equivalent(a: Any, b: Any) -> bool | None:
    """Whether two values represent the same setting, comparing durations by
    value so representation differences ("1 min" vs "60s", "10 s" vs the
    raw-milliseconds "10000") are not treated as differences. `a` and `b` may be
    Materialize's `SHOW` strings or LaunchDarkly's typed values. Returns `None`
    when the two cannot be compared confidently (e.g. one side is missing, or a
    byte size like "1GB" vs the raw number 1073741824)."""
    if a is None or b is None:
        return None
    if isinstance(a, bool) or isinstance(b, bool):
        ba, bb = _as_bool(a), _as_bool(b)
        if ba is None or bb is None:
            return None
        return ba == bb
    sa, sb = str(a).strip(), str(b).strip()
    if sa == "" or sb == "":
        # Only equal if both are empty; an empty default vs a set value differs.
        return sa == sb
    da, db = parse_duration_seconds(sa), parse_duration_seconds(sb)
    if da is not None or db is not None:
        # At least one side is a duration with a unit; parse a bare number on
        # the other side as milliseconds (how Materialize reads it).
        if da is None:
            da = parse_duration_seconds(sa, assume_bare_millis=True)
        if db is None:
            db = parse_duration_seconds(sb, assume_bare_millis=True)
        if da is None or db is None:
            return None
        return math.isclose(da, db, rel_tol=1e-9, abs_tol=1e-12)
    try:
        return float(sa) == float(sb)
    except ValueError:
        return sa == sb


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
    #    still relies on. `MzVersion`'s string already carries the `v` prefix.
    try:
        previous_version = get_latest_published_version()
        previous_image = f"materialize/materialized:{previous_version}"
        previous = collect_synced_parameters(
            c, image=previous_image, label=f"release {previous_version}"
        )
    except Exception as e:
        print(f"WARNING: could not determine the last published release: {e}")
        previous = None

    known = set(current)
    if previous is not None:
        known |= set(previous)

    # 3. Flags in LaunchDarkly: keys+tags from the list endpoint, and served
    #    defaults (per environment) fetched individually for the flags present
    #    in both -- the list endpoint only returns summaries.
    try:
        with launchdarkly_api.ApiClient(
            launchdarkly_api.Configuration(api_key=dict(ApiKey=LAUNCHDARKLY_API_TOKEN))
        ) as api_client:
            api = feature_flags_api.FeatureFlagsApi(api_client)
            ld_tags = launchdarkly_flag_tags(api)
            ld_keys = set(ld_tags)
            in_both = sorted(
                name
                for name in current
                if name in ld_tags and CI_TEST_TAG not in ld_tags[name]
            )
            served_defaults, env_keys_seen = launchdarkly_served_defaults(api, in_both)
    except launchdarkly_api.ApiException as e:
        raise UIError(
            f"Error calling the LaunchDarkly API (status={e.status}, reason={e.reason})"
        )
    print(
        f"Found {len(ld_keys)} flags in LaunchDarkly project "
        f"'{LAUNCHDARKLY_PROJECT}'; {len(in_both)} present in both Materialize "
        f"and LaunchDarkly"
    )

    # Surface a misconfigured environment name: if none of the environments we
    # check exist on the fetched flags, the default-divergence result is
    # meaningless.
    missing_envs = [e for e in LAUNCHDARKLY_ENVIRONMENTS if e not in env_keys_seen]
    if env_keys_seen and missing_envs:
        print(
            f"WARNING: environment(s) {', '.join(missing_envs)} not found in "
            f"LaunchDarkly; available environments: "
            f"{', '.join(sorted(env_keys_seen))}. Set LAUNCHDARKLY_ENVIRONMENTS "
            f"to the correct keys for the default-divergence check."
        )

    # ERROR: parameters in Materialize that are missing in LaunchDarkly.
    missing_in_ld = set(current) - ld_keys

    # WARN: flags in LaunchDarkly that are not known to either the current build
    # or the last release (ignoring throwaway CI test flags).
    stale_in_ld = {key for key in ld_keys - known if CI_TEST_TAG not in ld_tags[key]}

    # WARN: cloud vs self-managed. Self-managed deployments have no LaunchDarkly
    # and run on the compiled-in default, so a production LaunchDarkly value that
    # differs from the compiled-in default means cloud and self-managed behave
    # differently. That is worth surfacing (e.g. a feature enabled in cloud but
    # still off by default in self-managed), except for parameters that
    # deliberately tune cloud-only infrastructure (INTENTIONAL_LD_OVERRIDES).
    cloud_vs_self_managed: dict[str, tuple[str, Any]] = {}
    for name in in_both:
        if name in INTENTIONAL_LD_OVERRIDES:
            continue
        prod_value = served_defaults.get(name, {}).get(PRODUCTION_ENVIRONMENT)
        if values_equivalent(current[name], prod_value) is False:
            cloud_vs_self_managed[name] = (current[name], prod_value)

    # WARN: flags whose served default differs *between environments* (e.g.
    # production vs staging). Environments should serve the same default unless a
    # staged rollout is in progress. No allowlist needed -- a value overridden
    # the same way in every environment (e.g. replica expiration) agrees across
    # environments and so does not warn.
    env_divergences: dict[str, dict[str, Any]] = {}
    for name in in_both:
        per_env = served_defaults.get(name, {})
        differs = any(
            values_equivalent(per_env.get(a), per_env.get(b)) is False
            for a, b in itertools.combinations(LAUNCHDARKLY_ENVIRONMENTS, 2)
        )
        if differs:
            env_divergences[name] = {
                e: per_env.get(e) for e in LAUNCHDARKLY_ENVIRONMENTS
            }

    print(
        f"Divergence: {len(cloud_vs_self_managed)} flag(s) differ between cloud "
        f"('{PRODUCTION_ENVIRONMENT}') and the self-managed compiled-in default; "
        f"{len(env_divergences)} differ between environments."
    )

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

    if cloud_vs_self_managed:
        print(
            f"--- WARNING: flags whose cloud default ('{PRODUCTION_ENVIRONMENT}') "
            f"differs from the self-managed compiled-in default"
        )
        for name in sorted(cloud_vs_self_managed):
            mz_value, prod_value = cloud_vs_self_managed[name]
            print(f"  {name}: self-managed={mz_value!r} cloud={prod_value!r}")
        print(
            "Self-managed deployments run on the compiled-in default. If the "
            "difference is intentional cloud-only tuning, add the flag to "
            "INTENTIONAL_LD_OVERRIDES; otherwise reconcile the compiled-in "
            "default (e.g. a feature enabled in cloud but still off by default)."
        )

    if env_divergences:
        print(
            f"--- WARNING: flags whose LaunchDarkly default differs between "
            f"environments ({', '.join(LAUNCHDARKLY_ENVIRONMENTS)})"
        )
        for name in sorted(env_divergences):
            rendered = ", ".join(
                f"{env}={value!r}" for env, value in env_divergences[name].items()
            )
            print(f"  {name}: {rendered}")
        print(
            "Environments should usually serve the same default; confirm any "
            "staged rollout is intentional."
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

    if (
        not missing_in_ld
        and not stale_in_ld
        and not cloud_vs_self_managed
        and not env_divergences
    ):
        print(
            "All synchronized parameters are defined in LaunchDarkly, with "
            "matching defaults across environments and with the self-managed "
            "compiled-in default, and no stale flags were found."
        )
