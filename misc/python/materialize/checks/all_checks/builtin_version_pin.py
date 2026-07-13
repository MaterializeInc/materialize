# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion

# Builtin tables in stable, user-referenceable schemas (all in mz_catalog),
# each mapped to the version that converted it from a table to a materialized
# view, or None while it is still a table.
#
# An old binary with enable_alter_table_add_column on pins a view over a
# builtin table at VERSION 0. Once a later binary has converted that builtin
# to a view, VERSION 0 no longer validates and catalog open panics, unless the
# binary carries the tolerance fix. The check confirms the pin survives an
# upgrade instead of wedging the catalog.
#
# A builtin is skipped for a given upgrade only inside the window where the
# writer is a table-era binary but the first reader of the converted view is a
# released binary that predates the fix. See _active_builtins. Outside that
# window the entry runs again on its own, so record a conversion here rather
# than deleting the builtin.
#
# When you convert a builtin to a view, set its version here. mz_internal
# builtin tables are excluded: users cannot create views with unstable
# dependencies, so conversions there cannot poison customer catalogs.
# mz_role_auth is excluded because only mz_system can read it.
_STABLE_BUILTINS: dict[str, MzVersion | None] = {
    "mz_array_types": None,
    "mz_audit_events": MzVersion.parse_mz("v26.33.0"),
    "mz_aws_privatelink_connections": None,
    "mz_base_types": None,
    "mz_cluster_replica_sizes": None,
    "mz_cluster_replicas": MzVersion.parse_mz("v26.30.0"),
    "mz_clusters": MzVersion.parse_mz("v26.30.0"),
    "mz_columns": None,
    "mz_default_privileges": MzVersion.parse_mz("v26.30.0"),
    "mz_egress_ips": None,
    "mz_functions": None,
    "mz_iceberg_sinks": None,
    "mz_index_columns": None,
    "mz_kafka_connections": None,
    "mz_kafka_sinks": None,
    "mz_kafka_sources": None,
    "mz_list_types": None,
    "mz_map_types": None,
    "mz_operators": None,
    "mz_pseudo_types": None,
    "mz_sinks": None,
    "mz_ssh_tunnel_connections": None,
    "mz_system_privileges": MzVersion.parse_mz("v26.30.0"),
    "mz_tables": None,
    "mz_types": None,
    "mz_views": None,
}


def _pin_views(suffix_tables: list[str], prefix: str) -> str:
    create = "\n".join(
        f"> CREATE VIEW {prefix}{t} AS SELECT count(*) AS c FROM mz_catalog.{t};"
        for t in suffix_tables
    )
    rename = "\n".join(
        f"> ALTER VIEW {prefix}{t} RENAME TO {prefix}{t}_r;" for t in suffix_tables
    )
    return f"{create}\n{rename}"


class BuiltinItemVersionPin(Check):
    """Views over builtin tables must keep working when a later version
    converts the builtin to another item type (e.g. mz_clusters became a
    materialized view in v26.30).

    With enable_alter_table_add_column on, re-parsing a persisted view pins
    referenced builtin tables at VERSION 0 in the in-memory create_sql
    (resolve_item_name_id pins any versioned item, not just user tables).
    A rename then persists the pinned reference. A later version where the
    builtin is no longer a table rejects VERSION 0 with InvalidVersion and
    environmentd panics during catalog open, wedging the upgrade.
    """

    def _can_run(self, e: Executor) -> bool:
        # mz_iceberg_sinks is the newest builtin referenced below.
        return self.base_version >= MzVersion.parse_mz("v26.25.0")

    def _active_builtins(self) -> list[str]:
        """Builtins safe to pin for this upgrade.

        The writer of the VERSION 0 pin is base_version, which pins only while
        the builtin is still a table (base_version < converted). A reader
        crashes on the pin only if it has converted the builtin to a view and
        lacks the tolerance fix, which is true of every released binary up to
        the last released version. So a builtin is unsafe only when its
        conversion shipped in a release the upgrade passes through, that is
        base_version < converted <= last released version. The current binary
        under test carries the fix, so a conversion newer than the last release
        is safe and is exactly the case worth exercising.
        """
        # Imported lazily: the module resolves published versions over the
        # network, and the check registry imports this file at startup.
        from materialize.checks.scenarios_upgrade import get_last_version

        last_released = get_last_version()
        return [
            name
            for name, converted in _STABLE_BUILTINS.items()
            if converted is None or not (self.base_version < converted <= last_released)
        ]

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_alter_table_add_column = true;

                """) + _pin_views(self._active_builtins(), "builtin_pin_"))

    def manipulate(self) -> list[Testdrive]:
        # Re-run the poisoning on the intermediate versions of multi-version
        # upgrade scenarios. In 0dt scenarios this also exercises applying a
        # poisoned catalog update while the next generation follows the
        # catalog in read-only mode.
        return [
            Testdrive(_pin_views(self._active_builtins(), f"builtin_pin_m{i}_"))
            for i in [1, 2]
        ]

    def validate(self) -> Testdrive:
        selects = "\n".join(
            f"> SELECT c >= 0 FROM {prefix}{t}_r;\ntrue"
            for prefix in ["builtin_pin_", "builtin_pin_m1_", "builtin_pin_m2_"]
            for t in self._active_builtins()
        )
        return Testdrive(selects)
