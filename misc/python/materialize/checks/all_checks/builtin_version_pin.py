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

# All builtin tables in stable, user-referenceable schemas (currently all in
# mz_catalog), including former builtin tables that later versions converted
# to materialized views. Conversions keep the name resolvable, so creating a
# view over an already-converted builtin is harmless, while a builtin that is
# still a table gets poisoned by the rename below and turns the next
# conversion into an upgrade wedge.
#
# mz_internal builtin tables are excluded: users cannot create views with
# unstable dependencies, so conversions there cannot poison customer catalogs.
# mz_role_auth is excluded because it is only readable by mz_system.
_STABLE_BUILTIN_TABLES = [
    "mz_array_types",
    "mz_audit_events",
    "mz_aws_privatelink_connections",
    "mz_base_types",
    "mz_cluster_replica_sizes",
    "mz_cluster_replicas",
    "mz_clusters",
    "mz_columns",
    "mz_default_privileges",
    "mz_egress_ips",
    "mz_functions",
    "mz_iceberg_sinks",
    "mz_index_columns",
    "mz_kafka_connections",
    "mz_kafka_sinks",
    "mz_kafka_sources",
    "mz_list_types",
    "mz_map_types",
    "mz_operators",
    "mz_pseudo_types",
    "mz_sinks",
    "mz_ssh_tunnel_connections",
    "mz_system_privileges",
    "mz_tables",
    "mz_types",
    "mz_views",
]


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
        # mz_iceberg_sinks is the newest table in the list above.
        return self.base_version >= MzVersion.parse_mz("v26.25.0")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_alter_table_add_column = true;

                """) + _pin_views(_STABLE_BUILTIN_TABLES, "builtin_pin_"))

    def manipulate(self) -> list[Testdrive]:
        # Re-run the poisoning on the intermediate versions of multi-version
        # upgrade scenarios. In 0dt scenarios this also exercises applying a
        # poisoned catalog update while the next generation follows the
        # catalog in read-only mode.
        return [
            Testdrive(_pin_views(_STABLE_BUILTIN_TABLES, f"builtin_pin_m{i}_"))
            for i in [1, 2]
        ]

    def validate(self) -> Testdrive:
        selects = "\n".join(
            f"> SELECT c >= 0 FROM {prefix}{t}_r;\ntrue"
            for prefix in ["builtin_pin_", "builtin_pin_m1_", "builtin_pin_m2_"]
            for t in _STABLE_BUILTIN_TABLES
        )
        return Testdrive(selects)
