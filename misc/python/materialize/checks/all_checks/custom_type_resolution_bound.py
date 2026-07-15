# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.mz_version import MzVersion

# The version that introduced the custom-type resolution bound and the flag that
# relaxes it while re-planning persisted catalog items.
BOUND_INTRODUCED_IN = MzVersion.parse_mz("v26.34.0-dev")

RELAX_BOUND = """
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unbounded_custom_type_resolution = on
""".strip()

RESTORE_BOUND = """
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET unsafe_enable_unbounded_custom_type_resolution
""".strip()


class CustomTypeResolutionBound(Check):
    """A wide custom record type that predates the custom-type resolution bound
    must keep loading during bootstrap after upgrade, rather than crash-looping
    environmentd when its persisted `create_sql` is re-planned by the stricter
    planner. See SQL-514."""

    def _create_wide_type(self) -> str:
        # `d14` resolves to ~49k sub-type nodes, within the per-root node budget.
        # `wide` holds three `d14` fields, so its resolved tree exceeds the
        # budget in aggregate and a version that enforces the bound rejects
        # creating it. It is therefore only plantable by a version that predates
        # the bound, or with the bound relaxed.
        lines = ["> CREATE TYPE d0 AS LIST (ELEMENT TYPE = int4)"]
        for i in range(1, 15):
            lines.append(f"> CREATE TYPE d{i} AS (a d{i - 1}, b d{i - 1})")
        lines.append("> CREATE TYPE wide AS (a d14, b d14, c d14)")
        return "\n".join(lines)

    def initialize(self) -> Testdrive:
        create = self._create_wide_type()
        if self.base_version >= BOUND_INTRODUCED_IN:
            # This version enforces the bound, so relax it to plant `wide`,
            # mimicking a catalog written by a version that predates the bound.
            body = f"{RELAX_BOUND}\n\n{create}\n\n{RESTORE_BOUND}"
        else:
            # This version predates the bound and stores `wide` without complaint.
            body = create
        return Testdrive(body)

    def manipulate(self) -> list[Testdrive]:
        # The type just needs to survive the upgrade phases in the catalog. The
        # regression is that re-planning its `create_sql` during bootstrap fails
        # the new bound and aborts the process.
        check_present = "> SELECT name FROM mz_types WHERE name = 'wide'\nwide"
        return [Testdrive(check_present), Testdrive(check_present)]

    def validate(self) -> Testdrive:
        # Reaching validation means every bootstrap in the scenario re-planned
        # `wide` successfully. That the type survived every bootstrap is what the
        # unconditional `mz_types` check below proves. The `SELECT NULL::wide`
        # assertions only pin the graceful failure of the versions that have a
        # stable, known diagnostic for it:
        #
        # - v26.34.0 and later enforce the bound, so resolving the grandfathered
        #   type returns "custom type is too complex to resolve" rather than
        #   panicking.
        # - v26.29.0 up to v26.34.0 have no bound, so resolution succeeds and
        #   pgwire binary encoding rejects the nested list. v26.29.0 introduced
        #   the "no binary output function available for type list" wording.
        #
        # Versions before v26.29.0 are left unasserted. Their `SELECT NULL::wide`
        # either succeeds or reports a different diagnostic, and the self-managed
        # upgrade scenarios reach back that far (v0.147.0).
        return Testdrive(
            "> SELECT name FROM mz_types WHERE name = 'wide'\n"
            "wide\n"
            "\n"
            "![2602900<=version<2603400] SELECT NULL::wide\n"
            "contains: no binary output function available for type list\n"
            "\n"
            "![version>=2603400] SELECT NULL::wide\n"
            "contains: custom type is too complex to resolve\n"
        )
