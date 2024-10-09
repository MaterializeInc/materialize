# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from textwrap import dedent

from materialize.mzcompose.composition import Composition
from materialize.zippy.balancerd_capabilities import BalancerdIsRunning
from materialize.zippy.debezium_capabilities import DebeziumSourceExists
from materialize.zippy.framework import (
    Action,
    ActionFactory,
    Capabilities,
    Capability,
    State,
)
from materialize.zippy.mysql_cdc_capabilities import MySqlCdcTableExists
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.pg_cdc_capabilities import PostgresCdcTableExists
from materialize.zippy.source_capabilities import SourceExists
from materialize.zippy.storaged_capabilities import StoragedRunning
from materialize.zippy.table_capabilities import TableExists
from materialize.zippy.view_capabilities import ViewExists
from materialize.zippy.watermarked_object_capabilities import WatermarkedObjectExists


class CreateViewParameterized(ActionFactory):
    """Emits CreateView Actions within the constraints specified in the constructor."""

    @classmethod
    def requires(cls) -> list[set[type[Capability]]]:
        return [
            {BalancerdIsRunning, MzIsRunning, SourceExists},
            {BalancerdIsRunning, MzIsRunning, TableExists},
            {BalancerdIsRunning, MzIsRunning, DebeziumSourceExists},
            {BalancerdIsRunning, MzIsRunning, PostgresCdcTableExists},
            {BalancerdIsRunning, MzIsRunning, MySqlCdcTableExists},
        ]

    def __init__(
        self,
        max_views: int = 10,
        max_inputs: int = 5,
        expensive_aggregates: bool = True,
    ) -> None:
        self.max_views = max_views
        self.max_inputs = max_inputs
        self.expensive_aggregates = expensive_aggregates

    def new(self, capabilities: Capabilities) -> list[Action]:
        new_view_name = capabilities.get_free_capability_name(
            ViewExists, self.max_views
        )
        if new_view_name:
            potential_inputs: list[WatermarkedObjectExists] = []
            for source_capability in [
                SourceExists,
                TableExists,
                DebeziumSourceExists,
                PostgresCdcTableExists,
                MySqlCdcTableExists,
            ]:
                potential_inputs.extend(capabilities.get(source_capability))

            inputs = random.sample(
                potential_inputs,
                min(len(potential_inputs), random.randint(1, self.max_inputs)),
            )

            return [
                CreateView(
                    capabilities=capabilities,
                    view=ViewExists(
                        name=new_view_name,
                        has_index=random.choice([True, False]),
                        expensive_aggregates=self.expensive_aggregates,
                        inputs=inputs,
                    ),
                )
            ]
        else:
            return []


class CreateView(Action):
    """Creates a view that is a join over one or more sources or tables"""

    def __init__(self, capabilities: Capabilities, view: ViewExists) -> None:
        self.view = view
        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
        first_input = self.view.inputs[0]
        outer_join = " ".join(
            f"JOIN {f.get_name_for_query()} USING (f1)" for f in self.view.inputs[1:]
        )

        index = (
            f"> CREATE DEFAULT INDEX ON {self.view.name}" if self.view.has_index else ""
        )

        aggregates = [f"COUNT({first_input.get_name_for_query()}.f1) AS count_all"]

        if self.view.expensive_aggregates:
            aggregates.extend(
                [
                    f"COUNT(DISTINCT {first_input.get_name_for_query()}.f1) AS count_distinct",
                    f"MIN({first_input.get_name_for_query()}.f1) AS min_value",
                    f"MAX({first_input.get_name_for_query()}.f1) AS max_value",
                ]
            )

        aggregates = ", ".join(aggregates)

        refresh = random.choice(
            ["ON COMMIT", f"EVERY '{random.randint(1, 5)} seconds'"]
        )

        c.testdrive(
            dedent(
                f"""
                > CREATE MATERIALIZED VIEW {self.view.name}
                  WITH (REFRESH {refresh}) AS
                  SELECT {aggregates}
                  FROM {first_input.get_name_for_query()}
                  {outer_join}
                """
            )
            + index,
            mz_service=state.mz_service,
        )

    def provides(self) -> list[Capability]:
        return [self.view]


class ValidateView(Action):
    """Validates a view."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {BalancerdIsRunning, MzIsRunning, StoragedRunning, ViewExists}

    def __init__(
        self, capabilities: Capabilities, view: ViewExists | None = None
    ) -> None:
        if view is None:
            self.view = random.choice(capabilities.get(ViewExists))
        else:
            self.view = view

        # Trigger the PeekPersist optimization
        self.select_limit = random.choice(["", "LIMIT 1"])
        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
        watermarks = self.view.get_watermarks()
        view_min = watermarks.min
        view_max = watermarks.max

        if view_min <= view_max:
            c.testdrive(
                (
                    dedent(
                        f"""
                    > SELECT count_all, count_distinct, min_value, max_value FROM {self.view.name} {self.select_limit} /* expecting count_all = {(view_max-view_min)+1} count_distinct = {(view_max-view_min)+1} min_value = {view_min} max_value = {view_max} */ ;
                    {(view_max-view_min)+1} {(view_max-view_min)+1} {view_min} {view_max}
                """
                    )
                    if self.view.expensive_aggregates
                    else dedent(
                        f"""
                    > SELECT count_all FROM {self.view.name} {self.select_limit} /* expecting count_all = {(view_max-view_min)+1} */ ;
                    {(view_max-view_min)+1}
                """
                    )
                ),
                mz_service=state.mz_service,
            )
