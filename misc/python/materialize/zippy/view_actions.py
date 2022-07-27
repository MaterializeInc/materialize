# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from typing import List, Set, Type, Union

from materialize.mzcompose import Composition
from materialize.zippy.framework import Action, Capabilities, Capability
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.source_capabilities import SourceExists
from materialize.zippy.table_capabilities import TableExists
from materialize.zippy.view_capabilities import ViewExists

WatermarkedObjects = List[Union[TableExists, SourceExists]]


class CreateView(Action):
    """Creates a view that is a join over one or more sources or tables"""

    @classmethod
    def requires(self) -> List[Set[Type[Capability]]]:
        return [{MzIsRunning, SourceExists}, {MzIsRunning, TableExists}]

    def __init__(self, capabilities: Capabilities) -> None:
        view_name = "view" + str(random.randint(1, 10))

        this_view = ViewExists(name=view_name)
        existing_views = [
            v for v in capabilities.get(ViewExists) if v.name == this_view.name
        ]
        self.view = this_view

        if len(existing_views) == 0:
            self.new_view = True
            sources: WatermarkedObjects = capabilities.get(SourceExists)
            tables: WatermarkedObjects = capabilities.get(TableExists)

            potential_froms = sources + tables
            this_view.froms = random.sample(
                potential_froms, min(len(potential_froms), random.randint(1, 5))
            )

            self.view = this_view
            assert len(self.view.froms) > 0
        elif len(existing_views) == 1:
            self.new_view = False
            self.view = existing_views[0]
        else:
            assert False

    def run(self, c: Composition) -> None:
        if not self.new_view:
            return

        some_from = random.sample(self.view.froms, 1)[0]
        outer_join = "\n  ".join(
            f"JOIN {f.name} USING (f1)" for f in self.view.froms[1:]
        )

        c.testdrive(
            f"""
> CREATE MATERIALIZED VIEW {self.view.name} AS
  SELECT
    MIN({some_from.name}.f1),
    MAX({some_from.name}.f1),
    COUNT({some_from.name}.f1) AS c1,
    COUNT(DISTINCT {some_from.name}.f1) AS c2
  FROM {self.view.froms[0].name}
  {outer_join};
"""
        )

    def provides(self) -> List[Capability]:
        return [self.view] if self.new_view else []


class ValidateView(Action):
    """Validates a view."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, ViewExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.view = random.choice(capabilities.get(ViewExists))

    def run(self, c: Composition) -> None:
        watermarks = self.view.get_watermarks()
        view_min = watermarks.min
        view_max = watermarks.max

        if view_min <= view_max:
            c.testdrive(
                f"""
> SELECT * FROM {self.view.name} /* {view_min} {view_max} {(view_max-view_min)+1} {(view_max-view_min)+1} */ ;
{view_min} {view_max} {(view_max-view_min)+1} {(view_max-view_min)+1}
"""
            )
