# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Reads whose placement the interactive compute runtime changes.

The benchmark's clusterd container runs two runtimes when its image supports the option, so
these run against a two-runtime replica on this build and against whatever the other build's
image provides, which is what the comparison should measure.
"""

from materialize.feature_benchmark.action import Action, TdAction
from materialize.feature_benchmark.measurement_source import MeasurementSource, Td
from materialize.feature_benchmark.scenario import Scenario


class InteractiveRuntime(Scenario):
    """Group parent."""


class PeekDataflowJoin(InteractiveRuntime):
    """A join over two indexed views, repeated. Neither input can take the fast path, so each
    query builds a peek dataflow that imports both indexes, which is the cost of a temporary
    dataflow on the runtime that serves it."""

    SCALE = 5
    REPEAT = 10

    def init(self) -> list[Action]:
        return [
            self.table_ten(),
            TdAction(f"""
> CREATE MATERIALIZED VIEW v1 AS SELECT {self.unique_values()} AS f1 FROM {self.join()}

> CREATE MATERIALIZED VIEW v2 AS SELECT {self.unique_values()} AS f1 FROM {self.join()}

> CREATE DEFAULT INDEX ON v1

> CREATE DEFAULT INDEX ON v2

> SELECT count(*) FROM v1 JOIN v2 USING (f1)
{self.n()}
"""),
        ]

    def benchmark(self) -> MeasurementSource:
        joins = "\n".join(
            f"> SELECT count(*) FROM v1 JOIN v2 USING (f1)\n{self.n()}\n"
            for _ in range(self.REPEAT)
        )
        return Td(f"""
> SELECT 1
  /* A */
1

{joins}

> SELECT 1
  /* B */
1
""")


class PointLookup(InteractiveRuntime):
    """A literal lookup on an indexed view, repeated. On the interactive runtime the walk reads
    the arrangement the maintenance runtime published rather than a local trace."""

    REPEAT = 1000

    def init(self) -> list[Action]:
        return [
            self.table_ten(),
            TdAction(f"""
> CREATE MATERIALIZED VIEW v1 AS SELECT {self.unique_values()} AS f1 FROM {self.join()}

> CREATE DEFAULT INDEX ON v1

> SELECT count(*) = {self.n()} FROM v1
true
"""),
        ]

    def benchmark(self) -> MeasurementSource:
        lookups = "\n".join(
            "> SELECT * FROM v1 WHERE f1 = 1\n1\n" for _ in range(self.REPEAT)
        )
        return Td(f"""
> SET auto_route_introspection_queries TO false

> BEGIN

> SELECT 1
  /* A */
1

{lookups}

> SELECT 1
  /* B */
1
""")


class CreateIndexPublish(InteractiveRuntime):
    """CREATE INDEX plus the first read that uses it. A publishing runtime installs a publisher
    per arrangement, and the first read on the interactive runtime waits for its publication.
    """

    def init(self) -> list[Action]:
        return [
            self.table_ten(),
            TdAction(f"""
> CREATE TABLE t1 (f1 INTEGER, f2 INTEGER)

> INSERT INTO t1 (f1) SELECT {self.unique_values()} FROM {self.join()}

> SELECT 1 FROM t1 WHERE f1 = 0
1
"""),
        ]

    def benchmark(self) -> MeasurementSource:
        return Td("""
> DROP INDEX IF EXISTS i1
  /* A */

> CREATE INDEX i1 ON t1(f1)

> SELECT count(*) FROM t1 AS a1, t1 AS a2 WHERE a1.f1 = a2.f1 AND a1.f1 = 0 AND a2.f1 = 0
  /* B */
1
""")


class IntrospectionRead(InteractiveRuntime):
    """A read of a per-replica introspection relation, repeated. The interactive runtime serves it
    from the maintenance runtime's published logging index."""

    REPEAT = 100

    def init(self) -> list[Action]:
        return [
            self.table_ten(),
            TdAction(f"""
> CREATE MATERIALIZED VIEW v1 AS SELECT {self.unique_values()} AS f1 FROM {self.join()}

> CREATE DEFAULT INDEX ON v1

> SELECT count(*) = {self.n()} FROM v1
true
"""),
        ]

    def benchmark(self) -> MeasurementSource:
        reads = "\n".join(
            "> SELECT count(*) > 0 FROM mz_introspection.mz_dataflow_arrangement_sizes\ntrue\n"
            for _ in range(self.REPEAT)
        )
        return Td(f"""
> SELECT 1
  /* A */
1

{reads}

> SELECT 1
  /* B */
1
""")
