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
from typing import List, Set, Type

from materialize.mzcompose import Composition
from materialize.zippy.framework import Action, Capabilities, Capability
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.sink_capabilities import SinkExists
from materialize.zippy.view_capabilities import ViewExists


class CreateSink(Action):
    """Creates a sink over an existing view. Then creates a source over that sink and a view over that source."""

    @classmethod
    def requires(self) -> List[Set[Type[Capability]]]:
        return [{MzIsRunning, ViewExists}]

    def __init__(self, capabilities: Capabilities) -> None:
        sink_name = "sink" + str(random.randint(1, 10))

        this_sink = SinkExists(name=sink_name)
        self.sink = this_sink
        existing_sinks = [
            s for s in capabilities.get(SinkExists) if s.name == this_sink.name
        ]

        if len(existing_sinks) == 0:
            self.new_sink = True
            self.source_view = random.choice(capabilities.get(ViewExists))
            self.dest_view = ViewExists(
                name=f"{sink_name}_view", froms=[self.source_view]
            )
        elif len(existing_sinks) == 1:
            self.new_sink = False
            self.sink = existing_sinks[0]
        else:
            assert False

    def run(self, c: Composition) -> None:
        if not self.new_sink:
            return

        c.testdrive(
            dedent(
                f"""
                > CREATE CONNECTION IF NOT EXISTS {self.sink.name}_kafka_conn FOR KAFKA BROKER '${{testdrive.kafka-addr}}';

                > CREATE SINK {self.sink.name} FROM {self.source_view.name}
                  INTO KAFKA CONNECTION {self.sink.name}_kafka_conn
                  TOPIC 'sink-{self.sink.name}' WITH (reuse_topic=true)
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${{testdrive.schema-registry-url}}'

                # Ingest the sink again in order to be able to validate its contents

                > CREATE SOURCE {self.sink.name}_source
                  FROM KAFKA CONNECTION {self.sink.name}_kafka_conn
                  TOPIC 'sink-{self.sink.name}'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${{testdrive.schema-registry-url}}'
                  ENVELOPE NONE

                # The sink-dervied source has upsert semantics, so produce a "normal" ViewExists output
                # from the 'before' and the 'after'

                > CREATE MATERIALIZED VIEW {self.dest_view.name} AS
                  SELECT SUM(min)::int AS min, SUM(max)::int AS max, SUM(c1)::int AS c1, SUM(c2)::int AS c2 FROM (
                    SELECT (after).min, (after).max, (after).c1, (after).c2 FROM {self.sink.name}_source
                    UNION ALL
                    SELECT - (before).min, - (before).max, -(before).c1, -(before).c2 FROM {self.sink.name}_source
                  );
            """
            )
        )

    def provides(self) -> List[Capability]:
        return [self.sink, self.dest_view] if self.new_sink else []
