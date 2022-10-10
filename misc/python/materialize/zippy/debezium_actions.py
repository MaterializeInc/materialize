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
from materialize.zippy.debezium_capabilities import (
    DebeziumRunning,
    DebeziumSourceExists,
    PostgresTableExists,
)
from materialize.zippy.framework import Action, Capabilities, Capability
from materialize.zippy.kafka_capabilities import KafkaRunning
from materialize.zippy.mz_capabilities import MzIsRunning


class DebeziumStart(Action):
    """Start a Debezium instance."""

    def provides(self) -> List[Capability]:
        return [DebeziumRunning()]

    def run(self, c: Composition) -> None:
        c.start_and_wait_for_tcp(services=["debezium"])


class DebeziumStop(Action):
    """Stop the Debezium instance."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {DebeziumRunning}

    def withholds(self) -> Set[Type[Capability]]:
        return {DebeziumRunning}

    def run(self, c: Composition) -> None:
        c.kill("debezium")


class CreateDebeziumSource(Action):
    """Creates a Debezium source in Materialized."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, KafkaRunning, PostgresTableExists}

    def __init__(self, capabilities: Capabilities) -> None:
        # To avoid conflicts, we make sure the postgres table and the debezium source have matching names
        postgres_table = random.choice(capabilities.get(PostgresTableExists))
        debezium_source_name = f"debezium_source_{postgres_table.name}"
        this_debezium_source = DebeziumSourceExists(name=debezium_source_name)

        existing_debezium_sources = [
            s
            for s in capabilities.get(DebeziumSourceExists)
            if s.name == this_debezium_source.name
        ]

        if len(existing_debezium_sources) == 0:
            self.new_debezium_source = True

            self.debezium_source = this_debezium_source
            self.postgres_table = postgres_table
            self.debezium_source.postgres_table = self.postgres_table
        elif len(existing_debezium_sources) == 1:
            self.new_debezium_source = False

            self.debezium_source = existing_debezium_sources[0]
            assert self.debezium_source.postgres_table is not None
            self.postgres_table = self.debezium_source.postgres_table
        else:
            assert False

    def run(self, c: Composition) -> None:
        if self.new_debezium_source:
            upsert = "UPSERT" if self.postgres_table.has_pk else ""
            c.testdrive(
                dedent(
                    f"""
                    $ http-request method=POST url=http://debezium:8083/connectors content-type=application/json
                    {{
                      "name": "{self.debezium_source.name}",
                      "config": {{
                        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                        "database.hostname": "postgres",
                        "database.port": "5432",
                        "database.user": "postgres",
                        "database.password": "postgres",
                        "database.dbname" : "postgres",
                        "database.server.name": "postgres",
                        "schema.include.list": "public",
                        "table.include.list": "public.{self.postgres_table.name}",
                        "plugin.name": "pgoutput",
                        "publication.name": "dbz_publication_{self.debezium_source.name}",
                        "publication.autocreate.mode": "filtered",
                        "slot.name" : "slot_{self.postgres_table.name}",
                        "database.history.kafka.bootstrap.servers": "kafka:9092",
                        "database.history.kafka.topic": "schema-changes.history",
                        "truncate.handling.mode": "include",
                        "decimal.handling.mode": "precise"
                      }}
                    }}

                    $ schema-registry-wait subject=postgres.public.{self.postgres_table.name}-value

                    > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${{testdrive.kafka-addr}}';

                    > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${{testdrive.schema-registry-url}}';

                    # UPSERT is requred due to https://github.com/MaterializeInc/materialize/issues/14211
                    > CREATE SOURCE {self.debezium_source.name}
                      FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.{self.postgres_table.name}')
                      FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                      ENVELOPE DEBEZIUM
                      {upsert}
                    """
                )
            )

    def provides(self) -> List[Capability]:
        return [self.debezium_source] if self.new_debezium_source else []
