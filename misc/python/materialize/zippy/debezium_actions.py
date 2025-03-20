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
from materialize.zippy.debezium_capabilities import (
    DebeziumRunning,
    DebeziumSourceExists,
    PostgresTableExists,
)
from materialize.zippy.framework import Action, Capabilities, Capability, State
from materialize.zippy.kafka_capabilities import KafkaRunning
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.replica_capabilities import source_capable_clusters
from materialize.zippy.storaged_capabilities import StoragedRunning


class DebeziumStart(Action):
    """Start a Debezium instance."""

    def provides(self) -> list[Capability]:
        return [DebeziumRunning()]

    def run(self, c: Composition, state: State) -> None:
        c.up("debezium")


class DebeziumStop(Action):
    """Stop the Debezium instance."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {DebeziumRunning}

    def withholds(self) -> set[type[Capability]]:
        return {DebeziumRunning}

    def run(self, c: Composition, state: State) -> None:
        c.kill("debezium")


class CreateDebeziumSource(Action):
    """Creates a Debezium source in Materialized."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {
            BalancerdIsRunning,
            MzIsRunning,
            StoragedRunning,
            KafkaRunning,
            PostgresTableExists,
        }

    def __init__(self, capabilities: Capabilities) -> None:
        # To avoid conflicts, we make sure the postgres table and the debezium source have matching names
        postgres_table = random.choice(capabilities.get(PostgresTableExists))
        cluster_name = random.choice(source_capable_clusters(capabilities))
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
            self.cluster_name = cluster_name
        elif len(existing_debezium_sources) == 1:
            self.new_debezium_source = False

            self.debezium_source = existing_debezium_sources[0]
            assert self.debezium_source.postgres_table is not None
            self.postgres_table = self.debezium_source.postgres_table
        else:
            raise RuntimeError("More than one Debezium source exists")

        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
        if self.new_debezium_source:
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
                        "decimal.handling.mode": "precise",
                        "topic.prefix": "postgres"
                      }}
                    }}

                    $ schema-registry-wait topic=postgres.public.{self.postgres_table.name}

                    > CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT);

                    > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${{testdrive.schema-registry-url}}');

                    > CREATE SOURCE {self.debezium_source.name}
                      IN CLUSTER {self.cluster_name}
                      FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.{self.postgres_table.name}')

                    > CREATE TABLE {self.debezium_source.get_name_for_query()} FROM SOURCE {self.debezium_source.name} (REFERENCE "postgres.public.{self.postgres_table.name}")
                      FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                      ENVELOPE DEBEZIUM
                    """
                )
            )

    def provides(self) -> list[Capability]:
        return [self.debezium_source] if self.new_debezium_source else []
