# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Dict, List

from materialize.zippy.debezium_actions import CreateDebeziumSource, DebeziumStart
from materialize.zippy.framework import ActionOrFactory, Scenario
from materialize.zippy.kafka_actions import (
    CreateTopicParameterized,
    Ingest,
    KafkaInsertParallel,
    KafkaStart,
)
from materialize.zippy.kafka_capabilities import Envelope
from materialize.zippy.mz_actions import KillComputed, KillStoraged, MzStart, MzStop
from materialize.zippy.peek_actions import PeekCancellation
from materialize.zippy.pg_cdc_actions import CreatePostgresCdcTable
from materialize.zippy.postgres_actions import (
    CreatePostgresTable,
    PostgresDML,
    PostgresRestart,
    PostgresStart,
)
from materialize.zippy.replica_actions import (
    CreateReplica,
    DropDefaultReplica,
    DropReplica,
    KillReplica,
)
from materialize.zippy.sink_actions import CreateSinkParameterized
from materialize.zippy.source_actions import CreateSourceParameterized
from materialize.zippy.table_actions import DML, CreateTableParameterized, ValidateTable
from materialize.zippy.view_actions import CreateViewParameterized, ValidateView


class KafkaSources(Scenario):
    """A Zippy test using Kafka sources exclusively."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return [KafkaStart, MzStart]

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            MzStart: 1,
            MzStop: 10,
            KillStoraged: 15,
            KillComputed: 15,
            CreateTopicParameterized(): 5,
            CreateSourceParameterized(): 5,
            CreateViewParameterized(max_inputs=2): 5,
            CreateSinkParameterized(): 5,
            ValidateView: 10,
            Ingest: 100,
            PeekCancellation: 5,
        }


class UserTables(Scenario):
    """A Zippy test using user tables exclusively."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return [KafkaStart, MzStart]

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            MzStart: 1,
            MzStop: 15,
            KillComputed: 15,
            CreateTableParameterized(): 10,
            CreateViewParameterized(): 10,
            CreateSinkParameterized(): 10,
            ValidateTable: 20,
            ValidateView: 20,
            DML: 30,
        }


class DebeziumPostgres(Scenario):
    """A Zippy test using Debezium Postgres exclusively."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return [KafkaStart, DebeziumStart, PostgresStart, MzStart]

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            CreatePostgresTable: 10,
            CreateDebeziumSource: 10,
            KillStoraged: 15,
            KillComputed: 15,
            CreateViewParameterized(): 10,
            ValidateView: 20,
            PostgresDML: 100,
        }


class PostgresCdc(Scenario):
    """A Zippy test using Postgres CDC exclusively."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return [PostgresStart, MzStart]

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            CreatePostgresTable: 10,
            CreatePostgresCdcTable: 10,
            KillStoraged: 15,
            KillComputed: 15,
            PostgresRestart: 10,
            CreateViewParameterized(): 10,
            ValidateView: 20,
            PostgresDML: 100,
        }


class ClusterReplicas(Scenario):
    """A Zippy test that uses CREATE / DROP REPLICA and random killing."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return [KafkaStart, MzStart, DropDefaultReplica, CreateReplica]

    # Due to gh#13235 it is not possible to have MzStop/MzStart in this scenario
    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            KillStoraged: 10,
            KillComputed: 10,
            CreateReplica: 30,
            KillReplica: 10,
            DropReplica: 10,
            CreateTopicParameterized(): 10,
            CreateSourceParameterized(): 10,
            CreateTableParameterized(): 10,
            CreateViewParameterized(): 20,
            CreateSinkParameterized(): 10,
            ValidateView: 20,
            Ingest: 50,
            DML: 50,
            PeekCancellation: 5,
        }


class KafkaParallelInsert(Scenario):
    """A Zippy test using simple views over Kafka sources with parallel insertion."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return [KafkaStart, MzStart]

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            KillStoraged: 5,
            KillComputed: 5,
            CreateTopicParameterized(): 10,
            CreateSourceParameterized(): 10,
            CreateViewParameterized(expensive_aggregates=False, max_inputs=1): 5,
            ValidateView: 10,
            KafkaInsertParallel: 50,
        }


class KafkaSourcesLarge(Scenario):
    """A Zippy test using a large number of Kafka sources, views and sinks."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return [KafkaStart, MzStart]

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            # Killing computed causes a massive memory spike during re-hyration
            # MzStart: 1,
            # MzStop: 2,
            # KillComputed: 2,
            KillStoraged: 2,
            CreateTopicParameterized(max_topics=5): 10,
            CreateSourceParameterized(max_sources=50): 10,
            CreateViewParameterized(
                max_views=100, expensive_aggregates=False, max_inputs=1
            ): 5,
            CreateSinkParameterized(max_sinks=50): 10,
            ValidateView: 10,
            Ingest: 100,
            PeekCancellation: 5,
        }


class DataflowsLarge(Scenario):
    """A Zippy test using a smaller number but more complex dataflows."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return [KafkaStart, MzStart]

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            # Killing computed causes a massive memory spike during re-hyration
            # MzStart: 1,
            # MzStop: 2,
            # KillComputed: 2,
            KillStoraged: 2,
            CreateReplica: 2,
            CreateTableParameterized(max_tables=2): 10,
            CreateTopicParameterized(max_topics=2, envelopes=[Envelope.UPSERT]): 10,
            CreateSourceParameterized(max_sources=10): 10,
            CreateViewParameterized(
                max_views=5, expensive_aggregates=True, max_inputs=5
            ): 10,
            ValidateView: 10,
            Ingest: 50,
            DML: 50,
        }
