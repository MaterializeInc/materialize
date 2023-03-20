# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Dict, List

from materialize.zippy.crdb_actions import CockroachRestart, CockroachStart
from materialize.zippy.debezium_actions import CreateDebeziumSource, DebeziumStart
from materialize.zippy.framework import ActionOrFactory, Scenario
from materialize.zippy.kafka_actions import (
    CreateTopicParameterized,
    Ingest,
    KafkaInsertParallel,
    KafkaStart,
)
from materialize.zippy.kafka_capabilities import Envelope
from materialize.zippy.minio_actions import MinioRestart, MinioStart
from materialize.zippy.mz_actions import KillClusterd, MzRestart, MzStart, MzStop
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
)
from materialize.zippy.sink_actions import CreateSinkParameterized
from materialize.zippy.source_actions import CreateSourceParameterized
from materialize.zippy.storaged_actions import (
    StoragedKill,
    StoragedRestart,
    StoragedStart,
)
from materialize.zippy.table_actions import DML, CreateTableParameterized, ValidateTable
from materialize.zippy.view_actions import CreateViewParameterized, ValidateView

DEFAULT_BOOTSTRAP: List[ActionOrFactory] = [
    KafkaStart,
    CockroachStart,
    MinioStart,
    MzStart,
    StoragedStart,
]


class KafkaSources(Scenario):
    """A Zippy test using Kafka sources exclusively."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return DEFAULT_BOOTSTRAP

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            MzStart: 5,
            MzStop: 1,
            KillClusterd: 5,
            StoragedKill: 5,
            StoragedStart: 5,
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
        return DEFAULT_BOOTSTRAP

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            MzStart: 1,
            MzStop: 15,
            KillClusterd: 10,
            StoragedRestart: 5,
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
        return [
            *DEFAULT_BOOTSTRAP,
            DebeziumStart,
            PostgresStart,
        ]

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            CreatePostgresTable: 10,
            CreateDebeziumSource: 10,
            KillClusterd: 5,
            StoragedKill: 5,
            StoragedStart: 5,
            CreateViewParameterized(): 10,
            ValidateView: 20,
            PostgresDML: 100,
        }


class PostgresCdc(Scenario):
    """A Zippy test using Postgres CDC exclusively."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return [*DEFAULT_BOOTSTRAP, PostgresStart]

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            CreatePostgresTable: 10,
            CreatePostgresCdcTable: 10,
            KillClusterd: 5,
            StoragedKill: 5,
            StoragedStart: 5,
            PostgresRestart: 10,
            CreateViewParameterized(): 10,
            ValidateView: 20,
            PostgresDML: 100,
        }


class ClusterReplicas(Scenario):
    """A Zippy test that uses CREATE / DROP REPLICA and random killing."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return [
            *DEFAULT_BOOTSTRAP,
            DropDefaultReplica,
            CreateReplica,
        ]

    # Due to gh#13235 it is not possible to have MzStop/MzStart in this scenario
    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            KillClusterd: 5,
            StoragedRestart: 5,
            CreateReplica: 30,
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
        return DEFAULT_BOOTSTRAP

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            KillClusterd: 5,
            StoragedKill: 5,
            StoragedStart: 5,
            CreateTopicParameterized(): 10,
            CreateSourceParameterized(): 10,
            CreateViewParameterized(expensive_aggregates=False, max_inputs=1): 5,
            ValidateView: 10,
            KafkaInsertParallel: 50,
        }


class CrdbMinioRestart(Scenario):
    """A Zippy test that restarts CRDB and Minio."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return DEFAULT_BOOTSTRAP

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            CreateTopicParameterized(): 5,
            CreateSourceParameterized(): 5,
            CreateViewParameterized(max_inputs=2): 5,
            CreateSinkParameterized(): 5,
            Ingest: 50,
            CreateTableParameterized(): 10,
            DML: 50,
            ValidateView: 15,
            MzRestart: 5,
            KillClusterd: 5,
            StoragedRestart: 10,
            CockroachRestart: 15,
            MinioRestart: 15,
        }


class CrdbRestart(Scenario):
    """A Zippy test that restarts Cockroach."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return DEFAULT_BOOTSTRAP

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            CreateTopicParameterized(): 5,
            CreateSourceParameterized(): 5,
            CreateViewParameterized(max_inputs=2): 5,
            CreateSinkParameterized(): 5,
            Ingest: 50,
            CreateTableParameterized(): 10,
            DML: 50,
            ValidateView: 15,
            MzRestart: 5,
            KillClusterd: 5,
            StoragedRestart: 10,
            CockroachRestart: 15,
        }


class KafkaSourcesLarge(Scenario):
    """A Zippy test using a large number of Kafka sources, views and sinks (no killings)."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return DEFAULT_BOOTSTRAP

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            CreateTopicParameterized(max_topics=5): 10,
            CreateSourceParameterized(max_sources=25): 10,
            CreateViewParameterized(
                max_views=50, expensive_aggregates=False, max_inputs=1
            ): 5,
            CreateSinkParameterized(max_sinks=25): 10,
            ValidateView: 10,
            Ingest: 100,
            PeekCancellation: 5,
        }


class DataflowsLarge(Scenario):
    """A Zippy test using a smaller number but more complex dataflows."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return DEFAULT_BOOTSTRAP

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            CreateReplica: 2,
            CreateTopicParameterized(max_topics=2, envelopes=[Envelope.UPSERT]): 10,
            CreateSourceParameterized(max_sources=10): 10,
            CreateViewParameterized(
                max_views=5, expensive_aggregates=True, max_inputs=5
            ): 10,
            ValidateView: 10,
            Ingest: 50,
            DML: 50,
        }


class UserTablesLarge(Scenario):
    """A Zippy scenario over tables (no killing)."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return DEFAULT_BOOTSTRAP

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            CreateTableParameterized(max_tables=2): 10,
            CreateViewParameterized(
                max_views=5, expensive_aggregates=True, max_inputs=5
            ): 10,
            CreateSinkParameterized(max_sinks=10): 10,
            ValidateView: 10,
            Ingest: 50,
            DML: 50,
        }


class PostgresCdcLarge(Scenario):
    """A Zippy test using Postgres CDC exclusively (Pg not killed)."""

    def bootstrap(self) -> List[ActionOrFactory]:
        return [*DEFAULT_BOOTSTRAP, PostgresStart]

    def config(self) -> Dict[ActionOrFactory, float]:
        return {
            CreatePostgresTable: 10,
            CreatePostgresCdcTable: 10,
            KillClusterd: 5,
            StoragedKill: 5,
            StoragedStart: 5,
            CreateViewParameterized(): 10,
            ValidateView: 20,
            PostgresDML: 100,
        }
