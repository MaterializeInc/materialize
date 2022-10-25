# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Dict, List, Type

from materialize.zippy.debezium_actions import CreateDebeziumSource, DebeziumStart
from materialize.zippy.framework import Action, Scenario
from materialize.zippy.kafka_actions import (
    CreateTopic,
    Ingest,
    KafkaInsertParallel,
    KafkaStart,
)
from materialize.zippy.mz_actions import KillComputed, KillStoraged, MzStart, MzStop
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
from materialize.zippy.sink_actions import CreateSink
from materialize.zippy.source_actions import CreateSource
from materialize.zippy.table_actions import DML, CreateTable, ValidateTable
from materialize.zippy.view_actions import CreateView, CreateViewSimple, ValidateView


class KafkaSources(Scenario):
    """A Zippy test using Kafka sources exclusively."""

    def bootstrap(self) -> List[Type[Action]]:
        return [KafkaStart, MzStart]

    def config(self) -> Dict[Type[Action], float]:
        return {
            MzStart: 1,
            MzStop: 10,
            KillStoraged: 15,
            KillComputed: 15,
            CreateTopic: 5,
            CreateSource: 5,
            CreateView: 5,
            CreateSink: 5,
            ValidateView: 10,
            Ingest: 100,
        }


class UserTables(Scenario):
    """A Zippy test using user tables exclusively."""

    def bootstrap(self) -> List[Type[Action]]:
        return [KafkaStart, MzStart]

    def config(self) -> Dict[Type[Action], float]:
        return {
            MzStart: 1,
            MzStop: 15,
            KillComputed: 15,
            CreateTable: 10,
            CreateView: 10,
            CreateSink: 10,
            ValidateTable: 20,
            ValidateView: 20,
            DML: 30,
        }


class DebeziumPostgres(Scenario):
    """A Zippy test using Debezium Postgres exclusively."""

    def bootstrap(self) -> List[Type[Action]]:
        return [KafkaStart, DebeziumStart, PostgresStart, MzStart]

    def config(self) -> Dict[Type[Action], float]:
        return {
            CreatePostgresTable: 10,
            CreateDebeziumSource: 10,
            KillStoraged: 15,
            KillComputed: 15,
            CreateView: 10,
            ValidateView: 20,
            PostgresDML: 100,
        }


class PostgresCdc(Scenario):
    """A Zippy test using Postgres CDC exclusively."""

    def bootstrap(self) -> List[Type[Action]]:
        return [PostgresStart, MzStart]

    def config(self) -> Dict[Type[Action], float]:
        return {
            CreatePostgresTable: 10,
            CreatePostgresCdcTable: 10,
            KillStoraged: 15,
            KillComputed: 15,
            PostgresRestart: 10,
            CreateView: 10,
            ValidateView: 20,
            PostgresDML: 100,
        }


class ClusterReplicas(Scenario):
    """A Zippy test that uses CREATE / DROP REPLICA and random killing."""

    def bootstrap(self) -> List[Type[Action]]:
        return [KafkaStart, MzStart, DropDefaultReplica, CreateReplica]

    # Due to gh#13235 it is not possible to have MzStop/MzStart in this scenario
    def config(self) -> Dict[Type[Action], float]:
        return {
            KillStoraged: 10,
            KillComputed: 10,
            CreateReplica: 30,
            KillReplica: 10,
            DropReplica: 10,
            CreateTopic: 10,
            CreateSource: 10,
            CreateTable: 10,
            CreateView: 20,
            CreateSink: 10,
            ValidateView: 20,
            Ingest: 50,
            DML: 50,
        }


class KafkaParallelInsert(Scenario):
    """A Zippy test using simple views over Kafka sources with parallel insertion."""

    def bootstrap(self) -> List[Type[Action]]:
        return [KafkaStart, MzStart]

    def config(self) -> Dict[Type[Action], float]:
        return {
            KillStoraged: 5,
            KillComputed: 5,
            CreateTopic: 10,
            CreateSource: 10,
            CreateViewSimple: 5,
            ValidateView: 10,
            KafkaInsertParallel: 50,
        }
