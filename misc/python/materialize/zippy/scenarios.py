# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.zippy.all_actions import Action, ValidateAll  # noqa
from materialize.zippy.backup_and_restore_actions import BackupAndRestore
from materialize.zippy.balancerd_actions import (
    BalancerdRestart,
    BalancerdStart,
    BalancerdStop,
)
from materialize.zippy.blob_store_actions import BlobStoreRestart, BlobStoreStart
from materialize.zippy.crdb_actions import CockroachRestart, CockroachStart
from materialize.zippy.debezium_actions import CreateDebeziumSource, DebeziumStart
from materialize.zippy.framework import ActionFactory, ActionOrFactory  # noqa
from materialize.zippy.kafka_actions import (
    CreateTopicParameterized,
    Ingest,
    KafkaInsertParallel,
    KafkaStart,
)
from materialize.zippy.kafka_capabilities import Envelope
from materialize.zippy.mysql_actions import (
    CreateMySqlTable,
    MySqlDML,
    MySqlRestart,
    MySqlStart,
)
from materialize.zippy.mysql_cdc_actions import CreateMySqlCdcTable
from materialize.zippy.mz_actions import (
    KillClusterd,
    Mz0dtDeploy,
    MzRestart,
    MzStart,
    MzStop,
)
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
from materialize.zippy.source_actions import (
    AlterSourceConnectionParameterized,
    CreateSourceParameterized,
)
from materialize.zippy.storaged_actions import (
    StoragedKill,
    StoragedRestart,
    StoragedStart,
)
from materialize.zippy.table_actions import DML, CreateTableParameterized, ValidateTable
from materialize.zippy.view_actions import CreateViewParameterized, ValidateView


class Scenario:
    def bootstrap(self) -> list[ActionOrFactory]:
        return [
            KafkaStart,
            CockroachStart,
            BlobStoreStart,
            MzStart,
            StoragedStart,
            BalancerdStart,
        ]

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
        raise RuntimeError

    def finalization(self) -> list[ActionOrFactory]:
        return [
            MzStart,
            BalancerdStart,
            StoragedStart,
            ValidateAll(),
            BackupAndRestore,
            ValidateAll(),
        ]


class KafkaSources(Scenario):
    """A Zippy test using Kafka sources exclusively."""

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
        return {
            MzStart: 5,
            MzStop: 1,
            Mz0dtDeploy: 5,
            KillClusterd: 5,
            # Disabled because a separate clusterd is not supported by Mz0dtDeploy yet
            # StoragedKill: 5,
            # StoragedStart: 5,
            CreateTopicParameterized(): 5,
            CreateSourceParameterized(): 5,
            CreateViewParameterized(max_inputs=2): 5,
            CreateSinkParameterized(): 5,
            ValidateView: 10,
            Ingest: 100,
            PeekCancellation: 5,
        }


class AlterConnectionWithKafkaSources(Scenario):
    """A Zippy test using Kafka sources and alter connections."""

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
        return {
            MzStart: 5,
            MzStop: 1,
            Mz0dtDeploy: 1,
            KillClusterd: 5,
            # Disabled because a separate clusterd is not supported by Mz0dtDeploy yet
            # StoragedKill: 5,
            # StoragedStart: 5,
            CreateTopicParameterized(): 5,
            CreateSourceParameterized(): 5,
            CreateViewParameterized(max_inputs=2): 5,
            CreateSinkParameterized(): 5,
            AlterSourceConnectionParameterized(): 5,
            ValidateView: 10,
            Ingest: 100,
            PeekCancellation: 5,
        }


class UserTables(Scenario):
    """A Zippy test using user tables exclusively."""

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
        return {
            MzStart: 5,
            BalancerdStart: 1,
            MzStop: 10,
            Mz0dtDeploy: 10,
            BalancerdStop: 1,
            BalancerdRestart: 1,
            BackupAndRestore: 1,
            KillClusterd: 10,
            # Disabled because a separate clusterd is not supported by Mz0dtDeploy yet
            # StoragedRestart: 5,
            CreateTableParameterized(): 10,
            CreateViewParameterized(): 10,
            CreateSinkParameterized(): 10,
            ValidateTable: 20,
            ValidateView: 20,
            DML: 30,
        }


class DebeziumPostgres(Scenario):
    """A Zippy test using Debezium Postgres exclusively."""

    def bootstrap(self) -> list[ActionOrFactory]:
        return super().bootstrap() + [
            DebeziumStart,
            PostgresStart,
        ]

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
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

    def bootstrap(self) -> list[ActionOrFactory]:
        return super().bootstrap() + [PostgresStart]

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
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

    def finalization(self) -> list[ActionOrFactory]:
        # Postgres sources can't be backup up and restored since Postgres
        # refuses to re-read previously confirmed LSNs
        return [
            MzStart,
            BalancerdStart,
            StoragedStart,
            ValidateAll(),
        ]


class MySqlCdc(Scenario):
    """A Zippy test using MySQL CDC exclusively."""

    def bootstrap(self) -> list[ActionOrFactory]:
        return super().bootstrap() + [MySqlStart]

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
        return {
            CreateMySqlTable: 10,
            CreateMySqlCdcTable: 10,
            KillClusterd: 5,
            StoragedKill: 5,
            StoragedStart: 5,
            MySqlRestart: 10,
            CreateViewParameterized(): 10,
            ValidateView: 20,
            MySqlDML: 100,
        }


class ClusterReplicas(Scenario):
    """A Zippy test that uses CREATE / DROP REPLICA and random killing."""

    def bootstrap(self) -> list[ActionOrFactory]:
        return super().bootstrap() + [
            DropDefaultReplica,
            CreateReplica,
        ]

    # Due to gh#13235 it is not possible to have MzStop/MzStart in this scenario
    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
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

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
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


class CrdbBlobStoreRestart(Scenario):
    """A Zippy test that restarts CRDB and BlobStore."""

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
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
            Mz0dtDeploy: 5,
            KillClusterd: 5,
            # Disabled because a separate clusterd is not supported by Mz0dtDeploy yet
            # StoragedRestart: 10,
            CockroachRestart: 15,
            BlobStoreRestart: 15,
        }


class CrdbRestart(Scenario):
    """A Zippy test that restarts Cockroach."""

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
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
            Mz0dtDeploy: 5,
            KillClusterd: 5,
            # Disabled because a separate clusterd is not supported by Mz0dtDeploy yet
            # StoragedRestart: 10,
            CockroachRestart: 15,
        }


class KafkaSourcesLarge(Scenario):
    """A Zippy test using a large number of Kafka sources, views and sinks (no killings)."""

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
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

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
        return {
            CreateReplica: 2,
            CreateTopicParameterized(
                max_topics=2, envelopes_with_weights={Envelope.UPSERT: 100}
            ): 10,
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

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
        return {
            CreateTableParameterized(max_tables=2): 10,
            CreateViewParameterized(
                max_views=5, expensive_aggregates=True, max_inputs=5
            ): 10,
            CreateSinkParameterized(max_sinks=10): 10,
            ValidateView: 10,
            DML: 50,
        }


class BackupAndRestoreLarge(Scenario):
    """A Zippy scenario with the occasional Backup+Restore."""

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
        return {
            CreateTableParameterized(max_tables=2): 10,
            CreateViewParameterized(
                max_views=5, expensive_aggregates=True, max_inputs=5
            ): 10,
            # Sinks don't make sense in this test since we don't record the
            # state of a sink after a backup&restore cycle, see for example
            # https://github.com/MaterializeInc/database-issues/issues/9589
            # CreateSinkParameterized(max_sinks=10): 10,
            ValidateView: 10,
            DML: 50,
            BackupAndRestore: 0.1,
        }


class PostgresCdcLarge(Scenario):
    """A Zippy test using Postgres CDC exclusively (Pg not killed)."""

    def bootstrap(self) -> list[ActionOrFactory]:
        return super().bootstrap() + [PostgresStart]

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
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

    def finalization(self) -> list[ActionOrFactory]:
        # Postgres sources can't be backup up and restored since Postgres
        # refuses to re-read previously confirmed LSNs
        return [
            MzStart,
            BalancerdStart,
            StoragedStart,
            ValidateAll(),
        ]


class MySqlCdcLarge(Scenario):
    """A Zippy test using MySQL CDC exclusively (MySQL not killed)."""

    def bootstrap(self) -> list[ActionOrFactory]:
        return super().bootstrap() + [MySqlStart]

    def actions_with_weight(self) -> dict[ActionOrFactory, float]:
        return {
            CreateMySqlTable: 10,
            CreateMySqlCdcTable: 10,
            KillClusterd: 5,
            StoragedKill: 5,
            StoragedStart: 5,
            CreateViewParameterized(): 10,
            ValidateView: 20,
            MySqlDML: 100,
        }
