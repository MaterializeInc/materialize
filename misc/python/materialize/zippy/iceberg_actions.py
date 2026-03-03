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
from materialize.mzcompose.helpers.iceberg import setup_polaris_for_iceberg
from materialize.zippy.balancerd_capabilities import BalancerdIsRunning
from materialize.zippy.framework import (
    Action,
    ActionFactory,
    Capabilities,
    Capability,
    State,
)
from materialize.zippy.iceberg_capabilities import IcebergIsRunning, IcebergSinkExists
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.replica_capabilities import source_capable_clusters
from materialize.zippy.storaged_capabilities import StoragedRunning
from materialize.zippy.view_capabilities import ViewExists


class IcebergStart(Action):
    """Bootstrap Polaris and MinIO for Iceberg sinks."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {BalancerdIsRunning, MzIsRunning}

    @classmethod
    def incompatible_with(cls) -> set[type[Capability]]:
        return {IcebergIsRunning}

    def run(self, c: Composition, state: State) -> None:
        c.up("postgres")
        username, key = setup_polaris_for_iceberg(c)
        state.iceberg_username = username
        state.iceberg_key = key

        c.testdrive(
            dedent(
                f"""
                > CREATE SECRET IF NOT EXISTS iceberg_access_key_secret AS '{key}'
                > CREATE CONNECTION IF NOT EXISTS iceberg_aws_conn TO AWS (
                    ACCESS KEY ID = '{username}',
                    SECRET ACCESS KEY = SECRET iceberg_access_key_secret,
                    ENDPOINT = 'http://minio:9000/',
                    REGION = 'us-east-1'
                  )
                > CREATE CONNECTION IF NOT EXISTS iceberg_polaris_conn TO ICEBERG CATALOG (
                    CATALOG TYPE = 'REST',
                    URL = 'http://polaris:8181/api/catalog',
                    CREDENTIAL = 'root:root',
                    WAREHOUSE = 'default_catalog',
                    SCOPE = 'PRINCIPAL_ROLE:ALL'
                  )
                """
            ),
            mz_service=state.mz_service,
        )

    def provides(self) -> list[Capability]:
        return [IcebergIsRunning()]


class CreateIcebergSinkParameterized(ActionFactory):
    """Creates an Iceberg sink over an existing view."""

    @classmethod
    def requires(cls) -> list[set[type[Capability]]]:
        return [
            {
                BalancerdIsRunning,
                MzIsRunning,
                StoragedRunning,
                ViewExists,
                IcebergIsRunning,
            }
        ]

    def __init__(self, max_sinks: int = 10) -> None:
        self.max_sinks = max_sinks

    def new(self, capabilities: Capabilities) -> list[Action]:
        new_sink_name = capabilities.get_free_capability_name(
            IcebergSinkExists, self.max_sinks
        )

        if new_sink_name:
            source_view = random.choice(capabilities.get(ViewExists))
            cluster_name = random.choice(source_capable_clusters(capabilities))

            return [
                CreateIcebergSink(
                    sink=IcebergSinkExists(
                        name=new_sink_name,
                        source_view=source_view,
                        cluster_name=cluster_name,
                    ),
                    capabilities=capabilities,
                ),
            ]
        else:
            return []


class CreateIcebergSink(Action):
    def __init__(
        self,
        sink: IcebergSinkExists,
        capabilities: Capabilities,
    ) -> None:
        assert (
            sink is not None
        ), "CreateIcebergSink Action can not be referenced directly, it is produced by CreateIcebergSinkParameterized factory"
        self.sink = sink
        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
        table_name = self.sink.name.replace("-", "_")
        c.testdrive(
            dedent(
                f"""
                > CREATE SINK {self.sink.name}
                  IN CLUSTER {self.sink.cluster_name}
                  FROM {self.sink.source_view.name}
                  INTO ICEBERG CATALOG CONNECTION iceberg_polaris_conn (
                    NAMESPACE 'default_namespace',
                    TABLE '{table_name}'
                  )
                  USING AWS CONNECTION iceberg_aws_conn
                  KEY (count_all) NOT ENFORCED
                  MODE UPSERT
                  WITH (COMMIT INTERVAL '10s')

                > SELECT status FROM mz_internal.mz_sink_statuses WHERE name = '{self.sink.name}'
                running
                """
            ),
            mz_service=state.mz_service,
        )

    def provides(self) -> list[Capability]:
        return [self.sink]
