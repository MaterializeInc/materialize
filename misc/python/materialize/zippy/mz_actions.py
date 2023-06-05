# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent
from typing import List, Set, Type

from materialize.mzcompose import Composition
from materialize.zippy.crdb_capabilities import CockroachIsRunning
from materialize.zippy.framework import Action, Capability
from materialize.zippy.minio_capabilities import MinioIsRunning
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.view_capabilities import ViewExists


class MzStart(Action):
    """Starts a Mz instance (all components are running in the same container)."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {CockroachIsRunning, MinioIsRunning}

    def run(self, c: Composition) -> None:
        c.up("materialized")

        for config_param in [
            "max_tables",
            "max_sources",
            "max_objects_per_schema",
            "max_materialized_views",
            "max_sinks",
        ]:
            c.sql(
                f"ALTER SYSTEM SET {config_param} TO 1000",
                user="mz_system",
                port=6877,
                print_statement=False,
            )

    def provides(self) -> List[Capability]:
        return [MzIsRunning()]


class MzStop(Action):
    """Stops the entire Mz instance (all components are running in the same container)."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning}

    def run(self, c: Composition) -> None:
        c.kill("materialized")

    def withholds(self) -> Set[Type[Capability]]:
        return {MzIsRunning}


class MzRestart(Action):
    """Restarts the entire Mz instance (all components are running in the same container)."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning}

    def run(self, c: Composition) -> None:
        c.kill("materialized")
        c.up("materialized")


class KillClusterd(Action):
    """Kills the clusterd processes in the environmentd container. The process orchestrator will restart them."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, ViewExists}

    def run(self, c: Composition) -> None:
        # Depending on the workload, clusterd may not be running, hence the || true
        c.exec("materialized", "bash", "-c", "kill -9 `pidof clusterd` || true")


class ComputeStartDisabledCluster(Action):
    """Start a compute cluster that is then made unresponsive.
    Such a cluster should not have any adverse effects, such as memory
    leaks in other parts of the system.
    """

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {CockroachIsRunning, MinioIsRunning}

    def run(self, c: Composition) -> None:
        c.up("cluster_disabled")

        c.testdrive(
            dedent(
                """
                > CREATE CLUSTER cluster_disabled REPLICAS (cluster_disabled (
                  STORAGECTL ADDRESSES ['cluster_disabled:2100'],
                  STORAGE ADDRESSES ['cluster_disabled:2103'],
                  COMPUTECTL ADDRESSES ['cluster_disabled:2101'],
                  COMPUTE ADDRESSES ['cluster_disabled:2102'],
                  WORKERS 1))
                """
            )
        )

        c.pause("cluster_disabled")

    def provides(self) -> List[Capability]:
        return []


class ComputeStartDisabledReplica(Action):
    """Start a compute cluster replica that is then made unresponsive.
    Such a replica should not have any adverse effects, such as memory
    leaks in other parts of the system.
    """

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {CockroachIsRunning, MinioIsRunning}

    def run(self, c: Composition) -> None:
        c.up("compute_replica_disabled")

        c.testdrive(
            dedent(
                """
                > CREATE CLUSTER REPLICA default.compute_replica_disabled
                  STORAGECTL ADDRESSES ['compute_replica_disabled:2100'],
                  STORAGE ADDRESSES ['compute_replica_disabled:2103'],
                  COMPUTECTL ADDRESSES ['compute_replica_disabled:2101'],
                  COMPUTE ADDRESSES ['compute_replica_disabled:2102'],
                  WORKERS 1
                """
            )
        )

        c.pause("compute_replica_disabled")

    def provides(self) -> List[Capability]:
        return []
