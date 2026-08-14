# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import os
from pathlib import Path
from textwrap import dedent
from typing import Any

from materialize.mzcompose.composition import Composition, Service
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Testdrive(),
    Materialized(),
]

# A cluster-coherent parameter, default off, and a replica-local one, default on.
# Both declare a scope in their definitions, so both are resolved per object.
CLUSTER_PARAM = "enable_eager_delta_joins"
REPLICA_PARAM = "enable_lgalloc"
# A second cluster-coherent parameter, used to check that an unparseable scoped
# value is dropped rather than stored or fatal.
UNPARSEABLE_PARAM = "enable_join_prioritize_arranged"


def write_config(config_file: Path, params: dict[str, Any]) -> None:
    """Atomically-enough rewrite the config sync file in place. The container
    bind-mounts the file itself, so it must be truncated and rewritten rather
    than replaced, or the mount would still point at the old inode."""
    with open(config_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(params))
        f.flush()
        os.fsync(f.fileno())


def workflow_default(c: Composition) -> None:
    # Create config file in MZ_ROOT directory
    mz_root = Path(os.environ.get("MZ_ROOT", Path(__file__).parent.parent.parent))
    config_file = mz_root / "dyncfg-test-config.json"

    try:
        system_params_1 = {
            "max_connections": 1000,
        }

        # Create a ConfigMap with the system parameters in JSON format
        write_config(config_file, system_params_1)

        print(f"config file is {config_file}")
        with c.override(Materialized(config_sync_file_path=str(config_file))):
            c.up(
                "materialized",
                Service("testdrive", idle=True),
            )

            # Wait for dyncfg to sync
            # Locally this works more or less immediately, but
            # seems to be failing CI.
            c.sleep(10)
            c.testdrive(
                input=dedent("""
                    > SHOW max_connections
                    1000
                """),
            )

            system_params_2 = {
                "max_connections": 67,
                # This is a bit awkward, but it works.
                "allowed_cluster_replica_sizes": "'25cc','50cc'",
            }

            # Write updated parameters to the file
            write_config(config_file, system_params_2)
            # Wait for dyncfg to sync
            c.sleep(2)
            c.testdrive(
                input=dedent("""
                    > SHOW max_connections
                    67

                    > SHOW allowed_cluster_replica_sizes
                    "\\"25cc\\", \\"50cc\\""
                """),
            )

            # A cluster with the default `r1` replica, to be named by the scoped
            # sections below. Created while the file is still flat, so the
            # assertion that no scoped rows exist yet is meaningful.
            c.testdrive(
                input=dedent("""
                    $ postgres-execute connection=mz_system
                    CREATE CLUSTER dyncfg_scoped SIZE 'scale=1,workers=1'

                    > SELECT count(*) FROM mz_internal.mz_cluster_system_parameters
                    0

                    > SELECT count(*) FROM mz_internal.mz_replica_system_parameters
                    0
                """),
            )

            system_params_3 = {
                # Flat keys keep their environment-wide meaning alongside the
                # reserved scoped sections.
                "max_connections": 67,
                "allowed_cluster_replica_sizes": "'25cc','50cc'",
                "clusters": {
                    "dyncfg_scoped": {
                        CLUSTER_PARAM: True,
                        # Unparseable for a `bool`, so it is dropped.
                        UNPARSEABLE_PARAM: "maybe",
                    },
                    # No such cluster, so this section is ignored.
                    "dyncfg_absent": {CLUSTER_PARAM: True},
                },
                "replicas": {
                    "dyncfg_scoped": {
                        "r1": {REPLICA_PARAM: False},
                        # No such replica, so this section is ignored.
                        "r99": {REPLICA_PARAM: False},
                    },
                    # No such cluster, so this section is ignored.
                    "dyncfg_absent": {"r1": {REPLICA_PARAM: False}},
                },
            }

            write_config(config_file, system_params_3)
            c.sleep(2)
            c.testdrive(
                input=dedent(f"""
                    > SHOW max_connections
                    67

                    > SELECT c.name, p.name, p.value FROM mz_internal.mz_cluster_system_parameters p JOIN mz_clusters c ON c.id = p.cluster_id ORDER BY c.name, p.name
                    dyncfg_scoped {CLUSTER_PARAM} true

                    > SELECT c.name, r.name, p.name, p.value FROM mz_internal.mz_replica_system_parameters p JOIN mz_cluster_replicas r ON r.id = p.replica_id JOIN mz_clusters c ON c.id = r.cluster_id ORDER BY c.name, r.name, p.name
                    dyncfg_scoped r1 {REPLICA_PARAM} false
                """),
            )

            # Create-time resolution: a cluster created while a section already
            # names it folds the overrides into its create transaction, so its
            # replica's first configuration carries them. This exercises that path,
            # which the cluster above never reaches. Asserted without a sleep, but
            # the sync loop also reconciles every 100ms here, so what this pins down
            # is that the create path resolves and commits the overrides rather than
            # their exact ordering against the replica's first configuration.
            #
            # The fold resolves from the file as of the last sync tick, hence the
            # sleep after the write.
            system_params_4 = {
                **system_params_3,
                "clusters": {
                    **system_params_3["clusters"],
                    "dyncfg_scoped_2": {CLUSTER_PARAM: True},
                },
                "replicas": {
                    **system_params_3["replicas"],
                    "dyncfg_scoped_2": {"r1": {REPLICA_PARAM: False}},
                },
            }

            write_config(config_file, system_params_4)
            c.sleep(2)
            c.testdrive(
                input=dedent(f"""
                    $ postgres-execute connection=mz_system
                    CREATE CLUSTER dyncfg_scoped_2 SIZE 'scale=1,workers=1'

                    > SELECT p.name, p.value FROM mz_internal.mz_cluster_system_parameters p JOIN mz_clusters c ON c.id = p.cluster_id WHERE c.name = 'dyncfg_scoped_2'
                    {CLUSTER_PARAM} true

                    > SELECT r.name, p.name, p.value FROM mz_internal.mz_replica_system_parameters p JOIN mz_cluster_replicas r ON r.id = p.replica_id JOIN mz_clusters c ON c.id = r.cluster_id WHERE c.name = 'dyncfg_scoped_2'
                    r1 {REPLICA_PARAM} false
                """),
            )

            # Dropping the sections removes the overrides, returning every object
            # to the environment-wide value.
            write_config(config_file, system_params_2)
            c.sleep(2)
            c.testdrive(
                input=dedent("""
                    > SELECT count(*) FROM mz_internal.mz_cluster_system_parameters
                    0

                    > SELECT count(*) FROM mz_internal.mz_replica_system_parameters
                    0
                """),
            )
    finally:
        # Clean up the config file
        if config_file.exists():
            config_file.unlink()
