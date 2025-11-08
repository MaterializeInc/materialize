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

from materialize.mzcompose.composition import Composition, Service
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Testdrive(),
    Materialized(),
]


def workflow_default(c: Composition) -> None:
    # Create config file in MZ_ROOT directory
    mz_root = Path(os.environ.get("MZ_ROOT", Path(__file__).parent.parent.parent))
    config_file = mz_root / "dyncfg-test-config.json"

    try:
        system_params_1 = {
            "max_connections": 1000,
        }

        # Create a ConfigMap with the system parameters in JSON format
        with open(config_file, "w", encoding="utf-8") as f:
            f.write(json.dumps(system_params_1))
            f.flush()
            os.fsync(f.fileno())

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
                input=dedent(
                    """
                    > SHOW max_connections
                    1000
                """
                ),
            )

            system_params_2 = {
                "max_connections": 67,
                # This is a bit awkward, but it works.
                "allowed_cluster_replica_sizes": "'25cc','50cc'",
            }

            # Write updated parameters to the file
            with open(config_file, "w", encoding="utf-8") as f:
                f.write(json.dumps(system_params_2))
                f.flush()
                os.fsync(f.fileno())
            # Wait for dyncfg to sync
            c.sleep(2)
            c.testdrive(
                input=dedent(
                    """
                    > SHOW max_connections
                    67

                    > SHOW allowed_cluster_replica_sizes
                    "\\"25cc\\", \\"50cc\\""
                """
                ),
            )
    finally:
        # Clean up the config file
        if config_file.exists():
            config_file.unlink()
