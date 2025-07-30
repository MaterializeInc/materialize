# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import glob
import subprocess

from materialize import MZ_ROOT, spawn
from materialize.cloudtest import DEFAULT_K8S_CONTEXT_NAME, DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.util.wait import wait


def test_successful_zip_creation(mz: MaterializeApplication) -> None:
    # Wait until the Materialize instance is ready
    wait(
        condition="condition=Ready",
        resource="pod",
        label="cluster.environmentd.materialize.cloud/cluster-id=u1",
    )

    print("-- Port forwarding the internal SQL port")
    subprocess.Popen(
        [
            "kubectl",
            "--context",
            DEFAULT_K8S_CONTEXT_NAME,
            "port-forward",
            "pods/environmentd-0",
            "6877:6877",
        ]
    )

    print("-- Running mz-debug")
    spawn.runv(
        [
            "cargo",
            "run",
            "--bin",
            "mz-debug",
            "--",
            "self-managed",
            "--k8s-context",
            DEFAULT_K8S_CONTEXT_NAME,
            "--k8s-namespace",
            DEFAULT_K8S_NAMESPACE,
            "--mz-connection-url",
            "postgresql://mz_system@localhost:6877/materialize",
        ],
        cwd=MZ_ROOT,
    )

    print("-- Looking for mz-debug zip files")
    zip_files = glob.glob(str(MZ_ROOT / "mz_debug*.zip"))
    assert len(zip_files) > 0, "No mz-debug zip file was created"
