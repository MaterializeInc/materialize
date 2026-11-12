# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Replica launch compatibility and fail-closed native configuration checks."""

import copy
import importlib.util
import subprocess
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location(
    "feature_benchmark_workflow", Path(__file__).with_name("mzcompose.py")
)
assert spec is not None and spec.loader is not None
workflow = importlib.util.module_from_spec(spec)
spec.loader.exec_module(workflow)


class NativeLaunchTest(unittest.TestCase):
    def services(self):
        environment = "local-az1-00000000-0000-0000-0000-000000000001-0"
        mz = workflow.create_mz_service(
            "registry/materialized:v26.0.0", 1, {}, False, environment
        )
        replica = workflow.create_clusterd_service(
            "registry/clusterd:v26.0.0", 1, {}, environment
        )
        self.assertIn(f"--environment-id={environment}", mz.config["command"])
        self.assertIn(
            f"CLUSTERD_ENVIRONMENT_ID={environment}", replica.config["environment"]
        )
        c = Mock()
        c.override.side_effect = lambda *services: nullcontext()
        c.sql_query.return_value = [("u7", "u9")]
        return c, mz, replica

    def test_native_configuration_survives_executor_restart(self):
        c, mz, replica = self.services()
        c.exec.return_value = SimpleNamespace(stdout="--catalog-replica-id <ID>")
        with patch.object(
            workflow, "native_catalog_options", return_value=["--catalog-config={}"]
        ):
            workflow.start_benchmark_replica(c, mz, replica)
        self.assertEqual(replica.config["image"], mz.config["image"])
        for argument in (
            "--catalog-cluster-id=u7",
            "--catalog-replica-id=u9",
            "--catalog-config={}",
        ):
            self.assertIn(argument, replica.config["command"])
        configured = copy.deepcopy(replica.config)
        workflow.Docker(c, 1, mz, replica).RestartMzClusterd()
        self.assertEqual(replica.config, configured)
        c.override.assert_called_with(mz, replica)

    def test_historical_images_keep_their_replica_configuration(self):
        for help_output in ("", "--storage-controller-listen-addr <ADDR>"):
            with self.subTest(help_output=help_output):
                c, mz, replica = self.services()
                original = copy.deepcopy(replica.config)
                c.exec.return_value = SimpleNamespace(stdout=help_output)
                with patch.object(workflow, "native_catalog_options") as options:
                    workflow.start_benchmark_replica(c, mz, replica)
                    options.assert_not_called()
                self.assertEqual(replica.config, original)
                c.up.assert_called_once_with("clusterd")

    def test_native_errors_do_not_start_a_legacy_replica(self):
        for broken_probe in (False, True):
            with self.subTest(broken_probe=broken_probe):
                c, mz, replica = self.services()
                if broken_probe:
                    c.exec.side_effect = subprocess.CalledProcessError(
                        1, "clusterd --help"
                    )
                else:
                    c.exec.return_value = SimpleNamespace(
                        stdout="--catalog-replica-id <ID>"
                    )
                with patch.object(
                    workflow,
                    "native_catalog_options",
                    side_effect=RuntimeError("missing donor"),
                ):
                    with self.assertRaises(
                        (RuntimeError, subprocess.CalledProcessError)
                    ):
                        workflow.start_benchmark_replica(c, mz, replica)
                c.up.assert_not_called()


if __name__ == "__main__":
    unittest.main()
