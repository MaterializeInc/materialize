# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Native replica ownership across the load-test rehydration boundary."""

import importlib.util
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location(
    "upsert_workflow", Path(__file__).with_name("mzcompose.py")
)
assert spec is not None and spec.loader is not None
workflow = importlib.util.module_from_spec(spec)
spec.loader.exec_module(workflow)


class LoadTestLaunchTest(unittest.TestCase):
    def test_inventory_precedes_launch_and_context_survives_restart(self):
        c = Mock()
        active = {}
        inventory = False
        source = False
        launches = []

        @contextmanager
        def override(*services):
            previous = active.copy()
            active.update({service.name: service for service in services})
            try:
                yield
            finally:
                active.clear()
                active.update(previous)

        def execute(service, *args):
            nonlocal inventory, source
            if args == ("load-test/setup.td",):
                self.assertFalse(launches)
                inventory = True
            elif args == ("load-test/source.td",):
                self.assertEqual(len(launches), 1)
                source = True

        def lookup(sql):
            self.assertTrue(inventory)
            if "rehydration_latency" in sql:
                return [(len(launches),)]
            return [("u7", "u9")]

        def up(*services):
            if "clusterd1" not in services:
                return
            self.assertTrue(inventory)
            command = active["clusterd1"].config["command"]
            self.assertIn("--catalog-cluster-id=u7", command)
            self.assertIn("--catalog-replica-id=u9", command)
            self.assertIn("--catalog-deploy-generation=3", command)
            launches.append(list(command))
            if len(launches) > 1:
                self.assertTrue(source)
                self.assertIn("materialized", services)

        c.override.side_effect = override
        c.exec.side_effect = execute
        c.sql_query.side_effect = lookup
        c.up.side_effect = up
        with patch.object(
            workflow,
            "native_catalog_options",
            return_value=["--catalog-deploy-generation=3"],
        ):
            workflow.workflow_load_test(c, Mock())
        # Initial hydration plus each of the five independent configurations.
        # In particular, stall-enabled and upsert-v2 must both reach rehydration.
        self.assertEqual(len(launches), 6)
        self.assertTrue(all(command == launches[0] for command in launches))
        self.assertEqual(
            sum(
                "rehydration_latency IS NOT NULL" in call.args[0]
                for call in c.testdrive.call_args_list
            ),
            5,
        )
        self.assertFalse(active)


if __name__ == "__main__":
    unittest.main()
