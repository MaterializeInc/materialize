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


class RehydrationStarted(Exception):
    pass


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
            if len(launches) == 2:
                self.assertTrue(source)
                self.assertIn("materialized", services)
                raise RehydrationStarted

        c.override.side_effect = override
        c.exec.side_effect = execute
        c.sql_query.side_effect = lookup
        c.up.side_effect = up
        with patch.object(
            workflow,
            "native_catalog_options",
            return_value=["--catalog-deploy-generation=3"],
        ):
            with self.assertRaises(RehydrationStarted):
                workflow.workflow_load_test(c, Mock())
        self.assertEqual(launches[0], launches[1])
        self.assertFalse(active)


if __name__ == "__main__":
    unittest.main()
