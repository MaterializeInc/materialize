# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Native replica ownership at the composition boundary."""

import importlib.util
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location(
    "bounded_memory_workflow", Path(__file__).with_name("mzcompose.py")
)
assert spec is not None and spec.loader is not None
workflow = importlib.util.module_from_spec(spec)
spec.loader.exec_module(workflow)


class LifecycleTest(unittest.TestCase):
    def test_live_replica_restart_preserves_crash_coverage(self):
        self.check_lifecycle(retired=False)

    def test_retired_replica_teardown_is_idempotent(self):
        self.check_lifecycle(retired=True)

    def check_lifecycle(self, retired):
        c = Mock()
        events = []
        replica = None
        identities = iter([("u1", "u2"), ("u1", "u3")])
        current_identity = None

        @contextmanager
        def override(*services):
            nonlocal replica
            old = replica
            for service in services:
                if service.name == "clusterd":
                    replica = service
            try:
                yield
            finally:
                replica = old

        def lookup(query):
            nonlocal current_identity
            if "count(*)" in query:
                return [(0 if retired else 1,)]
            current_identity = next(identities)
            return [current_identity]

        def up(*services):
            if "materialized" in services:
                events.append("materialized")
            if "clusterd" not in services:
                return
            self.assertIsNotNone(replica)
            self.assertIsNotNone(current_identity)
            cluster, identity = current_identity
            self.assertIn(f"--catalog-cluster-id={cluster}", replica.config["command"])
            self.assertIn(f"--catalog-replica-id={identity}", replica.config["command"])
            self.assertEqual(
                replica.config["deploy"]["resources"]["limits"]["memory"], "3.5Gb"
            )
            events.append(f"start {identity}")

        def testdrive(script, args):
            self.assertEqual(args, ["--default-timeout=5m"])
            events.append(script.splitlines()[-1])

        c.override.side_effect = override
        c.sql_query.side_effect = lookup
        c.up.side_effect = up
        c.kill.side_effect = lambda *services: events.append("stop")
        c.stop.side_effect = lambda *services: events.append("retired stopped")
        c.testdrive.side_effect = testdrive
        scenario = workflow.Scenario(
            name="lifecycle",
            materialized_memory="10Gb",
            clusterd_memory="3.5Gb",
            before_replica_start="prepare initial inventory",
            pre_restart="check initial execution",
            before_replica_restart="prepare replacement inventory",
            post_restart="check replacement execution",
        )
        with patch.object(workflow, "native_catalog_options", return_value=[]):
            workflow.run_scenario(c, scenario, "10Gb", "3.5Gb")
        self.assertEqual(
            events,
            [
                "materialized",
                "prepare initial inventory",
                "start u2",
                "check initial execution",
                *(["retired stopped"] if retired else []),
                "stop",
                "materialized",
                "prepare replacement inventory",
                "start u3",
                "check replacement execution",
            ],
        )
        if retired:
            c.stop.assert_called_once_with("clusterd")
            c.kill.assert_called_once_with("materialized")
        else:
            c.stop.assert_not_called()
            c.kill.assert_called_once_with("materialized", "clusterd")


if __name__ == "__main__":
    unittest.main()
