# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Native inventory admission for the profiling fixture."""

import importlib.util
import json
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location(
    "cluster_workflow", Path(__file__).with_name("mzcompose.py")
)
assert spec is not None and spec.loader is not None
workflow = importlib.util.module_from_spec(spec)
spec.loader.exec_module(workflow)


class NativeLaunchTest(unittest.TestCase):
    def test_profile_replica_has_inventory_and_retains_override(self):
        c = Mock()
        c.sql_query.return_value = [("u7", "u9")]
        active = {}

        @contextmanager
        def override(service):
            active[service.name] = service.config
            try:
                yield
            finally:
                active.pop(service.name)

        def up(name):
            if name == "clusterd1":
                c.sql_query.assert_called_once()
                self.assertTrue(
                    any(
                        "CREATE CLUSTER profile_fetch" in call.args[0]
                        for call in c.sql.call_args_list
                    )
                )
                command = active[name]["command"]
                for option in [
                    "--catalog-cluster-id=u7",
                    "--catalog-replica-id=u9",
                    "--catalog-deployment-generation=3",
                ]:
                    self.assertIn(option, command)
                for kind in ("COMPUTE", "STORAGE"):
                    prefix = f"CLUSTERD_{kind}_TIMELY_CONFIG="
                    topology = json.loads(
                        next(
                            value.removeprefix(prefix)
                            for value in active[name]["environment"]
                            if value.startswith(prefix)
                        )
                    )
                    self.assertEqual(topology["workers"], 2)
                    self.assertEqual(len(topology["addresses"]), 1)

        class ReachedProfileRequest(Exception):
            pass

        def profile_request(*args, **kwargs):
            self.assertIn("clusterd1", active)
            raise ReachedProfileRequest

        c.up.side_effect = up
        c.override.side_effect = override
        with (
            patch.object(
                workflow,
                "native_catalog_options",
                return_value=["--catalog-deployment-generation=3"],
            ),
            patch.object(workflow.requests, "post", side_effect=profile_request),
            self.assertRaises(ReachedProfileRequest),
        ):
            workflow.workflow_test_profile_fetch(c)
        self.assertFalse(active)
        self.assertEqual(
            [call.args for call in c.up.call_args_list],
            [("materialized",), ("clusterd1",)],
        )

    def test_missing_inventory_does_not_launch_replica(self):
        c = Mock()
        c.sql_query.return_value = []
        with self.assertRaises(ValueError):
            workflow.workflow_test_profile_fetch(c)
        c.up.assert_called_once_with("materialized")
        c.override.assert_not_called()


if __name__ == "__main__":
    unittest.main()
