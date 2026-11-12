# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Native identity and topology at the limits launcher boundary."""

import importlib.util
import json
import unittest
from contextlib import ExitStack, contextmanager
from pathlib import Path
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location(
    "limits_workflow", Path(__file__).with_name("mzcompose.py")
)
assert spec is not None and spec.loader is not None
workflow = importlib.util.module_from_spec(spec)
spec.loader.exec_module(workflow)


class NativeLaunchTest(unittest.TestCase):
    def test_setup_declares_inventory_before_starting_replicas(self):
        c = Mock()
        statements = []
        c.sql.side_effect = lambda sql, **kwargs: statements.append(sql)

        def start(composition, replicas, overrides):
            sql = "\n".join(statements)
            for cluster, _, _, _ in replicas:
                self.assertIn(f"CREATE CLUSTER {cluster} REPLICAS", sql)
            self.assertEqual([workers for _, _, _, workers in replicas], [3, 3, 3, 1])
            self.assertEqual(sum(len(names) for _, _, names, _ in replicas), 7)
            self.assertIs(composition, c)
            for call in c.up.call_args_list:
                self.assertFalse(
                    any(name.startswith("clusterd_") for name in call.args)
                )

        with (
            patch.object(workflow, "setup_sql_server_testing"),
            patch.object(
                workflow, "setup_polaris_for_iceberg", return_value=("user", "key")
            ),
            patch.object(
                workflow, "start_native_replicas", side_effect=start
            ) as launch,
            ExitStack() as overrides,
        ):
            workflow.setup(c, 3, overrides)
        launch.assert_called_once()

    def test_missing_inventory_does_not_start_processes(self):
        c = Mock()
        cursor = Mock()
        cursor.fetchall.return_value = []
        c.sql_cursor.return_value.__enter__ = Mock(return_value=cursor)
        c.sql_cursor.return_value.__exit__ = Mock(return_value=False)
        with patch.object(workflow, "native_catalog_options", return_value=[]):
            with ExitStack() as overrides:
                with self.assertRaises(ValueError):
                    workflow.start_native_replicas(
                        c, [("missing", "replica", ["node0", "node1"], 2)], overrides
                    )
        c.up.assert_not_called()
        c.override.assert_not_called()

    def test_all_processes_receive_current_identity_and_original_topology(self):
        c = Mock()
        cursor = Mock()
        c.sql_cursor.return_value.__enter__ = Mock(return_value=cursor)
        c.sql_cursor.return_value.__exit__ = Mock(return_value=False)
        cursor.fetchall.side_effect = [
            [("u1", "u2")],
            [("u3", "u4")],
            [("u5", "u6")],
            [("u7", "u8")],
        ]
        active = {}
        launches = []

        @contextmanager
        def override(service):
            old = active.get(service.name)
            active[service.name] = service
            try:
                yield
            finally:
                if old is None:
                    active.pop(service.name)
                else:
                    active[service.name] = old

        def up(*names):
            self.assertEqual(set(active), set(names))
            launches.append([active[name].config for name in names])

        c.override.side_effect = override
        c.up.side_effect = up
        replicas = [
            ("cluster", "replica", ["node0", "node1"], 2),
            ("single", "replica", ["single0"], 1),
        ]
        with patch.object(
            workflow,
            "native_catalog_options",
            return_value=["--catalog-deploy-generation=9"],
        ):
            with ExitStack() as overrides:
                workflow.start_native_replicas(c, replicas, overrides)
                workflow.start_native_replicas(c, replicas, overrides)
        self.assertFalse(active)
        for turn, identities in zip(
            launches,
            [(("u1", "u2"), ("u3", "u4")), (("u5", "u6"), ("u7", "u8"))],
            strict=True,
        ):
            for position, config in enumerate(turn):
                cluster, replica = identities[0 if position < 2 else 1]
                self.assertIn(f"--catalog-cluster-id={cluster}", config["command"])
                self.assertIn(f"--catalog-replica-id={replica}", config["command"])
                self.assertIn("--catalog-deploy-generation=9", config["command"])
                for prefix in (
                    "CLUSTERD_COMPUTE_TIMELY_CONFIG=",
                    "CLUSTERD_STORAGE_TIMELY_CONFIG=",
                ):
                    topology = json.loads(
                        next(
                            e.removeprefix(prefix)
                            for e in config["environment"]
                            if e.startswith(prefix)
                        )
                    )
                    self.assertEqual(topology["workers"], 2 if position < 2 else 1)
                    self.assertIn(
                        f"CLUSTERD_PROCESS={position if position < 2 else 0}",
                        config["environment"],
                    )
                    self.assertEqual(
                        len(topology["addresses"]), 2 if position < 2 else 1
                    )
        self.assertEqual(
            cursor.execute.call_args_list[0].args[1], ("cluster", "replica")
        )
        self.assertEqual(
            cursor.execute.call_args_list[1].args[1], ("single", "replica")
        )


if __name__ == "__main__":
    unittest.main()
