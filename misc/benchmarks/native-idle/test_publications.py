# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from capture_publications import capture_publications
from psycopg import sql


class PublicationDiagnosticsTest(unittest.TestCase):
    def test_failed_scrape_restores_log_filter(self):
        original = "info,mz_catalog=warn"
        connection = Mock()
        connection.execute.return_value.fetchone.return_value = (original,)
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory)
            with (
                patch("capture_publications.time.sleep"),
                self.assertRaisesRegex(OSError, "scrape unavailable"),
            ):
                capture_publications(
                    run,
                    connection,
                    {"adapterd": {}},
                    Mock(side_effect=OSError("scrape unavailable")),
                    10,
                )
            restore = connection.execute.call_args.args[0].as_string()
            self.assertEqual(
                restore,
                sql.SQL("ALTER SYSTEM SET log_filter = {}")
                .format(sql.Literal(original))
                .as_string(),
            )
            result = json.loads((run / "publication-result.json").read_text())
            self.assertEqual(result["status"], "failed")
            self.assertNotIn("measurement_status", result)

    def test_inventory_failure_preserves_completed_measurement_status(self):
        connection = Mock()
        connection.execute.return_value.fetchone.return_value = ("info",)
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory)
            with (
                patch("capture_publications.time.sleep"),
                patch(
                    "capture_publications.capture_inventory",
                    side_effect=RuntimeError("inventory failed"),
                ),
                self.assertRaisesRegex(RuntimeError, "inventory failed"),
            ):
                capture_publications(run, connection, {}, Mock(), 10)
            self.assertEqual(
                connection.execute.call_args.args[0].as_string(),
                "ALTER SYSTEM SET log_filter = 'info'",
            )
            result = json.loads((run / "publication-result.json").read_text())
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["measurement_status"], "passed")


if __name__ == "__main__":
    unittest.main()
