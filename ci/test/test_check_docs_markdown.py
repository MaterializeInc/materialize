#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Exercise the Markdown link gate against a small built-docs fixture."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

CHECKER = Path(__file__).with_name("check-docs-markdown.py")
BASE_URL = "https://preview.materialize.com/materialize/39470/markdown-docs/"


class MarkdownLinkGateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name) / "markdown-docs"
        self.root.mkdir()
        self.page = self.root / "index.md"

    def check(
        self, content: str, *options: str, base_url: str = BASE_URL
    ) -> subprocess.CompletedProcess[str]:
        self.page.write_text(content)
        return subprocess.run(
            [sys.executable, str(CHECKER), str(self.root), base_url, *options],
            capture_output=True,
            text=True,
            check=False,
        )

    def test_known_size_and_shortcode_warnings_do_not_fail_link_gate(self) -> None:
        content = "# Example\n{{< tabs >}}\n" + "x" * 100_000
        self.assertEqual(self.check(content, "--fail-on-links").returncode, 0)
        self.assertEqual(self.check(content).returncode, 1)

    def test_missing_markdown_page_fails_link_gate(self) -> None:
        result = self.check(f"[Missing]({BASE_URL}missing/index.md)", "--fail-on-links")
        self.assertEqual(result.returncode, 1)
        self.assertIn("Broken links to Markdown pages: 1", result.stdout)

    def test_relative_link_fails_link_gate(self) -> None:
        result = self.check("[Relative](/sql/create-cluster/)", "--fail-on-links")
        self.assertEqual(result.returncode, 1)
        self.assertIn("Relative links: 1 root-relative", result.stdout)

    def test_doubled_docs_prefix_fails_link_gate(self) -> None:
        bad_url = BASE_URL.removesuffix("markdown-docs/") + "docs/sql/create-cluster/"
        result = self.check(f"[Malformed]({bad_url})", "--fail-on-links")
        self.assertEqual(result.returncode, 1)
        self.assertIn(
            "Links with a repeated base path or /docs/ prefix: 1", result.stdout
        )
        self.assertIn(bad_url, result.stdout)

    def test_repeated_base_path_fails_link_gate(self) -> None:
        site_url = BASE_URL.removesuffix("markdown-docs/")
        bad_url = site_url + "materialize/39470/sql/types/numeric"
        result = self.check(f"[Malformed]({bad_url})", "--fail-on-links")
        self.assertEqual(result.returncode, 1)
        self.assertIn(
            "Links with a repeated base path or /docs/ prefix: 1", result.stdout
        )
        self.assertIn(bad_url, result.stdout)

    def test_site_at_root_flags_no_link_as_repeated(self) -> None:
        result = self.check(
            "[Page](https://example.com/sql/types/numeric)",
            "--fail-on-links",
            base_url="https://example.com/markdown-docs/",
        )
        self.assertEqual(result.returncode, 0, result.stdout)


if __name__ == "__main__":
    unittest.main()
