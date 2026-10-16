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

    def test_balanced_fences_pass_and_hide_their_content(self) -> None:
        content = (
            "```mzsql\nSELECT 1;\n```\n"
            "- ```\n  {{< tabs >}}\n  ```\n"
            "> ~~~~\n> ```\n> ~~~~\n"
            "````markdown\n```mzsql\n```\n````\n"
            "    {{< tabs >}}\n"
        )
        result = self.check(content, "--fail-on-links")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("Unbalanced or glued code fences: 0", result.stdout)
        self.assertIn("Pages with unrendered shortcode syntax: 0", result.stdout)

    def test_short_backtick_runs_are_not_fence_defects(self) -> None:
        content = (
            "Use `` `code` `` or ``` `` ``` in text.\n\n"
            "```mzsql\nSELECT '``';\n``\n```\n"
        )
        result = self.check(content, "--fail-on-links")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("Unbalanced or glued code fences: 0", result.stdout)

    def test_links_in_code_and_after_fence_are_read_as_commonmark(self) -> None:
        content = (
            "```mzsql\n[Code](https://example.com/a)\n```\n"
            "<https://example.com/b> [Relative](/sql/create-cluster/)\n"
        )
        result = self.check(content, "--fail-on-links")
        self.assertEqual(result.returncode, 1)
        self.assertIn("Unbalanced or glued code fences: 0", result.stdout)
        self.assertIn("Relative links: 1 root-relative", result.stdout)

    def test_links_other_than_inline_links_are_checked(self) -> None:
        content = (
            "![Image](../image.png)\n\n"
            "[ref]: /sql/create-role/\n"
            "[unused]: ../create-user/\n\n"
            '<div><a href="/sql/create-cluster/">Cluster</a></div>\n\n'
            'Inline <img src="../logo.png"> HTML.\n'
        )
        result = self.check(content, "--fail-on-links")
        self.assertEqual(result.returncode, 1)
        self.assertIn(
            "Relative links: 2 root-relative and 3 path-relative", result.stdout
        )

    def test_percent_encoded_link_to_existing_page_is_not_broken(self) -> None:
        (self.root / "a b.md").write_text("# Page\n")
        result = self.check(f"[Page]({BASE_URL}a%20b.md)", "--fail-on-links")
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_fence_marker_after_text_fails_link_gate(self) -> None:
        content = (
            "1. Create a connection:   ```mzsql\n"
            "   CREATE CONNECTION c TO SSH TUNNEL (HOST 'h');\n"
            "   ```\n"
            "\n"
            "## Heading ```\n"
        )
        result = self.check(content, "--fail-on-links")
        self.assertEqual(result.returncode, 1)
        self.assertIn("index.md:1: fence marker inside text", result.stdout)
        self.assertIn("index.md:5: fence marker inside text", result.stdout)

    def test_glued_fences_fail_link_gate(self) -> None:
        content = "```mzsql\nSELECT 1;\n``````nofmt\n 1\n```\n"
        result = self.check(content, "--fail-on-links")
        self.assertEqual(result.returncode, 1)
        self.assertIn("Unbalanced or glued code fences: 1", result.stdout)
        self.assertIn("index.md:3: text after a closing fence", result.stdout)

    def test_text_after_closing_fence_fails_link_gate(self) -> None:
        content = "```mzsql\nSHOW TABLES;\n```The results include\n"
        result = self.check(content, "--fail-on-links")
        self.assertEqual(result.returncode, 1)
        self.assertIn("index.md:3: text after a closing fence", result.stdout)

    def test_unexpected_info_string_fails_link_gate(self) -> None:
        content = "```mzsql and more\nSHOW TABLES;\n```\n"
        result = self.check(content, "--fail-on-links")
        self.assertEqual(result.returncode, 1)
        self.assertIn("Unbalanced or glued code fences: 1", result.stdout)
        self.assertIn("index.md:1: unexpected fence info string", result.stdout)

    def test_info_string_with_attributes_passes(self) -> None:
        content = '```hc {hl_lines="3"}\nSHOW TABLES;\n```\n'
        result = self.check(content, "--fail-on-links")
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_fence_open_at_end_of_page_fails_link_gate(self) -> None:
        content = "# Example\n\n```shell\nmz-debug --version\n"
        result = self.check(content, "--fail-on-links")
        self.assertEqual(result.returncode, 1)
        self.assertIn("Unbalanced or glued code fences: 1", result.stdout)
        self.assertIn("index.md:3: fence not closed", result.stdout)

    def test_fence_closed_only_by_its_container_fails_link_gate(self) -> None:
        content = "- ```mzsql\n  SELECT 1;\n\nAfter the list.\n"
        result = self.check(content, "--fail-on-links")
        self.assertEqual(result.returncode, 1)
        self.assertIn("index.md:1: fence not closed", result.stdout)
        content = "> ```mzsql\n> SELECT 1;\n\nAfter the quote.\n"
        result = self.check(content, "--fail-on-links")
        self.assertEqual(result.returncode, 1)
        self.assertIn("index.md:1: fence not closed", result.stdout)

    def test_link_inside_fenced_code_fails_link_gate(self) -> None:
        content = (
            "```markdown\n"
            "[Root](/sql/create-cluster/)\n"
            "[Path](../create-role)\n"
            '[ref]: /sql/create-role/ "Title"\n'
            '<a href="./x">x</a>\n'
            f"[Site]({BASE_URL.removesuffix('markdown-docs/')}sql/)\n"
            "[Absolute](https://example.com/a)\n"
            "[Anchor](#a)\n"
            "```\n"
        )
        result = self.check(content, "--fail-on-links")
        self.assertEqual(result.returncode, 1)
        self.assertIn("Unbalanced or glued code fences: 5", result.stdout)
        for number, target in [
            (2, "/sql/create-cluster/"),
            (3, "../create-role"),
            (4, "/sql/create-role/"),
            (5, "./x"),
            (6, f"{BASE_URL.removesuffix('markdown-docs/')}sql/"),
        ]:
            self.assertIn(
                f"index.md:{number}: link inside fenced code: {target}", result.stdout
            )
        self.assertIn(
            "Relative links: 0 root-relative and 0 path-relative", result.stdout
        )


if __name__ == "__main__":
    unittest.main()
