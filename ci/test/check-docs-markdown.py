#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Report on the size and links of the agent-readable Markdown docs.

Reads a built markdown-docs tree, the output of
`hugo --config config.toml,config.skill.toml`, and reports:

- pages over the 50,000 and 100,000 character thresholds of the
  Agent-Friendly Documentation Spec (https://agentdocsspec.com/spec/web/);
- links to another Markdown page, by the URL the page is served at, that name
  no file in the tree;
- root-relative links, such as `/sql/create-cluster`, which do not resolve
  once the Markdown is read away from the site.

Prints a summary and exits 1 if any page is over 100,000 characters or any
link is broken.

Example usage:

    $ ci/test/check-docs-markdown.py doc/user/public/docs/markdown-docs \\
        https://materialize.com/docs/markdown-docs/
"""

import argparse
import re
import sys
from pathlib import Path

WARN_CHARS = 50_000
FAIL_CHARS = 100_000

LINK_RE = re.compile(r"\]\(([^)\s]+)")
FENCE_RE = re.compile(r"^(```|~~~)")


def links(text: str) -> list[str]:
    """Return the Markdown link targets in text, skipping fenced code blocks."""
    targets = []
    fenced = False
    for line in text.splitlines():
        if FENCE_RE.match(line.lstrip()):
            fenced = not fenced
            continue
        if not fenced:
            targets.extend(LINK_RE.findall(line))
    return targets


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("root", type=Path, help="the built markdown-docs directory")
    parser.add_argument(
        "base_url", help="the URL the directory is served at, ending in a slash"
    )
    args = parser.parse_args()

    pages = sorted(args.root.rglob("*.md"))
    large: list[tuple[int, str]] = []
    broken: list[tuple[str, str]] = []
    root_relative: dict[str, int] = {}
    for page in pages:
        name = str(page.relative_to(args.root))
        text = page.read_text()
        if len(text) > WARN_CHARS:
            large.append((len(text), name))
        for target in links(text):
            if target.startswith(args.base_url):
                path = target[len(args.base_url) :].split("#", 1)[0]
                if not (args.root / path).is_file():
                    broken.append((name, target))
            elif target.startswith("/"):
                root_relative[name] = root_relative.get(name, 0) + 1

    over_fail = [entry for entry in large if entry[0] > FAIL_CHARS]
    print(f"Markdown pages: {len(pages)}")
    print(f"Over {WARN_CHARS:,} characters: {len(large)}")
    print(f"Over {FAIL_CHARS:,} characters: {len(over_fail)}")
    for chars, name in sorted(large, reverse=True):
        print(f"  {chars:>9,}  {name}")
    print(f"Broken links to Markdown pages: {len(broken)}")
    for name, target in broken:
        print(f"  {name}: {target}")
    print(
        f"Root-relative links: {sum(root_relative.values())},"
        f" on {len(root_relative)} pages"
    )
    return 1 if over_fail or broken else 0


if __name__ == "__main__":
    sys.exit(main())
