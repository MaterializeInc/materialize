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
- root-relative links, such as `/sql/create-cluster`, and path-relative links,
  such as `../create-role`, which do not resolve once the Markdown is read away
  from the site.

Links are read from inline links, reference definitions, and href and src
attributes, outside fenced code blocks.

Prints a summary and exits 1 if any page is over 100,000 characters, or any
link is broken or relative.

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

LINK_RES = (
    re.compile(r"\]\(([^)\s]+)"),
    re.compile(r"^ {0,3}\[[^\]]+\]:[ \t]*([./]\S*)(?:[ \t]*$|[ \t]+[\"'(])"),
    re.compile(r"(?:href|src)=\"([^\"]+)\""),
)
NOT_RELATIVE_RE = re.compile(r"^(#|//|<|[A-Za-z][A-Za-z0-9+.-]*:)")
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
            for link_re in LINK_RES:
                targets.extend(link_re.findall(line))
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
    relative: dict[str, int] = {"root": 0, "path": 0}
    relative_pages: set[str] = set()
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
            elif not NOT_RELATIVE_RE.match(target):
                relative["root" if target.startswith("/") else "path"] += 1
                relative_pages.add(name)

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
        f"Relative links: {relative['root']} root-relative and {relative['path']}"
        f" path-relative, on {len(relative_pages)} pages"
    )
    return 1 if over_fail or broken or relative_pages else 0


if __name__ == "__main__":
    sys.exit(main())
