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
- links that repeat the site's base path, or the /docs/ prefix, after it;
- root-relative links, such as `/sql/create-cluster`, and path-relative links,
  such as `../create-role`, which do not resolve once the Markdown is read away
  from the site.

Links are read from inline links, reference definitions, and href and src
attributes, outside fenced code blocks.

It also reports pages carrying Hugo shortcode syntax, such as "{{< tabs >}}",
outside fenced code blocks: a shortcode that reached the Markdown unrendered.

It also reports code fences that do not pair up: an opener glued to the end of
a line of text, a closer glued to the next opener or to text, and a fence still
open at the end of the page. Each one turns code into prose, or prose into
code, for a reader of the Markdown.

Prints a summary and exits 1 if any page is over 100,000 characters, any
link is broken or relative, any fence is broken, or any page carries shortcode
syntax. With --fail-on-links, only broken or relative links and broken fences
cause a failure, so known oversized pages and unrendered shortcodes remain
warnings in CI.

Example usage:

    $ ci/test/check-docs-markdown.py doc/user/public/docs/markdown-docs \\
        https://materialize.com/docs/markdown-docs/
"""

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

WARN_CHARS = 50_000
FAIL_CHARS = 100_000

LINK_RES = (
    re.compile(r"\]\(([^)\s]+)"),
    re.compile(r"^ {0,3}\[[^\]]+\]:[ \t]*([./]\S*)(?:[ \t]*$|[ \t]+[\"'(])"),
    re.compile(r"(?:href|src)=\"([^\"]+)\""),
)
NOT_RELATIVE_RE = re.compile(r"^(#|//|<|[A-Za-z][A-Za-z0-9+.-]*:)")
SHORTCODE_RE = re.compile(r"\{\{[<%]")
# A fence line: the run of backticks or tildes, then the info string or nothing.
# Openers are matched after any blockquote and list-item markers, closers after
# any blockquote markers.
FENCE_RE = re.compile(r"^\s*(`{3,}|~{3,})(.*)$")
CONTAINER_RE = re.compile(r"^(\s*(>|([-*+]|\d{1,9}[.)])(?=\s)))+")
BLOCKQUOTE_RE = re.compile(r"^(\s*>)+")
# An info string as the docs write them, as in "mzsql" or 'hc {hl_lines="3"}'.
INFO_STRING_RE = re.compile(r"^[\w+-]+(\s*\{[^}]*\})?$")
# A backtick fence opener at the end of a line of text, as in
# "connection:   ```mzsql".
TRAILING_OPENER_RE = re.compile(r"(?<=\S)\s*(?<!`)(`{3,})[\w+-]*\s*$")
BACKTICK_RUN_RE = re.compile(r"`{3,}")


def scan_fences(text: str) -> tuple[list[str], list[tuple[int, str]]]:
    """Return the lines of text outside fenced code blocks, and fence defects.

    A defect is a (line number, description) pair. Fences are read as CommonMark
    reads them, except that each defect is read as the fence its author meant,
    so that one broken fence does not turn the rest of the page into code.
    """
    lines: list[str] = []
    defects: list[tuple[int, str]] = []
    fence = ""
    opened_at = 0
    for number, line in enumerate(text.splitlines(), start=1):
        if fence:
            run = FENCE_RE.match(BLOCKQUOTE_RE.sub("", line))
            if not (run and run[1].startswith(fence)):
                continue
            rest = run[2].strip()
            if not rest:
                fence = ""
            elif not INFO_STRING_RE.match(rest):
                # CommonMark keeps the fence open: a closing fence takes no text.
                defects.append((number, f"text after a closing fence: {line.strip()}"))
                fence = ""
                lines.append(rest)
            elif len(run[1]) >= 2 * len(fence):
                defects.append((number, f"fences glued together: {line.strip()}"))
                fence, opened_at = run[1][len(fence) :], number
            else:
                defects.append(
                    (number, f"fence opener inside an open fence: {line.strip()}")
                )
                fence, opened_at = run[1], number
            continue
        run = FENCE_RE.match(CONTAINER_RE.sub("", line))
        if run and not (run[1][0] == "`" and "`" in run[2]):
            fence, opened_at = run[1], number
            continue
        trailing = TRAILING_OPENER_RE.search(line)
        if trailing and not BACKTICK_RUN_RE.search(line, 0, trailing.start()):
            # CommonMark reads this as text, and the closing fence that follows
            # as an opener.
            defects.append((number, f"fence opener after text: {line.strip()}"))
            fence, opened_at = trailing[1], number
            lines.append(line[: trailing.start()])
            continue
        lines.append(line)
    if fence:
        defects.append((opened_at, "fence still open at the end of the page"))
    return lines, defects


def links(lines: list[str]) -> list[str]:
    """Return the Markdown link targets in lines."""
    return [t for line in lines for link_re in LINK_RES for t in link_re.findall(line)]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("root", type=Path, help="the built markdown-docs directory")
    parser.add_argument(
        "base_url", help="the URL the directory is served at, ending in a slash"
    )
    parser.add_argument(
        "--fail-on-links",
        action="store_true",
        help="fail only for broken or relative links and broken fences",
    )
    args = parser.parse_args()

    # A root-relative source link that still carries the base path, or the
    # production /docs/ prefix, comes out with that prefix twice, as in
    # /materialize/PR/materialize/PR/sql/... or /materialize/PR/docs/sql/... .
    site_url = args.base_url.removesuffix("markdown-docs/")
    doubled_prefixes = {f"{site_url}docs/"}
    if base_path := urlparse(site_url).path.lstrip("/"):
        doubled_prefixes.add(f"{site_url}{base_path}")

    pages = sorted(args.root.rglob("*.md"))
    large: list[tuple[int, str]] = []
    broken: list[tuple[str, str]] = []
    doubled_prefix: list[tuple[str, str]] = []
    relative: dict[str, int] = {"root": 0, "path": 0}
    relative_pages: set[str] = set()
    shortcode_pages: list[str] = []
    fence_defects: list[tuple[str, int, str]] = []
    for page in pages:
        name = str(page.relative_to(args.root))
        text = page.read_text()
        if len(text) > WARN_CHARS:
            large.append((len(text), name))
        lines, defects = scan_fences(text)
        fence_defects += [(name, number, defect) for number, defect in defects]
        if any(SHORTCODE_RE.search(line) for line in lines):
            shortcode_pages.append(name)
        for target in links(lines):
            if target.startswith(args.base_url):
                path = target[len(args.base_url) :].split("#", 1)[0]
                if not (args.root / path).is_file():
                    broken.append((name, target))
            elif target.startswith(tuple(doubled_prefixes)):
                doubled_prefix.append((name, target))
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
    print(f"Links with a repeated base path or /docs/ prefix: {len(doubled_prefix)}")
    for name, target in doubled_prefix:
        print(f"  {name}: {target}")
    print(
        f"Relative links: {relative['root']} root-relative and {relative['path']}"
        f" path-relative, on {len(relative_pages)} pages"
    )
    print(f"Pages with unrendered shortcode syntax: {len(shortcode_pages)}")
    for name in shortcode_pages:
        print(f"  {name}")
    print(f"Unbalanced or glued code fences: {len(fence_defects)}")
    for name, number, defect in fence_defects:
        print(f"  {name}:{number}: {defect}")
    failed = bool(broken or doubled_prefix or relative_pages or fence_defects)
    if not args.fail_on_links:
        failed = failed or bool(over_fail or shortcode_pages)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
