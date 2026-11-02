#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Report on the size, links and code fences of the agent-readable Markdown docs.

Reads a built markdown-docs tree, the output of
`hugo --config config.toml,config.skill.toml`, parses each page as CommonMark,
and reports:

- pages over the 50,000 and 100,000 character thresholds of the
  Agent-Friendly Documentation Spec (https://agentdocsspec.com/spec/web/);
- links to another Markdown page, by the URL the page is served at, that name
  no file in the tree;
- links that repeat the site's base path, or the /docs/ prefix, after it;
- root-relative links, such as `/sql/create-cluster`, and path-relative links,
  such as `../create-role`, which do not resolve once the Markdown is read away
  from the site.

Links are the inline links, images, autolinks and reference definitions the
parse finds, and the href and src attributes of raw HTML. Text in code is not
a link.

It also reports pages carrying Hugo shortcode syntax, such as "{{< tabs >}}",
outside code blocks: a shortcode that reached the Markdown unrendered.

It also reports code fences that a reader of the Markdown sees as code turned
into prose, or prose into code:

- a run of three or more backticks inside a paragraph or heading, such as an
  opener glued to the end of a line of text;
- text after a closing fence, as in "```The results", which keeps the fence open;
- an info string other than a language name with optional {attributes};
- a fence that is not closed by a fence of its own, so that it runs to the end
  of its list item, blockquote or the page;
- a link inside fenced code that markdown-body.html rewrites, since the rewrite
  does not skip code: a root-relative or path-relative target, or one under the
  site URL.

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
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple
from urllib.parse import unquote, urlparse

from markdown_it import MarkdownIt
from markdown_it.token import Token

WARN_CHARS = 50_000
FAIL_CHARS = 100_000

MARKDOWN = MarkdownIt("commonmark")
NEWLINE_RE = re.compile(r"\r\n?")
HTML_LINK_RE = re.compile(r"(?:href|src)=\"([^\"]+)\"")
# The link forms markdown-body.html rewrites, as (prefix, target, end). Keep
# in sync with $forms there.
REWRITTEN_LINK_RES = (
    re.compile(r"(\]\()([^)\s]+)([)\s])"),
    re.compile(r"(^[ \t]{0,3}\[[^\]]+\]:[ \t]*)([./]\S*)([ \t]*$|[ \t]+[\"'(])"),
    re.compile(r"((?:href|src)=\")([^\"]+)(\")"),
)
NOT_RELATIVE_RE = re.compile(r"^(#|//|<|[A-Za-z][A-Za-z0-9+.-]*:)")
SHORTCODE_RE = re.compile(r"\{\{[<%]")
# An info string as the docs write them, as in "mzsql" or 'hc {hl_lines="3"}'.
INFO_STRING_RE = re.compile(r"^[\w+-]+(\s*\{[^}]*\})?$")
BACKTICK_RUN_RE = re.compile(r"`{3,}")
# What precedes a fence line's own content inside blockquotes.
CONTAINER_PREFIX_RE = re.compile(r"^[\s>]+")


class Page(NamedTuple):
    targets: list[str]
    has_shortcode: bool
    fence_defects: list[tuple[int, str]]


def walk(tokens: list[Token]) -> Iterator[Token]:
    """Yield tokens and, for images, the tokens of their alt text."""
    for token in tokens:
        yield token
        if token.type == "image":
            yield from walk(token.children or [])


def fence_run(line: str, marker: str) -> tuple[int, str]:
    """Return the length of the run of marker that starts line, and what follows.

    Leading whitespace and blockquote markers are skipped.
    """
    body = CONTAINER_PREFIX_RE.sub("", line)
    run = len(body) - len(body.lstrip(marker))
    return run, body[run:].strip()


def check_fence(token: Token, lines: list[str], site_url: str) -> list[tuple[int, str]]:
    """Return the (line number, description) defects of a fence token."""
    assert token.map
    start, end = token.map
    marker, width = token.markup[0], len(token.markup)
    defects = []

    def add(index: int, description: str) -> None:
        defects.append((index + 1, f"{description}: {lines[index].strip()}"))

    info = token.info.strip()
    if info and not INFO_STRING_RE.match(info):
        add(start, "unexpected fence info string")
    for index in range(start + 1, end):
        run, rest = fence_run(lines[index], marker)
        if run >= width and rest:
            add(index, "text after a closing fence")
    # A fence markdown-it closes maps through its closing line, so the last
    # mapped line is a closer exactly when the fence was closed.
    run, rest = fence_run(lines[end - 1], marker)
    if end - 1 == start or run < width or rest:
        add(start, "fence not closed")
    for index in range(start, end):
        for link_re in REWRITTEN_LINK_RES:
            for match in link_re.finditer(lines[index]):
                target = match[2]
                if target.startswith(site_url) or not NOT_RELATIVE_RE.match(target):
                    defects.append((index + 1, f"link inside fenced code: {target}"))
    return defects


def scan_page(text: str, site_url: str) -> Page:
    """Parse a page and return its link targets, shortcodes and fence defects.

    Line numbers are those of markdown-it, which splits lines on \\n, \\r and
    \\r\\n only.
    """
    text = NEWLINE_RE.sub("\n", text)
    lines = text.split("\n")
    env: dict = {}
    tokens = MARKDOWN.parse(text, env)
    # Every definition, used or not, is rewritten by markdown-body.html. A
    # reference link's link_open carries its definition's href, so links whose
    # href is a definition's are skipped below to count each target once.
    references = {reference["href"] for reference in env.get("references", {}).values()}
    targets = list(references)
    code_lines: set[int] = set()
    defects: list[tuple[int, str]] = []
    for token in tokens:
        match token.type:
            case "fence":
                assert token.map
                code_lines.update(range(*token.map))
                defects += check_fence(token, lines, site_url)
            case "code_block":
                assert token.map
                code_lines.update(range(*token.map))
            case "html_block":
                targets += HTML_LINK_RE.findall(token.content)
            case "inline":
                assert token.map
                line = token.map[0]
                for child in walk(token.children or []):
                    match child.type:
                        case "link_open" if child.attrs["href"] not in references:
                            targets.append(str(child.attrs["href"]))
                        case "image":
                            targets.append(str(child.attrs["src"]))
                        case "html_inline":
                            targets += HTML_LINK_RE.findall(child.content)
                        case "softbreak" | "hardbreak":
                            line += 1
                        case "text" if BACKTICK_RUN_RE.search(child.content):
                            defects.append(
                                (
                                    line + 1,
                                    f"fence marker inside text: {lines[line].strip()}",
                                )
                            )
    has_shortcode = any(
        SHORTCODE_RE.search(line)
        for number, line in enumerate(lines)
        if number not in code_lines
    )
    return Page([t for t in targets if t], has_shortcode, sorted(defects))


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
        scanned = scan_page(text, site_url)
        fence_defects += [
            (name, number, defect) for number, defect in scanned.fence_defects
        ]
        if scanned.has_shortcode:
            shortcode_pages.append(name)
        for target in scanned.targets:
            if target.startswith(args.base_url):
                # markdown-it percent-encodes the targets it returns.
                path = unquote(target[len(args.base_url) :].split("#", 1)[0])
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
