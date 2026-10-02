# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# docs_markdown.py - Render a Markdown copy of every page in a built Hugo site.

"""Render a Markdown copy of every page in a built Hugo docs site.

For each `<dir>/index.html` that has a `<main>`, writes `<dir>/index.md` from
the page's `<article>`. Generating from the built HTML keeps the Markdown in
parity with what readers see: every shortcode, include, and data-driven table
has already been expanded by Hugo.

Elements carrying `data-markdown-ignore` are left out, which is the same
marker agent-docs checkers honor when comparing the two renditions.

Links and images are rewritten to absolute URLs against `--base-url`, because
an agent that receives the Markdown has usually lost the URL it came from.
"""

import argparse
import json
import re
import sys
from collections.abc import Iterator
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin

VOID_TAGS = frozenset(
    "area base br col embed hr img input link meta source track wbr".split()
)

# Interactive or decorative elements with no Markdown meaning.
DROPPED_TAGS = frozenset(
    "button form input noscript script select style svg template textarea".split()
)

BLOCK_TAGS = frozenset(
    """address article aside blockquote dd details div dl dt fieldset figcaption
    figure footer h1 h2 h3 h4 h5 h6 header hr legend li main nav ol p pre section
    summary table tbody td tfoot th thead tr ul""".split()
)

# Callout shortcodes render as a <div> with one of these classes.
CALLOUT_CLASSES = frozenset(
    """annotation callout error important note private-preview public-preview
    tip warning""".split()
)

# Chroma language names that consumers do not recognize, mapped to ones they do.
CODE_LANGUAGE_ALIASES = {"mzsql": "sql", "nofmt": "text", "none": ""}


@dataclass
class Element:
    tag: str
    attrs: dict[str, str]
    children: list["Element | str"] = field(default_factory=list)

    @property
    def classes(self) -> set[str]:
        return set(self.attrs.get("class", "").split())

    def iter(self) -> Iterator["Element"]:
        yield self
        for child in self.children:
            if isinstance(child, Element):
                yield from child.iter()

    def find(self, tag: str) -> "Element | None":
        return next((e for e in self.iter() if e.tag == tag), None)

    def text(self) -> str:
        return "".join(
            c if isinstance(c, str) else c.text()
            for c in self.children
            if isinstance(c, str) or c.tag not in DROPPED_TAGS
        )


class TreeBuilder(HTMLParser):
    """Builds an `Element` tree, tolerating the unclosed tags HTML allows."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = Element("#root", {})
        self.stack = [self.root]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        element = Element(tag, {k: v or "" for k, v in attrs})
        self.stack[-1].children.append(element)
        if tag not in VOID_TAGS:
            self.stack.append(element)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.stack[-1].children.append(Element(tag, {k: v or "" for k, v in attrs}))

    def handle_endtag(self, tag: str) -> None:
        # Close back to the matching open element. An end tag with no open
        # match is stray and ignored.
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]
                return

    def handle_data(self, data: str) -> None:
        self.stack[-1].children.append(data)


def parse_html(html: str) -> Element:
    builder = TreeBuilder()
    builder.feed(html)
    builder.close()
    return builder.root


@dataclass
class Page:
    title: str
    description: str
    body: str


class PageError(Exception):
    pass


def collapse_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def escape_text(text: str) -> str:
    """Escapes characters that would otherwise start Markdown syntax."""
    text = re.sub(r"([\\`*\[\]])", r"\\\1", text)
    # An underscore between word characters cannot open emphasis.
    text = re.sub(r"(?<!\w)_|_(?!\w)", r"\\_", text)
    # `<` followed by a letter, `/` or `!` would read as raw HTML.
    text = re.sub(r"<(?=[A-Za-z/!])", r"\\<", text)
    return text


def escape_block_start(line: str) -> str:
    """Escapes a leading character that would turn a paragraph into a block."""
    if re.match(r"(#{1,6}(\s|$)|>|[-+*](\s|$)|=+\s*$)", line):
        return "\\" + line
    return re.sub(r"^(\d+)([.)])(\s|$)", r"\1\\\2\3", line)


def code_span(text: str) -> str:
    text = collapse_whitespace(text)
    if not text.strip():
        return ""
    fence = "`" * (max((len(m) for m in re.findall(r"`+", text)), default=0) + 1)
    if text.startswith("`") or text.endswith("`"):
        text = f" {text} "
    return f"{fence}{text}{fence}"


def wrap_inline(marker: str, text: str) -> str:
    """Wraps `text` in an emphasis marker, keeping edge whitespace outside it."""
    stripped = text.strip()
    if not stripped:
        return text
    lead = text[: len(text) - len(text.lstrip())]
    trail = text[len(text.rstrip()) :]
    return f"{lead}{marker}{stripped}{marker}{trail}"


class Converter:
    def __init__(self, page_url: str) -> None:
        self.page_url = page_url

    def url(self, href: str) -> str:
        if href.startswith("#"):
            return href
        return urljoin(self.page_url, href)

    def convert(self, document: Element) -> Page:
        main = document.find("main")
        if main is None:
            raise PageError("page has no <main>")
        article = main.find("article")
        if article is None:
            raise PageError("<main> has no <article>")
        h1 = article.find("h1")
        title = collapse_whitespace(h1.text()).strip() if h1 is not None else ""
        if not title:
            title_element = document.find("title")
            if title_element is not None:
                title = collapse_whitespace(title_element.text()).strip()
        description = ""
        for meta in document.iter():
            if meta.tag == "meta" and meta.attrs.get("name") == "description":
                description = collapse_whitespace(meta.attrs.get("content", "")).strip()
                break
        directive = next(
            (e for e in document.iter() if "llms-txt-directive" in e.classes), None
        )
        if directive is None:
            raise PageError("page has no .llms-txt-directive")
        blocks = [quote([self.paragraph(directive.children)])] + self.blocks(article)
        return Page(title, description, "\n\n".join(blocks))

    def blocks(self, element: Element) -> list[str]:
        """Renders an element's children as a list of Markdown blocks."""
        out: list[str] = []
        run: list[Element | str] = []

        def flush() -> None:
            paragraph = self.paragraph(run)
            if paragraph:
                out.append(paragraph)
            run.clear()

        for child in element.children:
            if isinstance(child, Element) and self.is_block(child):
                flush()
                out.extend(self.block(child))
            else:
                run.append(child)
        flush()
        return out

    def is_block(self, element: Element) -> bool:
        return element.tag in BLOCK_TAGS

    def paragraph(self, nodes: list[Element | str]) -> str:
        text = "".join(self.inline(node) for node in nodes)
        lines = [line.strip() for line in text.split("\n")]
        lines = [escape_block_start(line) for line in lines if line]
        return "  \n".join(lines)

    def block(self, element: Element) -> list[str]:
        if element.tag in DROPPED_TAGS or "data-markdown-ignore" in element.attrs:
            return []
        tag = element.tag
        classes = element.classes
        if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            text = self.inline_text(element)
            return [f"{'#' * int(tag[1])} {text}"] if text else []
        if tag == "p" and "heading" in classes:
            text = self.inline_text(element)
            return [f"**{text}**"] if text else []
        if tag == "pre":
            return [self.code_block(element)]
        if tag in ("ul", "ol"):
            return [self.list_block(element)] if self.list_items(element) else []
        if tag == "li":
            return [self.list_item(element, "- ")]
        if tag == "table":
            return self.table(element)
        if tag == "hr":
            return ["---"]
        if tag == "blockquote" or (tag == "div" and classes & CALLOUT_CLASSES):
            inner = self.blocks(element)
            return [quote(inner)] if inner else []
        if tag == "div" and "annotation-title" in classes:
            text = self.inline_text(element)
            return [f"**{text}**"] if text else []
        if tag == "div" and "tab-pane" in classes:
            label = collapse_whitespace(element.attrs.get("title", "")).strip()
            heading = [f"**{escape_text(label)}**"] if label else []
            return heading + self.blocks(element)
        if tag in ("summary", "dt"):
            text = self.inline_text(element)
            return [f"**{text}**"] if text else []
        return self.blocks(element)

    def inline_text(self, element: Element) -> str:
        return collapse_whitespace(self.inline(element)).strip()

    def inline(self, node: Element | str) -> str:
        if isinstance(node, str):
            return escape_text(collapse_whitespace(node))
        if node.tag in DROPPED_TAGS or "data-markdown-ignore" in node.attrs:
            return ""
        tag = node.tag
        if tag == "br":
            return "\n"
        if tag == "img":
            src = node.attrs.get("src", "")
            if not src:
                return ""
            alt = escape_text(collapse_whitespace(node.attrs.get("alt", "")).strip())
            return f"![{alt}]({self.url(src)})"
        if tag in ("code", "kbd", "samp"):
            return code_span(node.text())
        inner = "".join(self.inline(child) for child in node.children)
        if tag == "a":
            href = node.attrs.get("href", "")
            text = collapse_whitespace(inner).strip()
            if not href or not text:
                return inner
            title = node.attrs.get("title", "").replace('"', '\\"')
            suffix = f' "{title}"' if title else ""
            lead = " " if inner[:1].isspace() else ""
            trail = " " if inner[-1:].isspace() else ""
            return f"{lead}[{text}]({self.url(href)}{suffix}){trail}"
        if tag in ("strong", "b"):
            return wrap_inline("**", inner)
        if tag in ("em", "i"):
            return wrap_inline("*", inner)
        if tag in ("del", "s"):
            return wrap_inline("~~", inner)
        if tag == "li":
            return f"\n- {inner.strip()}\n"
        if tag in BLOCK_TAGS:
            # A block element in inline context, such as a <p> in a table
            # cell, is separated from its neighbors by a line break.
            return f"\n{inner.strip()}\n"
        return inner

    def code_block(self, pre: Element) -> str:
        code = pre.find("code")
        source = code if code is not None else pre
        text = source.text().rstrip("\n")
        if "mermaid" in pre.classes:
            language = "mermaid"
        else:
            language = source.attrs.get("data-lang", "")
            if not language:
                language = next(
                    (
                        c.removeprefix("language-")
                        for c in source.classes
                        if c.startswith("language-")
                    ),
                    "",
                )
            language = CODE_LANGUAGE_ALIASES.get(language, language)
        longest = max((len(m) for m in re.findall(r"`+", text)), default=0)
        fence = "`" * max(3, longest + 1)
        return f"{fence}{language}\n{text}\n{fence}"

    def list_items(self, element: Element) -> list[Element]:
        return [c for c in element.children if isinstance(c, Element) and c.tag == "li"]

    def list_block(self, element: Element) -> str:
        ordered = element.tag == "ol"
        start = int(element.attrs.get("start", "1") or "1") if ordered else 1
        items = []
        for i, item in enumerate(self.list_items(element)):
            marker = f"{start + i}. " if ordered else "- "
            items.append(self.list_item(item, marker))
        return "\n".join(items)

    def list_item(self, item: Element, marker: str) -> str:
        blocks = self.blocks(item)
        if not blocks:
            return marker.rstrip()
        # Paragraph-only items stay tight; any other block needs a blank line
        # to stay part of the item.
        tight = all(re.match(r"(- |\d+\. )", b) for b in blocks[1:])
        separator = "\n" if tight else "\n\n"
        body = separator.join(blocks)
        indent = " " * len(marker)
        lines = body.split("\n")
        return "\n".join(
            [marker + lines[0]] + [indent + line if line else "" for line in lines[1:]]
        )

    def table(self, table: Element) -> list[str]:
        rows = [
            [
                c
                for c in row.children
                if isinstance(c, Element) and c.tag in ("td", "th")
            ]
            for row in table_rows(table)
        ]
        rows = [row for row in rows if row]
        if not rows:
            return []
        if any(
            e.tag in ("pre", "table")
            for row in rows
            for cell in row
            for e in cell.iter()
            if e is not cell
        ):
            return self.table_as_blocks(rows)
        header, body = rows[0], rows[1:]
        width = max(len(row) for row in rows)

        def line(cells: list[str]) -> str:
            cells = cells + [""] * (width - len(cells))
            return "| " + " | ".join(cells) + " |"

        out = [line([self.cell(c) for c in header]), line(["---"] * width)]
        out.extend(line([self.cell(c) for c in row]) for row in body)
        return ["\n".join(out)]

    def cell(self, cell: Element) -> str:
        text = "".join(self.inline(child) for child in cell.children)
        lines = [line.strip() for line in text.split("\n")]
        return "<br>".join(line for line in lines if line).replace("|", "\\|")

    def table_as_blocks(self, rows: list[list[Element]]) -> list[str]:
        """Renders a table whose cells hold code blocks or nested tables.

        A pipe table cannot hold either, so each row is written as a sequence
        of blocks, labeled by the header cells when the table has several
        columns.
        """
        header: list[str] = []
        if all(c.tag == "th" for c in rows[0]):
            header = [self.inline_text(c) for c in rows[0]]
            rows = rows[1:]
        out: list[str] = []
        for row in rows:
            for i, cell in enumerate(row):
                blocks = self.blocks(cell)
                if not blocks:
                    continue
                if len(row) > 1 and i < len(header) and header[i]:
                    out.append(f"**{header[i]}**")
                out.extend(blocks)
        return out


def table_rows(table: Element) -> list[Element]:
    """Returns the table's own rows, excluding rows of nested tables."""
    rows = []
    for child in table.children:
        if not isinstance(child, Element):
            continue
        if child.tag == "tr":
            rows.append(child)
        elif child.tag in ("thead", "tbody", "tfoot"):
            rows.extend(table_rows(child))
    return rows


def quote(blocks: list[str]) -> str:
    lines = "\n\n".join(blocks).split("\n")
    return "\n".join(f"> {line}" if line else ">" for line in lines)


def render(page: Page) -> str:
    front_matter = [f"title: {json.dumps(page.title, ensure_ascii=False)}"]
    if page.description:
        front_matter.append(
            f"description: {json.dumps(page.description, ensure_ascii=False)}"
        )
    return "---\n" + "\n".join(front_matter) + "\n---\n\n" + page.body.strip() + "\n"


def page_url(base_url: str, site_root: Path, html_path: Path) -> str:
    relative = html_path.parent.relative_to(site_root).as_posix()
    base = base_url.rstrip("/") + "/"
    return base if relative == "." else f"{base}{relative}/"


def convert_site(site_root: Path, base_url: str) -> tuple[int, list[str]]:
    """Writes index.md beside every page's index.html.

    Returns the number of pages written and a list of per-page errors. Files
    without a <main>, such as alias redirect stubs, are skipped.
    """
    written = 0
    errors: list[str] = []
    for html_path in sorted(site_root.rglob("index.html")):
        document = parse_html(html_path.read_text())
        if document.find("main") is None:
            continue
        converter = Converter(page_url(base_url, site_root, html_path))
        try:
            page = converter.convert(document)
        except PageError as e:
            errors.append(f"{html_path.relative_to(site_root)}: {e}")
            continue
        html_path.with_name("index.md").write_text(render(page))
        written += 1
    return written, errors


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="docs-markdown",
        description="Render a Markdown copy of every page in a built Hugo site.",
    )
    parser.add_argument(
        "site_root",
        type=Path,
        help="the directory Hugo built the site into, e.g. public/docs",
    )
    parser.add_argument(
        "--base-url",
        required=True,
        help="the absolute URL site_root is served at, e.g. https://materialize.com/docs/",
    )
    args = parser.parse_args()
    if not re.match(r"https?://", args.base_url):
        parser.error("--base-url must be an absolute http(s) URL")
    written, errors = convert_site(args.site_root, args.base_url)
    for error in errors:
        print(f"docs-markdown: {error}", file=sys.stderr)
    print(f"docs-markdown: wrote {written} Markdown pages")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
