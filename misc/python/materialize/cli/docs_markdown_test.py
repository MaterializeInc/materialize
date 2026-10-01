# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path

import pytest

from materialize.cli.docs_markdown import (
    Converter,
    PageError,
    convert_site,
    parse_html,
    render,
)

PAGE_URL = "https://materialize.com/docs/sql/create-index/"


def body(article: str) -> str:
    html = f"<html><body><main><article>{article}</article></main></body></html>"
    return Converter(PAGE_URL).convert(parse_html(html)).body


def test_links_and_images_are_absolute() -> None:
    assert body('<p>See <a href="/docs/sql/">SQL</a>.</p>') == (
        "See [SQL](https://materialize.com/docs/sql/)."
    )
    assert body('<p><a href="../views/">views</a></p>') == (
        "[views](https://materialize.com/docs/sql/views/)"
    )
    assert body('<p><img src="/docs/images/a.svg" alt="A"></p>') == (
        "![A](https://materialize.com/docs/images/a.svg)"
    )


def test_fragment_links_stay_relative() -> None:
    assert body('<p><a href="#syntax">Syntax</a></p>') == "[Syntax](#syntax)"


def test_chroma_code_block() -> None:
    html = (
        '<div class="highlight"><pre class="chroma"><code class="language-mzsql" '
        'data-lang="mzsql"><span class="line"><span class="cl"><span class="k">'
        'SELECT</span> 1;\n</span></span><span class="line"><span class="cl">'
        "SELECT 2;\n</span></span></code></pre></div>"
    )
    assert body(html) == "```sql\nSELECT 1;\nSELECT 2;\n```"


def test_code_fence_outgrows_backticks_in_code() -> None:
    assert body("<pre><code>a ``` b</code></pre>") == "````\na ``` b\n````"


def test_inline_code_with_backtick() -> None:
    assert body("<p><code>a`b</code></p>") == "``a`b``"


def test_text_that_looks_like_markdown_is_escaped() -> None:
    assert body("<p>*not emphasis* and [not a link]</p>") == (
        "\\*not emphasis\\* and \\[not a link\\]"
    )
    assert body("<p># not a heading</p>") == "\\# not a heading"
    assert body("<p>1. not a list</p>") == "1\\. not a list"
    assert body("<p>&lt;cluster_name&gt;</p>") == "\\<cluster_name>"


def test_intraword_underscores_are_not_escaped() -> None:
    assert body("<p>mz_internal and _x_</p>") == "mz_internal and \\_x\\_"


def test_nested_lists() -> None:
    html = "<ul><li>a<ul><li>b</li></ul></li><li>c</li></ul>"
    assert body(html) == "- a\n  - b\n- c"


def test_ordered_list_start() -> None:
    assert body('<ol start="3"><li>x</li><li>y</li></ol>') == "3. x\n4. y"


def test_list_item_with_code_block() -> None:
    html = "<ol><li><p>Run:</p><pre><code>ls\n</code></pre></li></ol>"
    assert body(html) == "1. Run:\n\n   ```\n   ls\n   ```"


def test_pipe_table() -> None:
    html = (
        "<table><thead><tr><th>Field</th><th>Use</th></tr></thead><tbody>"
        "<tr><td><code>a|b</code></td><td><p>One.</p><p>Two.</p></td></tr>"
        "</tbody></table>"
    )
    assert body(html) == ("| Field | Use |\n| --- | --- |\n| `a\\|b` | One.<br>Two. |")


def test_table_with_code_blocks_renders_as_blocks() -> None:
    html = (
        "<table><tr><th>Function</th></tr><tr><td><pre><code>f(x)</code></pre>"
        "<p>Does f.</p></td></tr></table>"
    )
    assert body(html) == "```\nf(x)\n```\n\nDoes f."


def test_nested_table_rows_stay_in_the_nested_table() -> None:
    html = (
        "<table><tr><th>Outer</th></tr><tr><td><table><tr><th>Inner</th></tr>"
        "<tr><td>x</td></tr></table></td></tr></table>"
    )
    assert body(html) == "| Inner |\n| --- |\n| x |"


def test_callout_and_tabs() -> None:
    html = (
        '<div class="note"><strong class="gutter">NOTE:</strong> Careful.</div>'
        '<div class="code-tabs"><ul class="nav-tabs"></ul><div class="tab-content">'
        '<div class="tab-pane" title="Cloud"><p>Cloud text.</p></div>'
        '<div class="tab-pane" title="Self-Managed"><p>SM text.</p></div>'
        "</div></div>"
    )
    assert body(html) == (
        "> **NOTE:** Careful.\n\n**Cloud**\n\nCloud text.\n\n"
        "**Self-Managed**\n\nSM text."
    )


def test_ignored_and_interactive_elements_are_dropped() -> None:
    html = (
        '<div class="title-row"><h1>Title</h1>'
        '<a data-markdown-ignore href="x.md">View as Markdown</a></div>'
        "<p>Text<button>Copy</button><svg><title>icon</title></svg>.</p>"
        "<script>var x = 1;</script>"
    )
    assert body(html) == "# Title\n\nText."


def test_hard_line_break() -> None:
    assert body("<p>a<br>b</p>") == "a  \nb"


def test_front_matter() -> None:
    html = (
        '<html><head><title>T | Docs</title><meta name="description" '
        'content="Says &quot;hi&quot;."></head><body><main><article><h1>Title</h1>'
        "</article></main></body></html>"
    )
    page = Converter(PAGE_URL).convert(parse_html(html))
    assert render(page) == (
        '---\ntitle: "Title"\ndescription: "Says \\"hi\\"."\n---\n\n# Title\n'
    )


def test_page_without_article_is_an_error() -> None:
    with pytest.raises(PageError):
        Converter(PAGE_URL).convert(parse_html("<main><p>x</p></main>"))


def test_convert_site(tmp_path: Path) -> None:
    page = tmp_path / "sql" / "index.html"
    page.parent.mkdir()
    page.write_text('<main><article><a href="/docs/x/">x</a></article></main>')
    # Alias redirect stubs have no <main> and get no Markdown.
    alias = tmp_path / "old" / "index.html"
    alias.parent.mkdir()
    alias.write_text('<meta http-equiv="refresh" content="0; url=/docs/sql/">')

    written, errors = convert_site(tmp_path, "https://materialize.com/docs")

    assert (written, errors) == (1, [])
    assert (
        "[x](https://materialize.com/docs/x/)" in (page.parent / "index.md").read_text()
    )
    assert not (alias.parent / "index.md").exists()
