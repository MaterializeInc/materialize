# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests for `Composition.invoke`'s stdout/stderr merging.

`invoke` reads a subprocess's stdout and stderr as two separate pipes and
prints them to the merged CI log. If it printed partial reads verbatim, a line
from one stream could be split by a chunk of the other. The real-world symptom:
testdrive writes an error line to stdout while writing its `^^^ +++` Buildkite
uncollapse marker to stderr, and the marker lands in the middle of the error
line, so `ci-annotate-errors` can no longer match it to a known issue.
"""

from __future__ import annotations

from materialize.mzcompose.composition import _split_complete_lines


def _merge(chunks: list[tuple[str, str]]) -> str:
    """Merge (stream, chunk) reads the way `invoke` does, holding partial lines.

    Mirrors the reader loop: accumulate each stream's partial line and emit only
    complete lines, flushing any leftover at EOF.
    """
    partial = {"out": "", "err": ""}
    merged = ""
    for stream, chunk in chunks:
        complete, partial[stream] = _split_complete_lines(partial[stream] + chunk)
        merged += complete
    for stream in ("out", "err"):
        merged += partial[stream]
    return merged


def test_split_complete_lines_holds_partial() -> None:
    assert _split_complete_lines("a\nb\nc") == ("a\nb\n", "c")
    assert _split_complete_lines("no newline") == ("", "no newline")
    assert _split_complete_lines("done\n") == ("done\n", "")
    # A carriage return also flushes, so live progress is not held back.
    assert _split_complete_lines("progress\r") == ("progress\r", "")


def test_marker_does_not_split_error_line() -> None:
    # The reader read the error line so far (no newline yet), then drained the
    # full stderr marker block, then read the rest of the error line.
    chunks = [
        ("out", "    1: ERROR: internal error: internal err"),
        ("err", "^^^ +++\n+++ !!! Error Report\n1 errors were encountered\n"),
        ("out", "or in optimizer: recursion limit exceeded\n"),
    ]

    merged = _merge(chunks)

    assert (
        "    1: ERROR: internal error: internal error in optimizer: recursion limit exceeded"
        in merged
    )
    assert "internal err^^^ +++" not in merged


def test_full_output_preserved() -> None:
    # Every byte still ends up in the merged output, just at line boundaries.
    chunks = [
        ("out", "line one\npartial "),
        ("err", "stderr line\n"),
        ("out", "rest\n"),
    ]

    merged = _merge(chunks)

    assert merged.count("\n") == 3
    assert "partial rest" in merged
    assert "stderr line" in merged
