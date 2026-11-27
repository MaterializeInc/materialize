# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re
import shlex

from materialize.buildkite_insights.buildkite_api import generic_api

# See https://buildkite.com/docs/pipelines/links-and-images-in-log-output
_ESCAPE_SEQUENCE_RE = re.compile(
    r"""
    \x1b_[^\x07\x1b]*(?:\x07|\x1b\\)        # APC, Buildkite's timestamps
    | \x1b\[[0-?]*[ -/]*[@-~]               # CSI, colors
    | \x1b\]([^\x07\x1b]*)(?:\x07|\x1b\\)   # OSC, Buildkite's links and images
    | \x07
    """,
    re.VERBOSE,
)


def download_log(pipeline_slug: str, build_number: int, job_id: str) -> str:
    """Returns the job's log as plain text, with links and images shown as their labels."""
    request_path = f"organizations/materialize/pipelines/{pipeline_slug}/builds/{build_number}/jobs/{job_id}/log"
    json = generic_api.get(request_path, {}, as_json=True)
    log = _ESCAPE_SEQUENCE_RE.sub(_render_escape_sequence, json.get("content", ""))
    # The agent's pseudo-terminal adds "\r" before "\n". Docker's progress output
    # puts cursor movement between them, so this has to run after the escapes.
    return re.sub(r"\r+\n", "\n", log)


def _render_escape_sequence(match: re.Match[str]) -> str:
    code, _, attributes = (match[1] or "").partition(";")
    if code not in ("1338", "1339"):
        return ""
    # Buildkite quotes and escapes attributes like a shell, see `tokenizeString`
    # in https://github.com/buildkite/terminal-to-html.
    # NOTE: Unlike Buildkite, this mishandles upper-case keys, backslashes in
    # single-quoted values, and escape sequences inside values. Our emitters,
    # `buildkite.inline_link`, `buildkite.inline_image` and the
    # ci/test/console scripts, write none of these.
    lexer = shlex.shlex(attributes, posix=True)
    lexer.whitespace, lexer.whitespace_split, lexer.commenters = ";", True, ""
    try:
        attribute = {k: v for k, _, v in (token.partition("=") for token in lexer)}
    except ValueError:  # unclosed quote
        return ""
    return attribute.get("content") or attribute.get("alt") or attribute.get("url", "")
