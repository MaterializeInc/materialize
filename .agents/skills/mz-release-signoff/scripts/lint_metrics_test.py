# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests for the mz-release-signoff metric lint.

Every failure mode here is a silent pass rather than a wrong answer: the lint
stops seeing a name and CI stays green. A green run is therefore not evidence
that the extractor works, which is why the extraction is tested directly on
fixtures instead of through the skill's own markdown.
"""

from lint_metrics import (
    REFERENCE_NAME_COUNTS,
    names_in,
    names_in_document,
    reference_name_counts,
    resolve_continuation,
)


def full_names(text):
    return [name for _, name, base in names_in_document(text) if base is None]


def continuations(text):
    return [(name, base) for _, name, base in names_in_document(text) if base]


def test_names_after_a_fence_are_found():
    # Pairing backticks across a fence flips the parity of every span after it,
    # so prose is captured as code and the real spans become the separators.
    text = "```\nmz_inside_fence\n```\n\nAfter the fence `mz_after_fence`.\n"
    assert "mz_after_fence" in full_names(text)


def test_tilde_and_indented_and_unterminated_fences_are_extracted():
    # A query inside a fence names its metrics bare, so a fence shape that is
    # not extracted contributes nothing: the prose scan only sees backticks.
    for fence in (
        "~~~\nrate(mz_in_fence[5m])\n~~~",
        "  ```\nrate(mz_in_fence[5m])\n  ```",
        "```\nrate(mz_in_fence[5m])",
    ):
        assert "mz_in_fence" in full_names(f"Prose.\n\n{fence}\n"), fence


def test_name_in_a_promql_selector_is_found():
    # A label matcher lands in the same whitespace-delimited word as the metric,
    # and the skill's fenced queries are written in exactly this shape.
    token = 'sum(rate(mz_storage_shard_count{mz_version!~".*-dev.*"}[5m]))'
    assert "mz_storage_shard_count" in list(names_in(token))


def test_glob_stem_is_skipped():
    # The stem of a globbed family is not itself a catalogued name.
    assert list(names_in("mz_persist_*_bytes")) == []


def test_bare_prefix_is_not_a_metric():
    # Prose names a prefix as "its `mz_compute_` prefix". Nothing continues it,
    # so there is no name to check.
    assert list(names_in("mz_compute_")) == []


def test_brace_expansion_resolves_to_its_base():
    assert list(names_in("mz_row_set_finishing_seconds_{bucket,sum,count}")) == [
        "mz_row_set_finishing_seconds"
    ]


def test_roster_continuation_carries_its_base():
    text = "| `mz_persist_gc_seconds`, `_started` | counter |\n"
    assert continuations(text) == [("_started", "mz_persist_gc_seconds")]


def test_continuation_does_not_cross_lines():
    # Carried across lines a continuation attaches to whatever name appeared in
    # an earlier paragraph, which manufactures failures rather than finding them.
    text = "A rate of `mz_some_metric_seconds`.\n\nProse about `_started`.\n"
    assert continuations(text) == []


def test_bare_histogram_suffix_is_prose():
    text = "The `_sum` rate of `mz_slow_message_handling` is the busy time.\n"
    assert continuations(text) == []
    assert full_names(text) == ["mz_slow_message_handling"]


def test_continuation_resolving_two_ways_reports_both():
    # Deleting the intended metric leaves the shorter cut resolving, so the row
    # stays green while pointing at nothing. The caller rejects that.
    exact = {"mz_storage_upsert_merge_snapshot_deletes_total"} | {
        "mz_storage_upsert_deletes_total"
    }
    hits = resolve_continuation(
        "_deletes_total",
        "mz_storage_upsert_merge_snapshot_updates_total",
        exact,
        set(),
        set(),
    )
    assert hits == [
        "mz_storage_upsert_merge_snapshot_deletes_total",
        "mz_storage_upsert_deletes_total",
    ]


def test_continuation_pinned_to_one_metric_resolves_once():
    hits = resolve_continuation(
        "_started", "mz_persist_gc_seconds", {"mz_persist_gc_started"}, set(), set()
    )
    assert hits == ["mz_persist_gc_started"]


def test_every_reference_file_contributes_its_pinned_count():
    # A parity bug drops a whole file at once and changes no name that remains,
    # so only a count catches it. Update these deliberately, never to go green.
    assert reference_name_counts() == REFERENCE_NAME_COUNTS
