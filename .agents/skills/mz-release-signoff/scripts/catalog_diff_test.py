# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from catalog_diff import catalog_names, report

SINGLE_QUOTED = """\
metrics:
- name: '*_requests_total'
  help: 'Requests: all of them.'
- name: mz_a
  help: a
"""

DOUBLE_QUOTED_FOLDED = """\
metrics:
- name: "*_requests_total"
  help: "Requests: all of them."
- name: mz_a
  help: >-
    a
"""


def test_quoting_and_folding_do_not_change_names():
    assert catalog_names(SINGLE_QUOTED) == {"*_requests_total", "mz_a"}
    assert catalog_names(DOUBLE_QUOTED_FOLDED) == catalog_names(SINGLE_QUOTED)


def test_report_lists_added_and_removed_sorted():
    lines = report("v1", {"mz_a", "mz_b"}, "v2", {"mz_b", "mz_d", "mz_c"})
    assert lines == [
        "v1: 2 metrics",
        "v2: 3 metrics",
        "",
        "Added in v2 (a before-window on these is empty by construction):",
        "  + mz_c",
        "  + mz_d",
        "",
        "Removed in v2 (an after-window on these is empty by construction):",
        "  - mz_a",
    ]
