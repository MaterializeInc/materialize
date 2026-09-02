# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path

from materialize.cli import ci_annotate_errors


def test_log_start_at_last(tmp_path: Path) -> None:
    log_file = tmp_path / "run.log"
    log_file.write_text(
        "environmentd: fatal: historical setup failure\n"
        "--- Upgrading to v26.0.0\n"
        "environmentd: fatal: historical upgrade failure\n"
        "--- Upgrading to current-build\n"
        "environmentd: fatal: current build failure\n"
    )

    errors = ci_annotate_errors.get_errors(
        [str(log_file)], log_start_at_last="--- Upgrading to"
    )

    assert errors == [
        ci_annotate_errors.ErrorLog(
            b"environmentd: fatal: current build failure", str(log_file)
        )
    ]


def test_log_start_marker_absent(tmp_path: Path) -> None:
    log_file = tmp_path / "run.log"
    log_file.write_text("environmentd: fatal: setup failure\n")

    errors = ci_annotate_errors.get_errors(
        [str(log_file)], log_start_at_last="--- Upgrading to"
    )

    assert errors == [
        ci_annotate_errors.ErrorLog(
            b"environmentd: fatal: setup failure", str(log_file)
        )
    ]
