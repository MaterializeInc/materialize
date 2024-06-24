# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass


@dataclass
class BuildHistoryEntry:
    url_to_job: str
    passed: bool


@dataclass
class BuildHistory:
    pipeline: str
    branch: str
    last_build_step_outcomes: list[BuildHistoryEntry]

    def has_entries(self) -> bool:
        return len(self.last_build_step_outcomes) > 0

    def to_markdown(self) -> str:
        return (
            f'<a href="/materialize/{self.pipeline}/builds?branch=main">main</a> history: '
            + "".join(
                [
                    f"<a href=\"{outcome.url_to_job}\">{':bk-status-passed:' if outcome.passed else ':bk-status-failed:'}</a>"
                    for outcome in self.last_build_step_outcomes
                ]
            )
        )
