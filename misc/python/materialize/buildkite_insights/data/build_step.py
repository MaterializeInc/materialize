# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass
from datetime import datetime


@dataclass
class BuildItemOutcomeBase:
    step_key: str
    build_number: int
    commit_hash: str
    created_at: datetime
    duration_in_min: float | None
    passed: bool
    completed: bool
    retry_count: int

    def formatted_date(self) -> str:
        return self.created_at.strftime("%Y-%m-%d %H:%M:%S %z")


@dataclass
class BuildStepOutcome(BuildItemOutcomeBase):
    """Outcome of an atomic build step. For sharded jobs, more than one build step exists for a job."""

    id: str
    web_url_to_job: str
    parallel_job_index: int | None
    exit_status: int | None

    def web_url_to_build(self) -> str:
        return self.web_url_to_job[: self.web_url_to_job.index("#")]


@dataclass
class BuildJobOutcome(BuildItemOutcomeBase):
    """Outcome, which aggregates multiple build steps in case of a sharded job."""

    ids: list[str]
    web_url_to_build: str
    # number of merged shards, 1 otherwise
    count_items: int


@dataclass
class BuildStepMatcher:
    step_key: str
    # will be ignored if not specified
    parallel_job_index: int | None

    def matches(self, job_step_key: str, job_parallel_index: int | None) -> bool:
        if self.step_key != job_step_key:
            return False

        if self.parallel_job_index is None:
            # not specified
            return True

        return self.parallel_job_index == job_parallel_index
