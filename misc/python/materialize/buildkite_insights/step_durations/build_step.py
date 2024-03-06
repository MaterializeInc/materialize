#!/usr/bin/env python3

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
from typing import Any


@dataclass
class BuildStepOutcome:
    ids: list[str]
    step_key: str
    parallel_job_index: int | None
    build_number: int
    created_at: datetime
    duration_in_min: float | None
    passed: bool
    exit_status: int | None
    retry_count: int
    web_url: str
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


def extract_build_step_data(
    builds_data: list[Any],
    selected_build_steps: list[BuildStepMatcher],
    merge_sharded_executions: bool,
) -> list[BuildStepOutcome]:
    result = []
    for build in builds_data:
        step_infos = _extract_build_step_data_from_build(build, selected_build_steps)

        if merge_sharded_executions:
            step_infos = _merge_sharded_steps(step_infos)

        result.extend(step_infos)

    return result


def _extract_build_step_data_from_build(
    build_data: Any, selected_build_steps: list[BuildStepMatcher]
) -> list[BuildStepOutcome]:
    collected_steps = []

    for job in build_data["jobs"]:
        if not job.get("step_key"):
            continue

        if not _shall_include_build_step(job, selected_build_steps):
            continue

        if job["state"] in ["canceled", "running"]:
            continue

        id = build_data["id"]
        build_number = build_data["number"]
        created_at = datetime.fromisoformat(job["created_at"])
        build_step_key = job["step_key"]
        parallel_job_index = job.get("parallel_group_index")

        if job.get("started_at") and job.get("finished_at"):
            started_at = datetime.fromisoformat(job["started_at"])
            finished_at = datetime.fromisoformat(job["finished_at"])
            duration_in_min = (finished_at - started_at).total_seconds() / 60
        else:
            duration_in_min = None

        job_passed = job["state"] == "passed"
        exit_status = job.get("exit_status")
        retry_count = job.get("retries_count") or 0

        assert (
            not job_passed or duration_in_min is not None
        ), "Duration must be available for passed step"

        step_data = BuildStepOutcome(
            [id],
            build_step_key,
            parallel_job_index,
            build_number,
            created_at,
            duration_in_min,
            job_passed,
            exit_status=exit_status,
            retry_count=retry_count,
            web_url=f"{build_data['web_url']}#{job['id']}",
            count_items=1,
        )
        collected_steps.append(step_data)

    return collected_steps


def _shall_include_build_step(
    job: Any, selected_build_steps: list[BuildStepMatcher]
) -> bool:
    if len(selected_build_steps) == 0:
        return True

    job_step_key = job["step_key"]
    job_parallel_index = job.get("parallel_group_index")

    for build_step_matcher in selected_build_steps:
        if build_step_matcher.matches(job_step_key, job_parallel_index):
            return True

    return False


def _merge_sharded_steps(step_infos: list[BuildStepOutcome]) -> list[BuildStepOutcome]:
    step_info_by_key: dict[str, list[BuildStepOutcome]] = dict()

    for step_info in step_infos:
        executions = step_info_by_key.get(step_info.step_key) or []
        executions.append(step_info)
        step_info_by_key[step_info.step_key] = executions

    result = []

    for step_key, executions in step_info_by_key.items():
        if len(executions) == 1:
            result.append(executions[0])
        else:
            result.append(_merge_executions(executions))

    return result


def _merge_executions(
    executions_of_same_step: list[BuildStepOutcome],
) -> BuildStepOutcome:
    any_execution = executions_of_same_step[0]

    ids = [s.ids[0] for s in executions_of_same_step]
    min_created_at = min([s.created_at for s in executions_of_same_step])
    durations = [
        s.duration_in_min
        for s in executions_of_same_step
        if s.duration_in_min is not None
    ]
    sum_duration_in_min = sum(durations) if len(durations) > 0 else None
    all_passed = len([False for s in executions_of_same_step if not s.passed]) == 0
    exit_status_values = [
        s.exit_status for s in executions_of_same_step if s.exit_status is not None
    ]
    max_exit_status = max(exit_status_values) if len(exit_status_values) > 0 else None
    max_retry_count = max([s.retry_count for s in executions_of_same_step])
    count_shards = len(executions_of_same_step)
    web_url_without_job_id = any_execution.web_url
    if "#" in web_url_without_job_id:
        web_url_without_job_id = web_url_without_job_id[
            : web_url_without_job_id.index("#")
        ]

    return BuildStepOutcome(
        ids=ids,
        step_key=any_execution.step_key,
        parallel_job_index=None,
        build_number=any_execution.build_number,
        created_at=min_created_at,
        duration_in_min=sum_duration_in_min,
        passed=all_passed,
        exit_status=max_exit_status,
        retry_count=max_retry_count,
        web_url=web_url_without_job_id,
        count_items=count_shards,
    )
