# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from datetime import datetime
from typing import Any

from materialize import buildkite
from materialize.buildkite_insights.buildkite_api.buildkite_constants import (
    BUILDKITE_COMPLETED_BUILD_STEP_STATES,
)
from materialize.buildkite_insights.data.build_step import (
    BuildJobOutcome,
    BuildStepMatcher,
    BuildStepOutcome,
)


def extract_build_step_outcomes(
    builds_data: list[Any],
    selected_build_steps: list[BuildStepMatcher],
    build_step_states: list[str],
) -> list[BuildStepOutcome]:
    result = []
    for build in builds_data:
        step_infos = _extract_build_step_data_from_build(
            build, selected_build_steps, build_step_states
        )
        result.extend(step_infos)

    return result


def _extract_build_step_data_from_build(
    build_data: Any,
    selected_build_steps: list[BuildStepMatcher],
    build_step_states: list[str],
) -> list[BuildStepOutcome]:
    collected_steps = []

    for job in build_data["jobs"]:
        if not job.get("step_key"):
            continue

        if not _shall_include_build_step(job, selected_build_steps):
            continue

        build_job_state = job["state"]

        if len(build_step_states) > 0 and build_job_state not in build_step_states:
            continue

        id = build_data["id"]
        build_number = build_data["number"]
        commit_hash = build_data["commit"]
        created_at = datetime.fromisoformat(job["created_at"])
        build_step_key = job["step_key"]
        parallel_job_index = job.get("parallel_group_index")

        if job.get("started_at") and job.get("finished_at"):
            started_at = datetime.fromisoformat(job["started_at"])
            finished_at = datetime.fromisoformat(job["finished_at"])
            duration_in_min = (finished_at - started_at).total_seconds() / 60
        else:
            duration_in_min = None

        job_passed = build_job_state == "passed"
        job_completed = build_job_state in BUILDKITE_COMPLETED_BUILD_STEP_STATES
        exit_status = job.get("exit_status")
        retry_count = job.get("retries_count") or 0

        assert (
            not job_passed or duration_in_min is not None
        ), "Duration must be available for passed step"

        step_data = BuildStepOutcome(
            id=id,
            step_key=build_step_key,
            parallel_job_index=parallel_job_index,
            build_number=build_number,
            commit_hash=commit_hash,
            created_at=created_at,
            duration_in_min=duration_in_min,
            passed=job_passed,
            completed=job_completed,
            exit_status=exit_status,
            retry_count=retry_count,
            web_url_to_job=buildkite.get_job_url_from_build_url(
                build_data["web_url"], job["id"]
            ),
        )

        if retry_count == 0:
            collected_steps.append(step_data)
        else:
            # latest retry before other retries and original execution
            insertion_index = find_index_of_first_step_instance(
                collected_steps, build_number, build_step_key, parallel_job_index
            )
            collected_steps.insert(insertion_index, step_data)

    return collected_steps


def find_index_of_first_step_instance(
    steps: list[BuildStepOutcome],
    build_number: int,
    build_step_key: str,
    parallel_job_index: int | None,
) -> int:
    index = len(steps)

    while index > 0:
        prev_index = index - 1
        prev_step = steps[prev_index]

        if (
            prev_step.build_number == build_number
            and prev_step.step_key == build_step_key
            and prev_step.parallel_job_index == parallel_job_index
        ):
            index = prev_index
        else:
            break

    return index


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


def step_outcomes_to_job_outcomes(
    step_infos: list[BuildStepOutcome],
) -> list[BuildJobOutcome]:
    """
    This merges sharded executions of the same build and step.
    This may still produce multiple entries per step key in case of retries.
    """
    outcomes_by_build_and_step_key_and_retry: dict[str, list[BuildStepOutcome]] = dict()

    for step_info in step_infos:
        group_key = (
            f"{step_info.build_number}.{step_info.step_key}.{step_info.retry_count}"
        )
        outcomes_to_merge = (
            outcomes_by_build_and_step_key_and_retry.get(group_key) or []
        )
        outcomes_to_merge.append(step_info)
        outcomes_by_build_and_step_key_and_retry[group_key] = outcomes_to_merge

    result = []

    for _, outcomes_of_same_step in outcomes_by_build_and_step_key_and_retry.items():
        result.append(_step_outcomes_to_job_outcome(outcomes_of_same_step))

    return result


def _step_outcomes_to_job_outcome(
    outcomes_of_same_step: list[BuildStepOutcome],
) -> BuildJobOutcome:
    any_execution = outcomes_of_same_step[0]

    for outcome in outcomes_of_same_step:
        assert outcome.build_number == any_execution.build_number
        assert outcome.step_key == any_execution.step_key

    ids = [s.id for s in outcomes_of_same_step]
    min_created_at = min([s.created_at for s in outcomes_of_same_step])
    durations = [
        s.duration_in_min
        for s in outcomes_of_same_step
        if s.duration_in_min is not None
    ]
    sum_duration_in_min = sum(durations) if len(durations) > 0 else None
    all_passed = len([1 for s in outcomes_of_same_step if not s.passed]) == 0
    all_completed = len([1 for s in outcomes_of_same_step if not s.completed]) == 0
    max_retry_count = any_execution.retry_count
    count_shards = len(outcomes_of_same_step)
    web_url_without_job_id = any_execution.web_url_to_build()

    return BuildJobOutcome(
        ids=ids,
        step_key=any_execution.step_key,
        build_number=any_execution.build_number,
        commit_hash=any_execution.commit_hash,
        created_at=min_created_at,
        duration_in_min=sum_duration_in_min,
        passed=all_passed,
        completed=all_completed,
        retry_count=max_retry_count,
        web_url_to_build=web_url_without_job_id,
        count_items=count_shards,
    )


def extract_build_step_names_by_job_id(
    build_data: Any,
) -> dict[str, str]:
    return _extract_build_step_infos_by_job_id(build_data, "name")


def extract_build_steps_by_job_id(
    build_data: Any,
) -> dict[str, str]:
    return _extract_build_step_infos_by_job_id(build_data, "step_key")


def _extract_build_step_infos_by_job_id(
    build_data: Any, field_name: str
) -> dict[str, str]:
    build_job_info_by_job_id: dict[str, str] = dict()

    for job in build_data["jobs"]:
        build_job_id = job["id"]
        build_job_info = job.get(field_name, None)

        if build_job_info is not None:
            build_job_info_by_job_id[build_job_id] = build_job_info

    return build_job_info_by_job_id
