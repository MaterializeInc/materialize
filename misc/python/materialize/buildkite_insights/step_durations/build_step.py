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
    id: str
    step_key: str
    parallel_job: int | None
    build_number: int
    created_at: datetime
    duration_in_min: float | None
    passed: bool
    exit_status: int | None
    retry_count: int
    web_url: str


@dataclass
class BuildStep:
    step_key: str
    parallel_job: int | None


def extract_build_step_data(
    builds_data: list[Any],
    selected_build_steps: list[BuildStep],
) -> list[BuildStepOutcome]:
    result = []
    for build in builds_data:
        step_infos = _extract_build_step_data_from_build(build, selected_build_steps)
        result.extend(step_infos)

    return result


def _extract_build_step_data_from_build(
    build_data: Any, selected_build_steps: list[BuildStep]
) -> list[BuildStepOutcome]:
    collected_steps = []

    for job in build_data["jobs"]:
        if not job.get("step_key"):
            continue

        if (
            len(selected_build_steps) > 0
            and BuildStep(job["step_key"], job.get("parallel_group_index"))
            not in selected_build_steps
        ):
            continue

        if job["state"] in ["canceled", "running"]:
            continue

        id = build_data["id"]
        build_number = build_data["number"]
        created_at = datetime.fromisoformat(job["created_at"])
        build_step_key = job["step_key"]

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
            id,
            build_step_key,
            None,  # TODO: The parallel_job number
            build_number,
            created_at,
            duration_in_min,
            job_passed,
            exit_status=exit_status,
            retry_count=retry_count,
            web_url=f"{build_data['web_url']}#{job['id']}",
        )
        collected_steps.append(step_data)

    return collected_steps
