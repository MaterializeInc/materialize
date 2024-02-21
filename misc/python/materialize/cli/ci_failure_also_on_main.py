# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# ci_failure_also_on_main.py - Detect errors in log files during CI and find
# associated open GitHub issues in Materialize repository.

import os
import sys

from materialize.buildkite import add_annotation
from materialize.buildkite_insights.step_durations.build_step import (
    extract_build_step_data,
)
from materialize.buildkite_insights.util.buildkite_api import fetch_builds

NUMBER_OF_CONSIDERED_BUILDS = 3


def main() -> None:
    # This is only supposed to be invoked when the build step failed.

    pipeline_slug = os.getenv("BUILDKITE_PIPELINE_SLUG")
    step_key = os.getenv("BUILDKITE_STEP_KEY")
    step_name = os.getenv("BUILDKITE_LABEL") or step_key

    assert pipeline_slug is not None
    assert step_key is not None

    builds_data = fetch_builds(
        pipeline_slug=pipeline_slug,
        max_fetches=1,
        branch="main",
        build_state="finished",
        items_per_page=NUMBER_OF_CONSIDERED_BUILDS,
    )
    last_build_step_outcomes = extract_build_step_data(
        builds_data, selected_build_steps=[step_key]
    )

    if len(last_build_step_outcomes) == 0:
        print(f"Got no finished builds of pipeline {pipeline_slug} and step {step_key}")
        return
    else:
        print(
            f"Fetched {len(last_build_step_outcomes)} finished builds of pipeline {pipeline_slug} and step {step_key}"
        )

    last_execution_on_main_failed = not last_build_step_outcomes[0].passed
    failed_execution_count = len(
        [execution for execution in last_build_step_outcomes if not execution.passed]
    )
    total_execution_count = len(last_build_step_outcomes)

    if failed_execution_count == 0:
        print("None of the fetched builds failed")
        return

    if last_execution_on_main_failed:
        recent_failures_info = (
            f"This job also failed in the last build on main! "
            f"It failed in {failed_execution_count} of the last {total_execution_count} builds."
        )
    else:
        recent_failures_info = (
            f"This job passed in the last build on main. "
            f"However, it failed in {failed_execution_count} of the last {total_execution_count} builds."
        )

    add_annotation(
        style="info",
        title=f"{step_name}: Failed recently also on main",
        content=recent_failures_info,
    )


if __name__ == "__main__":
    sys.exit(main())
