#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any

from materialize.buildkite_insights.buildkite_api import generic_api


def get_builds(
    pipeline_slug: str,
    max_fetches: int | None,
    branch: str | None,
    build_states: list[str] | None,
    items_per_page: int = 100,
    include_retries: bool = True,
) -> list[Any]:
    request_path = f"organizations/materialize/pipelines/{pipeline_slug}/builds"
    params: dict[str, Any] = {
        "include_retried_jobs": str(include_retries).lower(),
        "per_page": str(items_per_page),
    }

    if branch is not None:
        params["branch"] = branch

    if build_states is not None and len(build_states) > 0:
        params["state[]"] = build_states

    return generic_api.get_multiple(request_path, params, max_fetches=max_fetches)


def get_builds_of_all_pipelines(
    max_fetches: int | None,
    items_per_page: int = 100,
    include_retries: bool = True,
) -> list[Any]:
    params = {
        "include_retried_jobs": str(include_retries).lower(),
        "per_page": str(items_per_page),
    }

    return generic_api.get_multiple(
        "organizations/materialize/builds",
        params,
        max_fetches=max_fetches,
    )
