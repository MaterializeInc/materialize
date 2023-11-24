#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
from typing import Any

from materialize.buildkite_insights.util.io import (
    read_results_from_file,
    write_results_to_file,
)
from materialize.buildkite_insights.util.web_request import buildkite_get_request


def get_file_name(pipeline_slug: str) -> str:
    return f"{pipeline_slug}_builds.json"


def get_data(pipeline_slug: str, no_fetch: bool, max_fetches: int | None) -> list[Any]:
    if no_fetch:
        return read_results_from_file(get_file_name(pipeline_slug))

    request_path = "organizations/materialize/pipelines/{pipeline_slug}/builds"
    params = {"branch": "main", "include_retried_jobs": "true", "per_page": "100"}

    result = buildkite_get_request(request_path, params, max_fetches=max_fetches)
    write_results_to_file(result, get_file_name(pipeline_slug))
    return result


def analyze_data(data: list[Any]) -> None:
    pass


def main(pipeline_slug: str, no_fetch: bool, max_fetches: int | None) -> None:
    data = get_data(pipeline_slug, no_fetch, max_fetches)
    analyze_data(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="buildkite-step-durations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--pipeline", default="tests", type=str)
    parser.add_argument("--build-step", default=None, type=str)
    parser.add_argument("--no-fetch", default=False, type=bool)
    parser.add_argument("--max-fetches", default=5, type=int)
    args = parser.parse_args()

    main(args.pipeline, args.no_fetch, args.max_fetches)
