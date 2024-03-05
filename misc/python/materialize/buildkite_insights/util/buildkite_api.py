#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
from typing import Any

import requests

BUILDKITE_API_URL = "https://api.buildkite.com/v2"


def fetch(request_path: str, params: dict[str, str]) -> Any:
    headers = {}
    token = os.getenv("BUILDKITE_CI_API_KEY") or os.getenv("BUILDKITE_TOKEN")

    if token is not None and len(token) > 0:
        headers["Authorization"] = f"Bearer {token}"
    else:
        print("Authentication token is not specified or empty!")

    url = f"{BUILDKITE_API_URL}/{request_path}"
    r = requests.get(headers=headers, url=url, params=params)
    return r.json()


def get(
    request_path: str, params: dict[str, str], max_fetches: int | None
) -> list[Any]:
    results = []

    print(f"Starting to fetch data from Buildkite: {request_path}")

    fetch_count = 0
    while True:
        result = fetch(request_path, params)
        fetch_count += 1

        if not result:
            print("No further results.")
            break

        if isinstance(result, dict) and result.get("message"):
            raise RuntimeError(f"Something went wrong! ({result['message']})")

        params["page"] = str(int(params.get("page", "1")) + 1)

        entry_count = len(result)
        created_at = result[-1]["created_at"]
        print(f"Fetched {entry_count} entries, created at {created_at}.")

        results.extend(result)

        if max_fetches is not None and fetch_count >= max_fetches:
            print("Max fetches reached.")
            break

    return results


def fetch_builds(
    pipeline_slug: str,
    max_fetches: int | None,
    branch: str | None,
    build_state: str | None,
    items_per_page: int = 100,
    include_retries: bool = True,
) -> list[Any]:
    request_path = f"organizations/materialize/pipelines/{pipeline_slug}/builds"
    params = {
        "include_retried_jobs": str(include_retries).lower(),
        "per_page": str(items_per_page),
    }

    if branch is not None:
        params["branch"] = branch

    if build_state is not None:
        params["state"] = build_state

    return get(request_path, params, max_fetches=max_fetches)
