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


def buildkite_get_request(
    request_path: str, params: dict[str, str], max_fetches: int | None
) -> list[Any]:
    headers = {}
    if token := os.getenv("BUILDKITE_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"

    url = f"{BUILDKITE_API_URL}/{request_path}"
    results = []

    print(f"Starting to fetch data from Buildkite: {url}")

    fetch_count = 0
    while True:
        r = requests.get(headers=headers, url=url, params=params)
        result = r.json()
        fetch_count += 1

        if not result:
            print("No further results.")
            break
        if max_fetches is not None and fetch_count >= max_fetches:
            print("Max fetches reached.")
            break

        if isinstance(result, dict) and result.get("message"):
            raise RuntimeError(f"Something went wrong! ({result['message']})")

        params["created_to"] = result[-1]["created_at"]

        entry_count = len(result)
        created_at = result[-1]["created_at"]
        print(f"Fetched {entry_count} entries, created at {created_at}.")

        results.extend(result)

    return results
