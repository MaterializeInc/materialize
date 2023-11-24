#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import os

import requests

BUILDKITE_API_URL = "https://api.buildkite.com/v2"


def main() -> None:
    headers = {}
    if token := os.getenv("BUILDKITE_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"
    url = f"{BUILDKITE_API_URL}/organizations/materialize/builds"
    params = {"include_retried_jobs": "true", "per_page": "100"}
    results = []

    print("Starting to fetch data from Buildkite...")

    while True:
        r = requests.get(headers=headers, url=url, params=params)
        result = r.json()
        if not result:
            print("No further results.")
            break

        params["created_to"] = result[-1]["created_at"]

        entry_count = len(result)
        created_at = result[-1]["created_at"]
        print(f"Fetched {entry_count} entries, created at {created_at}.")

        results.extend(result)

    with open("data.json", "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    main()
