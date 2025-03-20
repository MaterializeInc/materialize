#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time

from materialize.buildkite_insights.buildkite_api import builds_api, generic_api


def main() -> None:
    # Used to find recent instances of https://github.com/MaterializeInc/database-issues/issues/7338
    # 2 weeks ~ 2000 builds
    data = builds_api.get_builds_of_all_pipelines(max_fetches=20, branch=None)

    for build in data:
        request_path = f"organizations/materialize/pipelines/{build['pipeline']['slug']}/builds/{build['number']}/artifacts"
        params = {"per_page": "100"}
        result = generic_api.get_multiple(request_path, params, max_fetches=None)
        for artifact in result:
            # Some core files are corrupted, probably because they get dumped during shutdown, ignore them
            if (
                "core" in artifact["filename"]
                and build["pipeline"]["slug"] != "coverage"
                and artifact["file_size"] > 100000
            ):
                print(
                    f"{build['started_at']}: {artifact['filename']} in https://buildkite.com/materialize/{build['pipeline']['slug']}/builds/{build['number']}#{artifact['job_id']}"
                )
        time.sleep(2)


if __name__ == "__main__":
    main()
