#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.buildkite_insights.buildkite_api import builds_api
from materialize.buildkite_insights.util.data_io import (
    SimpleFilePath,
    write_results_to_file,
)


def main() -> None:
    result = builds_api.get_builds_of_all_pipelines(max_fetches=None, branch=None)
    write_results_to_file(result, SimpleFilePath("data.json"))


if __name__ == "__main__":
    main()
