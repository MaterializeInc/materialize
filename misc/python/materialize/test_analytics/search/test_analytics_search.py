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

from materialize.buildkite_insights.buildkite_api.buildkite_config import (
    MZ_PIPELINES_WITH_WILDCARD,
)
from materialize.test_analytics.search import test_analytics_search_logic
from materialize.test_analytics.search.test_analytics_search_source import (
    ANY_PIPELINE_VALUE,
    TestAnalyticsDataSource,
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="test-analytics-annotation-search",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--test-analytics-hostname", type=str, required=True)
    parser.add_argument("--test-analytics-username", type=str, required=True)
    parser.add_argument("--test-analytics-app-password", type=str, required=True)

    parser.add_argument(
        "pipeline",
        choices=MZ_PIPELINES_WITH_WILDCARD,
        type=str,
        help=f"Use {ANY_PIPELINE_VALUE} for all pipelines",
    )

    parser.add_argument("pattern", type=str, help="SQL LIKE pattern")

    parser.add_argument(
        "--branch",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--only-failed-builds",
        default=False,
        action="store_true",
    )
    parser.add_argument("--build-step-key", action="append", default=[], type=str)
    parser.add_argument("--not-newer-than-build-number", type=int)

    parser.add_argument("--max-results", default=50, type=int)

    parser.add_argument(
        "--short",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--oneline",
        default=False,
        action="store_true",
    )
    args = parser.parse_args()

    if args.short and args.oneline:
        print("Note: --oneline will be ignored if --short is set")

    if (
        args.not_newer_than_build_number is not None
        and args.pipeline == ANY_PIPELINE_VALUE
    ):
        print(
            "Combining a build number offset (--not-newer-than-build-number) and searching all pipelines does not make sense"
        )

    source = TestAnalyticsDataSource(
        test_analytics_hostname=args.test_analytics_hostname,
        test_analytics_username=args.test_analytics_username,
        test_analytics_app_password=args.test_analytics_app_password,
    )

    try:
        test_analytics_search_logic.start_search(
            search_source=source,
            pipeline_slug=args.pipeline,
            branch=args.branch,
            build_step_keys=args.build_step_key,
            only_failed_builds=args.only_failed_builds,
            not_newer_than_build_number=args.not_newer_than_build_number,
            like_pattern=args.pattern,
            max_results=args.max_results,
            short_result_presentation=args.short,
            one_line_match_presentation=args.oneline,
        )
    except Exception as e:
        print(f"Searching the test analytics database failed: {e}")
        print(
            "Please report the problem and consider using bin/buildkite-annotation-search in the mean time."
        )
