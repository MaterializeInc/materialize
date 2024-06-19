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

from materialize.buildkite_insights.annotation_search import annotation_search_logic
from materialize.buildkite_insights.buildkite_api.buildkite_config import MZ_PIPELINES
from materialize.buildkite_insights.cache.cache_constants import (
    FETCH_MODE_CHOICES,
    FetchMode,
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="buildkite-annotation-search",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "pipeline",
        choices=MZ_PIPELINES,
        type=str,
        help="Use * for all pipelines",
    )

    parser.add_argument("pattern", type=str)

    parser.add_argument(
        "--branch",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--fetch-builds",
        type=lambda mode: FetchMode[mode.upper()],
        choices=FETCH_MODE_CHOICES,
        default=FetchMode.AUTO,
        help="Whether to fetch fresh builds from Buildkite.",
    )
    parser.add_argument(
        "--fetch-annotations",
        type=lambda mode: FetchMode[mode.upper()],
        choices=FETCH_MODE_CHOICES,
        default=FetchMode.AUTO,
        help="Whether to fetch fresh annotations from Buildkite.",
    )
    parser.add_argument("--max-build-fetches", default=2, type=int)
    parser.add_argument("--first-build-page-to-fetch", default=1, type=int)
    parser.add_argument("--max-results", default=50, type=int)
    parser.add_argument(
        "--only-one-result-per-build",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--only-failed-builds",
        default=False,
        action="store_true",
    )
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
    parser.add_argument(
        "--only-failed-build-step-key", action="append", default=[], type=str
    )
    parser.add_argument(
        "--verbose",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--use-regex",
        action="store_true",
    )
    args = parser.parse_args()

    if args.short and args.oneline:
        print("Note: --oneline will be ignored if --short is set")

    annotation_search_logic.start_search(
        args.pipeline,
        args.branch,
        args.fetch_builds,
        args.fetch_annotations,
        args.max_build_fetches,
        args.first_build_page_to_fetch,
        args.max_results,
        args.only_one_result_per_build,
        args.only_failed_builds,
        args.only_failed_build_step_key,
        args.pattern,
        args.use_regex,
        args.short,
        args.oneline,
        args.verbose,
    )
