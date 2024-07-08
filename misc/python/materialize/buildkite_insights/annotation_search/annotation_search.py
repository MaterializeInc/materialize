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
from materialize.buildkite_insights.annotation_search.buildkite_search_source import (
    ANY_PIPELINE_VALUE,
    BuildkiteDataSource,
)
from materialize.buildkite_insights.buildkite_api.buildkite_config import (
    MZ_PIPELINES_WITH_WILDCARD,
)
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
        choices=MZ_PIPELINES_WITH_WILDCARD,
        type=str,
        help=f"Use {ANY_PIPELINE_VALUE} for all pipelines",
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

    source = BuildkiteDataSource(
        fetch_builds_mode=args.fetch_builds,
        fetch_annotations_mode=args.fetch_annotations,
        max_build_fetches=args.max_build_fetches,
        first_build_page_to_fetch=args.first_build_page_to_fetch,
        only_failed_builds=args.only_failed_builds,
        only_failed_build_step_keys=args.only_failed_build_step_key,
    )

    annotation_search_logic.start_search(
        args.pipeline,
        args.branch,
        source,
        args.max_results,
        args.only_one_result_per_build,
        args.pattern,
        args.use_regex,
        args.short,
        args.oneline,
        args.verbose,
    )
