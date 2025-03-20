# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re
from datetime import datetime
from typing import Any

from materialize.buildkite_insights.buildkite_api.buildkite_constants import (
    BUILDKITE_COMPLETED_BUILD_STATES,
    BUILDKITE_FAILED_BUILD_STATES,
    BUILDKITE_RELEVANT_FAILED_BUILD_STEP_STATES,
)
from materialize.buildkite_insights.cache import annotations_cache, builds_cache
from materialize.buildkite_insights.cache.cache_constants import FetchMode
from materialize.buildkite_insights.data.build_annotation import BuildAnnotation
from materialize.buildkite_insights.data.build_info import Build
from materialize.buildkite_insights.data.build_step import BuildStepMatcher
from materialize.buildkite_insights.util.build_step_utils import (
    extract_build_step_outcomes,
)

ANY_PIPELINE_VALUE = "*"
ANY_BRANCH_VALUE = "*"


class BuildkiteDataSource:

    def __init__(
        self,
        fetch_builds_mode: FetchMode,
        fetch_annotations_mode: FetchMode,
        max_build_fetches: int,
        first_build_page_to_fetch: int,
        only_failed_builds: bool,
        only_failed_build_step_keys: list[str],
    ):
        self.fetch_builds_mode = fetch_builds_mode
        self.fetch_annotations_mode = fetch_annotations_mode
        self.max_build_fetches = max_build_fetches
        self.first_build_page_to_fetch = first_build_page_to_fetch
        self.only_failed_builds = only_failed_builds
        self.only_failed_build_step_keys = only_failed_build_step_keys

    def fetch_builds(self, pipeline: str, branch: str | None) -> list[Build]:
        if self.only_failed_builds:
            build_states = BUILDKITE_FAILED_BUILD_STATES
        else:
            build_states = []

        # do not try to continue with incomplete data in case of an exceeded rate limit because fetching the annotations
        # will anyway most likely fail
        if pipeline == ANY_PIPELINE_VALUE:
            raw_builds = builds_cache.get_or_query_builds_for_all_pipelines(
                self.fetch_builds_mode,
                self.max_build_fetches,
                branch=branch,
                build_states=build_states,
                first_page=self.first_build_page_to_fetch,
            )
        else:
            raw_builds = builds_cache.get_or_query_builds(
                pipeline,
                self.fetch_builds_mode,
                self.max_build_fetches,
                branch=branch,
                build_states=build_states,
                first_page=self.first_build_page_to_fetch,
            )

        raw_builds = self.filter_builds(raw_builds, self.only_failed_build_step_keys)

        builds = []

        for build in raw_builds:
            build = Build(
                number=build["number"],
                pipeline=build["pipeline"]["slug"],
                state=build["state"],
                branch=build["branch"],
                web_url=build["web_url"],
                created_at=datetime.strptime(
                    build["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
                ).replace(microsecond=0),
            )
            builds.append(build)

        return builds

    def filter_builds(
        self,
        builds_data: list[Any],
        only_failed_build_step_keys: list[str],
    ) -> list[Any]:
        if len(only_failed_build_step_keys) == 0:
            return builds_data

        failed_build_step_matcher = [
            BuildStepMatcher(build_step_key, None)
            for build_step_key in only_failed_build_step_keys
        ]

        step_outcomes = extract_build_step_outcomes(
            builds_data=builds_data,
            selected_build_steps=failed_build_step_matcher,
            build_step_states=BUILDKITE_RELEVANT_FAILED_BUILD_STEP_STATES,
        )

        builds_containing_failed_step_keys = {
            outcome.build_number for outcome in step_outcomes
        }

        filtered_builds = [
            build
            for build in builds_data
            if int(build["number"]) in builds_containing_failed_step_keys
        ]
        return filtered_builds

    def fetch_annotations(
        self, build: Build, verbose: bool = False
    ) -> list[BuildAnnotation]:
        is_completed_build_state = build.state in BUILDKITE_COMPLETED_BUILD_STATES

        raw_annotations = annotations_cache.get_or_query_annotations(
            fetch_mode=self.fetch_annotations_mode,
            pipeline_slug=build.pipeline,
            build_number=build.number,
            add_to_cache_if_not_present=is_completed_build_state,
            quiet_mode=not verbose,
        )

        result = []
        for raw_annotation in raw_annotations:
            annotation_html = raw_annotation["body_html"]
            annotation_text = self.clean_annotation_text(annotation_html)
            annotation_title = self.try_extracting_title_from_annotation_html(
                annotation_html
            )

            result.append(
                BuildAnnotation(content=annotation_text, title=annotation_title)
            )

        return result

    def clean_annotation_text(self, annotation_html: str) -> str:
        return re.sub(r"<[^>]+>", "", annotation_html)

    def try_extracting_title_from_annotation_html(
        self, annotation_html: str
    ) -> str | None:
        # match <p>...</p> header
        header_paragraph_match = re.search("<p>(.*?)</p>", annotation_html)

        if header_paragraph_match is None:
            return None

        header_paragraph = header_paragraph_match.group(1)

        build_step_name_match = re.search(
            "<a href=.*?>(.*?)</a> (failed|succeeded)", header_paragraph
        )

        if build_step_name_match:
            return build_step_name_match.group(1)
        else:
            return self.clean_annotation_text(header_paragraph)
