#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import hashlib
from typing import Any

from materialize.buildkite_insights.buildkite_api import builds_api
from materialize.buildkite_insights.cache import generic_cache
from materialize.buildkite_insights.cache.cache_constants import FetchMode
from materialize.buildkite_insights.util.data_io import (
    get_file_path,
)


def get_or_query_builds(
    pipeline_slug: str,
    fetch_mode: FetchMode,
    max_fetches: int,
    branch: str | None,
    build_states: list[str] | None,
    items_per_page: int = 50,
) -> list[Any]:
    meta_data = f"{branch}-{build_states}"
    cache_file_path = _get_file_path_for_builds(
        pipeline_slug=pipeline_slug,
        meta_data=meta_data,
        max_fetches=max_fetches,
        items_per_page=items_per_page,
    )

    fetch_action = lambda: builds_api.get_builds(
        pipeline_slug=pipeline_slug,
        max_fetches=max_fetches,
        branch=branch,
        build_states=build_states,
        items_per_page=items_per_page,
    )

    return generic_cache.get_or_query_data(cache_file_path, fetch_action, fetch_mode)


def get_or_query_builds_for_all_pipelines(
    fetch_mode: FetchMode,
    max_fetches: int,
    build_states: list[str] | None,
    items_per_page: int = 50,
) -> list[Any]:
    meta_data = f"{build_states}"
    cache_file_path = _get_file_path_for_builds(
        pipeline_slug="all",
        meta_data=meta_data,
        max_fetches=max_fetches,
        items_per_page=items_per_page,
    )

    fetch_action = lambda: builds_api.get_builds_of_all_pipelines(
        max_fetches=max_fetches,
        items_per_page=items_per_page,
        build_states=build_states,
    )

    return generic_cache.get_or_query_data(cache_file_path, fetch_action, fetch_mode)


def _get_file_path_for_builds(
    pipeline_slug: str,
    meta_data: str,
    max_fetches: int,
    items_per_page: int,
) -> str:
    max_entries = max_fetches * items_per_page
    meta_data = f"{meta_data}-{max_entries}"
    hash_value = hashlib.sha256(bytes(meta_data, encoding="utf-8")).hexdigest()[:8]
    return get_file_path(
        file_prefix="builds", pipeline_slug=pipeline_slug, params_hash=hash_value
    )
