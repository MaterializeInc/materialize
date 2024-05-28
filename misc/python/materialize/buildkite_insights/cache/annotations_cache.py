# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Any

from materialize.buildkite_insights.buildkite_api import annotations_api
from materialize.buildkite_insights.cache import generic_cache
from materialize.buildkite_insights.cache.cache_constants import FetchMode
from materialize.buildkite_insights.cache.generic_cache import CacheFilePath


def get_or_query_annotations(
    fetch_mode: FetchMode,
    pipeline_slug: str,
    build_number: str,
    add_to_cache_if_not_present: bool,
    quiet_mode: bool = True,
) -> list[Any]:
    cache_file_path = CacheFilePath(
        cache_item_type="annotations",
        pipeline_slug=pipeline_slug,
        params_hash=build_number,
    )

    fetch_action = lambda: annotations_api.get_annotations(
        pipeline_slug=pipeline_slug,
        build_number=build_number,
    )

    return generic_cache.get_or_query_data(
        cache_file_path,
        fetch_action,
        fetch_mode,
        max_allowed_cache_age_in_hours=None,
        add_to_cache_if_not_present=add_to_cache_if_not_present,
        quiet_mode=quiet_mode,
    )
