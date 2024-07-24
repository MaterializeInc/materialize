# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.buildkite_insights.buildkite_api import logs_api
from materialize.buildkite_insights.cache import generic_cache
from materialize.buildkite_insights.cache.cache_constants import FetchMode
from materialize.buildkite_insights.cache.generic_cache import CacheFilePath


def get_or_download_log(
    pipeline_slug: str,
    fetch_mode: FetchMode,
    build_number: int,
    job_id: str,
) -> str:
    cache_file_path = _get_file_path_for_log(pipeline_slug=pipeline_slug, job_id=job_id)

    action = lambda: logs_api.download_log(
        pipeline_slug=pipeline_slug,
        build_number=build_number,
        job_id=job_id,
    )

    return generic_cache.get_or_query_data(
        cache_file_path,
        action,
        fetch_mode,
        max_allowed_cache_age_in_hours=96,
        quiet_mode=True,
    )


def _get_file_path_for_log(
    pipeline_slug: str,
    job_id: str,
) -> CacheFilePath:
    return CacheFilePath(
        cache_item_type="log",
        pipeline_slug=pipeline_slug,
        params_hash=job_id,
        file_extension="txt",
    )
