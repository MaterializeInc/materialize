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

from materialize.buildkite_insights.buildkite_api import artifacts_api
from materialize.buildkite_insights.cache import generic_cache
from materialize.buildkite_insights.cache.cache_constants import FetchMode
from materialize.buildkite_insights.util.data_io import get_file_path


def get_or_query_job_artifact_list(
    pipeline_slug: str,
    fetch_mode: FetchMode,
    build_number: int,
    job_id: str,
) -> list[Any]:
    cache_file_path = _get_file_path_for_job_artifact_list(
        pipeline_slug=pipeline_slug, build_number=build_number, job_id=job_id
    )

    fetch_action = lambda: artifacts_api.get_build_job_artifact_list(
        pipeline_slug=pipeline_slug,
        build_number=build_number,
        job_id=job_id,
    )

    return generic_cache.get_or_query_data(cache_file_path, fetch_action, fetch_mode)


def get_or_download_artifact(
    pipeline_slug: str,
    fetch_mode: FetchMode,
    build_number: int,
    job_id: str,
    artifact_id: str,
) -> str:
    cache_file_path = _get_file_path_for_artifact(
        pipeline_slug=pipeline_slug, artifact_id=artifact_id
    )

    download_action = lambda: artifacts_api.download_artifact(
        pipeline_slug=pipeline_slug,
        build_number=build_number,
        job_id=job_id,
        artifact_id=artifact_id,
    )

    return generic_cache.get_or_query_data(
        cache_file_path, download_action, fetch_mode, quiet_mode=True
    )


def _get_file_path_for_job_artifact_list(
    pipeline_slug: str,
    build_number: int,
    job_id: str,
) -> str:
    meta_data = f"{build_number}-{job_id}"
    hash_value = hashlib.sha256(bytes(meta_data, encoding="utf-8")).hexdigest()[:8]
    return get_file_path(
        file_prefix="build_job_artifact_list",
        pipeline_slug=pipeline_slug,
        params_hash=hash_value,
    )


def _get_file_path_for_artifact(
    pipeline_slug: str,
    artifact_id: str,
) -> str:
    # artifacts are text but also stored as string json
    return get_file_path(
        file_prefix="artifact",
        pipeline_slug=pipeline_slug,
        params_hash=artifact_id,
    )
