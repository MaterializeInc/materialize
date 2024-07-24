# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any

from materialize.buildkite_insights.buildkite_api import artifacts_api
from materialize.buildkite_insights.cache import generic_cache
from materialize.buildkite_insights.cache.cache_constants import FetchMode
from materialize.buildkite_insights.cache.generic_cache import CacheFilePath
from materialize.util import (
    decompress_zst_to_directory,
    ensure_dir_exists,
    sha256_of_utf8_string,
)


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
    is_zst_compressed: bool,
) -> str:
    cache_file_path = _get_file_path_for_artifact(
        pipeline_slug=pipeline_slug, artifact_id=artifact_id
    )

    if not is_zst_compressed:
        action = lambda: artifacts_api.download_artifact(
            pipeline_slug=pipeline_slug,
            build_number=build_number,
            job_id=job_id,
            artifact_id=artifact_id,
        )
    else:

        def action() -> str:
            zst_file_path = _get_file_path_for_artifact(
                pipeline_slug=pipeline_slug,
                artifact_id=artifact_id,
                cache_item_type="compressed-artifact",
                file_extension="zst",
            )
            uncompress_directory_path = zst_file_path.get().replace(".zst", "")

            ensure_dir_exists(zst_file_path.get_path_to_directory())
            ensure_dir_exists(uncompress_directory_path)

            artifacts_api.download_artifact_to_file(
                pipeline_slug=pipeline_slug,
                build_number=build_number,
                job_id=job_id,
                artifact_id=artifact_id,
                file_path=zst_file_path.get(),
            )

            extracted_files = decompress_zst_to_directory(
                zst_file_path=zst_file_path.get(),
                destination_dir_path=uncompress_directory_path,
            )

            if len(extracted_files) != 1:
                raise RuntimeError(
                    f"Only archives with exactly one file supported at the moment. {zst_file_path.get()} contains {len(extracted_files)} files"
                )

            with open(extracted_files[0]) as file:
                return file.read()

    return generic_cache.get_or_query_data(
        cache_file_path,
        action,
        fetch_mode,
        max_allowed_cache_age_in_hours=96,
        quiet_mode=True,
    )


def _get_file_path_for_job_artifact_list(
    pipeline_slug: str,
    build_number: int,
    job_id: str,
) -> CacheFilePath:
    meta_data = f"{build_number}-{job_id}"
    hash_value = sha256_of_utf8_string(meta_data)[:8]
    return CacheFilePath(
        cache_item_type="build_job_artifact_list",
        pipeline_slug=pipeline_slug,
        params_hash=hash_value,
    )


def _get_file_path_for_artifact(
    pipeline_slug: str,
    artifact_id: str,
    cache_item_type: str = "artifact",
    file_extension: str = "json",
) -> CacheFilePath:
    # artifacts are text but also stored as string json
    return CacheFilePath(
        cache_item_type=cache_item_type,
        pipeline_slug=pipeline_slug,
        params_hash=artifact_id,
        file_extension=file_extension,
    )
