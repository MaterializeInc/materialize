# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any

from materialize.buildkite_insights.buildkite_api import generic_api


def get_build_job_artifact_list(
    pipeline_slug: str, build_number: int, job_id: str
) -> list[Any]:
    request_path = f"organizations/materialize/pipelines/{pipeline_slug}/builds/{build_number}/jobs/{job_id}/artifacts"
    return generic_api.get(request_path, {})


def download_artifact(
    pipeline_slug: str, build_number: int, job_id: str, artifact_id: str
) -> str:
    request_path = f"organizations/materialize/pipelines/{pipeline_slug}/builds/{build_number}/jobs/{job_id}/artifacts/{artifact_id}/download"
    return generic_api.get(request_path, {}, as_json=False)


def download_artifact_to_file(
    pipeline_slug: str, build_number: int, job_id: str, artifact_id: str, file_path: str
) -> str:
    request_path = f"organizations/materialize/pipelines/{pipeline_slug}/builds/{build_number}/jobs/{job_id}/artifacts/{artifact_id}/download"
    return generic_api.get_and_download_to_file(request_path, {}, file_path)
