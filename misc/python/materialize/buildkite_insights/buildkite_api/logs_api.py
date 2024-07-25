# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re

from materialize.buildkite_insights.buildkite_api import generic_api

BUILDKITE_TIMESTAMP_PATTERN = re.compile(r"bk;t=\d+")


def download_log(pipeline_slug: str, build_number: int, job_id: str) -> str:
    request_path = f"organizations/materialize/pipelines/{pipeline_slug}/builds/{build_number}/jobs/{job_id}/log"
    json = generic_api.get(request_path, {}, as_json=True)
    json_content = json.get("content", "")
    return _remove_timestamps(_remove_beeps(json_content))


def _remove_timestamps(json_content: str) -> str:
    return BUILDKITE_TIMESTAMP_PATTERN.sub("", json_content)


def _remove_beeps(json_content: str) -> str:
    return json_content.replace("\a", "")
