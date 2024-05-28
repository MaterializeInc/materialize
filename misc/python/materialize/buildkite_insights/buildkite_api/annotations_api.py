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


def get_annotations(
    pipeline_slug: str,
    build_number: str,
) -> list[Any]:
    request_path = f"organizations/materialize/pipelines/{pipeline_slug}/builds/{build_number}/annotations"
    return generic_api.get(request_path, {})
