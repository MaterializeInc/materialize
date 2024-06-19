# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.buildkite_insights.data.build_annotation import BuildAnnotation
from materialize.buildkite_insights.data.build_info import Build


class AnnotationSearchSource:
    def fetch_builds(
        self, pipeline: str, branch: str, verbose: bool = False
    ) -> list[Build]:
        raise NotImplementedError

    def fetch_annotations(
        self, build: Build, verbose: bool = False
    ) -> list[BuildAnnotation]:
        raise NotImplementedError
