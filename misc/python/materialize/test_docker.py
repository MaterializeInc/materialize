# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pytest

from materialize.docker import is_image_tag_of_release_version
from materialize.mzcompose.services.materialized import Materialized


@pytest.mark.parametrize(
    "tag, release",
    [
        ("v26.44.0", True),
        ("v26.44.0-dev.0", True),
        ("v26.44.0-dev.0--pr.g0123456789abcdef", False),
        ("latest", False),
        ("mzbuild-VUQ4U5N5RVFPTEQCIWZTJBYICI2TZCMX", False),
    ],
)
def test_image_tag_classification(tag: str, release: bool) -> None:
    assert is_image_tag_of_release_version(tag) is release


def test_materialized_accepts_mzbuild_image() -> None:
    Materialized(
        image="materialize/materialized:mzbuild-VUQ4U5N5RVFPTEQCIWZTJBYICI2TZCMX"
    )
