# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pytest

from materialize.mz_version import MzVersion

# The Docker tags CI publishes images under, as `dev_docker_tag()` builds them:
# SemVer build metadata spelled with `--` because `+` is not a valid tag
# character.
DOCKER_TAGS = [
    "v26.43.0-dev.0--pr.g0123456789abcdef",
    "v26.42.0-rc.3--pr.g0123456789abcdef",
    "v26.42.0--pr.g0123456789abcdef",
    "v26.42.1--pr.g0123456789abcdef",
]


@pytest.mark.parametrize("tag", DOCKER_TAGS)
def test_docker_tag_round_trips(tag: str) -> None:
    assert str(MzVersion.parse_mz(tag)) == tag


@pytest.mark.parametrize("tag", DOCKER_TAGS)
def test_docker_tag_suffix_is_build_metadata(tag: str) -> None:
    version = MzVersion.parse_mz(tag)
    assert version.build == "pr.g0123456789abcdef"
    assert version.prerelease is None or "--" not in version.prerelease


@pytest.mark.parametrize(
    "tag, meets_gate",
    [
        # A -dev.0 gate is what the operator uses for a flag that ships in
        # 26.42, so everything built from the 26.42 line must clear it, and
        # nothing released before it may.
        ("v26.43.0-dev.0--pr.g0123456789abcdef", True),
        ("v26.42.0-rc.3--pr.g0123456789abcdef", True),
        ("v26.42.0--pr.g0123456789abcdef", True),
        ("v26.42.1--pr.g0123456789abcdef", True),
        ("v26.42.0-dev.0", True),
        ("v26.41.0", False),
        ("v26.40.2", False),
    ],
)
def test_docker_tag_against_dev_gate(tag: str, meets_gate: bool) -> None:
    gate = MzVersion.parse_mz("v26.42.0-dev.0")
    assert (MzVersion.parse_mz(tag) >= gate) is meets_gate


def test_release_sorts_above_its_prereleases() -> None:
    versions = sorted(
        MzVersion.parse_mz(tag)
        for tag in [
            "v26.42.0--pr.g0123456789abcdef",
            "v26.42.0-rc.3--pr.g0123456789abcdef",
            "v26.42.0-dev.0",
            "v26.41.0",
        ]
    )
    assert [str(v) for v in versions] == [
        "v26.41.0",
        "v26.42.0-dev.0",
        "v26.42.0-rc.3--pr.g0123456789abcdef",
        "v26.42.0--pr.g0123456789abcdef",
    ]


def test_drop_dev_suffix_ignores_build_metadata() -> None:
    version = MzVersion.parse_mz(
        "v26.43.0-dev.0--pr.g0123456789abcdef", drop_dev_suffix=True
    )
    assert str(version) == "v26.43.0"


def test_dev_version_with_hash() -> None:
    version = MzVersion.parse_mz("v0.45.0-dev (f01773cb1)")
    assert version.is_dev_version()
    assert str(version) == "v0.45.0-dev"
