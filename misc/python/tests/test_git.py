# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pytest  # type: ignore

from semver import VersionInfo

from materialize import git


def test_versioninfo() -> None:
    # just make sure upgrades don't break our most important invariants
    assert VersionInfo.parse("0.20.0") > VersionInfo.parse("0.3.50")
    assert VersionInfo.parse("1.0.0").prerelease is None
    assert VersionInfo.parse("1.0.0-rc1").prerelease is not None


def test_tags_returns_ordered_newest_first() -> None:
    tags = git.get_version_tags(fetch=False)
    # use a couple hard coded tags for extra sanity
    current_latest = VersionInfo.parse("0.3.1")
    older = VersionInfo.parse("0.2.0")
    seen_latest = False
    prev = tags[0]
    for tag in tags:
        # ensure that actual versions are correctly ordered
        assert (prev.major, prev.minor, prev.patch) >= (
            tag.major,
            tag.minor,
            tag.patch,
        )
        if prev.prerelease and tag.prerelease:
            assert prev.prerelease >= tag.prerelease
        prev = tag

        if tag == current_latest:
            seen_latest = True
        elif tag == older:
            assert seen_latest, "We should see newer tags first"
