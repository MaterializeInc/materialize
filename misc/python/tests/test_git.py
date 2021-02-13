# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from unittest.mock import patch
import pytest  # type: ignore

from semver import VersionInfo

from materialize import git


def test_versioninfo() -> None:
    # just make sure upgrades don't break our most important invariants
    assert VersionInfo.parse("0.20.0") > VersionInfo.parse("0.3.50")
    assert VersionInfo.parse("1.0.0").prerelease is None
    assert VersionInfo.parse("1.0.0-rc1").prerelease is not None


def test_tags_returns_ordered_newest_first() -> None:
    with patch("materialize.spawn.capture") as mock:
        # NOTE(benesch): we mock out the return value of `git tag` because local
        # Git clones are missing tags weirdly often. I'm not sure why.
        # Regardless, it's better for unit tests not to depend on the local Git
        # repository state.
        mock.return_value = """0.2.0
0.3.2-rc1
0.3.1"""
        tags = git.get_version_tags(fetch=False)
    assert tags == [
        VersionInfo.parse("0.3.2-rc1"),
        VersionInfo.parse("0.3.1"),
        VersionInfo.parse("0.2.0"),
    ]
