# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pytest  # type: ignore

from materialize import git


def test_tag() -> None:
    assert git.Tag.from_str("v0.3.2-rc1") == git.Tag(0, 3, 2, "rc1")
    assert git.Tag.from_str("v0.3.2") == git.Tag(0, 3, 2, None)

    assert git.Tag.from_str("v0.20.0") > git.Tag.from_str("v0.3.50")

    with pytest.raises(ValueError):
        git.Tag.from_str("v0.3")

    assert git.Tag.from_str("v0.3.1") > git.Tag.from_str("v0.2.0")
    assert git.Tag.from_str("v0.3.1") > git.Tag.from_str("v0.2.0-rc1")

    assert git.Tag.from_str("v0.3.1-rc1") < git.Tag.from_str("v0.3.1")
    assert git.Tag.from_str("v0.3.1") > git.Tag.from_str("v0.3.1-rc1")

    assert git.Tag.from_str("v0.3.2") > git.Tag.from_str("v0.3.1-rc1")
    assert git.Tag.from_str("v0.3.2") != git.Tag.from_str("v0.3.2-rc1")
    assert git.Tag.from_str("v1.0.0") > git.Tag.from_str("v0.3.1")


def test_tags_returns_ordered_newest_first() -> None:
    tags = git.get_version_tags(fetch=False)
    # use a couple hard coded tags for extra sanity
    current_latest = git.Tag.from_str("v0.3.1")
    older = git.Tag.from_str("v0.2.0")
    seen_latest = False
    prev = tags[0]
    for tag in tags:
        # ensure that actual versions are correctly ordered
        assert (prev.major, prev.minor, prev.micro) >= (
            tag.major,
            tag.minor,
            tag.micro,
        )
        if prev.patch and tag.patch:
            assert prev.patch > tag.patch
        prev = tag

        if tag == current_latest:
            seen_latest = True
        elif tag == older:
            assert seen_latest, "We should see newer tags first"
