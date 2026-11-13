# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from ci import tarball_uploader
from materialize import MZ_ROOT, cargo
from materialize.mz_version import MzVersion


def mz_deploy_version(workspace: cargo.Workspace | None = None) -> MzVersion:
    """The version to publish, taken from the release tag being built.

    mz-deploy ships on the Materialize release train, so the tag is a plain
    `vX.Y.Z` release tag rather than a tool-specific one. Asserts the tag agrees
    with the crate version, which catches a tag pushed at a commit that
    `bin/bump-version` never touched.
    """
    version = MzVersion.parse(os.environ["BUILDKITE_TAG"])
    workspace = workspace or cargo.Workspace(MZ_ROOT)
    crate_version = MzVersion.parse_without_prefix(
        workspace.crates["mz-deploy"].version_string
    )
    assert crate_version == version, (
        f"git tag {version} does not match src/mz-deploy/Cargo.toml {crate_version}; "
        "was bin/bump-version run on this commit?"
    )
    return version


def should_update_latest(version: MzVersion) -> bool:
    """Whether this version may claim the `mz-deploy-latest-*` redirect.

    Excludes prereleases, so release candidates publish a versioned tarball
    without moving the URL our install docs point at, and excludes back-ported
    patches, which would otherwise drag `latest` backwards.
    """
    return version.prerelease is None and tarball_uploader.is_latest_version(version)
