# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sys

from materialize import github
from materialize.mz_version import MzVersion
from materialize.version_list import VersionsFromDocs

MIN_VERSION = MzVersion.parse_mz("v26.27.0")


def main() -> int:
    versions = [
        version
        for version in VersionsFromDocs(
            respect_released_tag=True, only_publish_helm_chart=False, skip_rc=True
        ).minor_versions()
        if version >= MIN_VERSION
    ]
    if not versions:
        print("No released versions found in the docs")
        return 0
    latest = versions[-1]

    existing_tags = github.list_release_tags()
    failed = []
    for version in versions:
        tag = str(version)
        if tag in existing_tags:
            continue
        print(f"Creating GitHub release for {tag}...")
        try:
            github.create_release(
                tag,
                body=f"See the [release notes](https://materialize.com/docs/releases/#{tag.replace('.', '')}) for what changed in this release.",
                make_latest=version == latest,
            )
        except Exception as e:
            print(f"Failed to create GitHub release for {tag}: {e}")
            failed.append(tag)
    if failed:
        print(f"Failed to create GitHub releases for: {', '.join(failed)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
