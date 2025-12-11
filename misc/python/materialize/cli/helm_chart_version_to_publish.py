# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# helm_chart_version_to_publish — Get the latest helm-chart version to publish

import argparse

from materialize import docker
from materialize.version_list import VersionsFromDocs, get_all_self_managed_versions


def main():
    parser = argparse.ArgumentParser(
        prog="helm-chart-version-to-publish",
        description="Get the latest helm-chart version to publish",
    )
    parser.parse_args()

    version = VersionsFromDocs(
        # Also release to self-managed, even if we didn't ship to cloud, for
        # example because we skip a week intentionally.
        respect_released_tag=False,
        # Don't publish future releases which are prepared in docs already.
        respect_date=True,
        # We use `publish_helm_chart: false` to manually prevent a release from
        # being published.
        only_publish_helm_chart=True,
    ).all_versions()[-1]
    published_versions = get_all_self_managed_versions()
    if version not in published_versions:
        assert docker.image_of_release_version_exists(
            version, quiet=True
        ), f"Version {version} not found on DockerHub"
        print(version)


if __name__ == "__main__":
    main()
