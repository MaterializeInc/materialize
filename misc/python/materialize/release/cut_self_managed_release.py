# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Cut a new self-managed release and push the lts-v* branch to the upstream Materialize repository."""

import argparse
import re
import sys

from semver.version import Version

from materialize import MZ_ROOT, spawn
from materialize.git import checkout, commit_all_changed, fetch, get_branch_name


def parse_version(version: str) -> Version:
    return Version.parse(re.sub(r"^v", "", version))


def main():
    parser = argparse.ArgumentParser(
        prog="cut_self_managed_release",
        description="Creates a new self-managed Materialize release",
    )
    parser.add_argument(
        "--mz-version",
        help="Version of Materialize, has to exist already",
        type=parse_version,
        required=True,
    )
    parser.add_argument(
        "--helm-chart-version",
        help="Version of Helm Chart, will be created",
        type=parse_version,
        required=True,
    )
    parser.add_argument(
        "--remote",
        help="Git remote name of Materialize repo",
        type=str,
        required=True,
    )

    args = parser.parse_args()
    mz_version = f"v{args.mz_version}"
    helm_chart_version = f"v{args.helm_chart_version}"
    lts_branch = f"lts-v{args.mz_version.major}.{args.mz_version.minor}"
    remote_lts_branch = f"{args.remote}/{lts_branch}"
    current_branch = get_branch_name()

    try:
        fetch(args.remote)
        print(f"Checking out LTS branch {remote_lts_branch}")
        checkout(remote_lts_branch)
        print(
            f"Bumping helm-chart version to {helm_chart_version} with Materialize {mz_version}"
        )
        spawn.runv(
            [
                MZ_ROOT / "bin" / "helm-chart-version-bump",
                f"--helm-chart-version={helm_chart_version}",
                mz_version,
            ]
        )
        print("Recreating helm-docs")
        spawn.runv(["helm-docs", "misc/helm-charts"])
        print("Creating commit")
        commit_all_changed(
            f"Bumping helm-chart version to {helm_chart_version} with Materialize {mz_version}"
        )
        print(f"Pushing to {remote_lts_branch}")
        spawn.runv(["git", "push", args.remote, f"HEAD:{lts_branch}"])
    finally:
        # The caller may have started in a detached HEAD state.
        if current_branch:
            checkout(current_branch)


if __name__ == "__main__":
    sys.exit(main())
