# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import json
import logging
import os
import shutil
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests
from semver.version import VersionInfo

from materialize import ROOT, cargo, spawn

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=os.environ.get("MZ_DEV_LOG", "INFO").upper(),
)
logger = logging.getLogger(__name__)

PUBLISH_CRATES = ["mz-sql-lexer"]


@dataclass(frozen=True)
class Version:
    rust: VersionInfo
    node: str
    is_development: bool


def generate_version(
    crate_version: VersionInfo, build_identifier: Optional[int]
) -> Version:
    node_version = str(crate_version)
    is_development = False
    if crate_version.prerelease == "dev":
        if build_identifier is None:
            raise ValueError(
                "a build identifier must be provided for prerelease builds"
            )
        node_version = str(crate_version.replace(prerelease=f"dev.{build_identifier}"))
        is_development = True
    else:
        buildkite_tag = os.environ.get("BUILDKITE_TAG")
        assert (
            buildkite_tag == node_version
        ), f"Buildkite tag ({buildkite_tag}) does not match environmentd version ({crate_version})"
    return Version(rust=crate_version, node=node_version, is_development=is_development)


def build_package(version: Version, crate_path: Path) -> Path:
    spawn.runv(["bin/wasm-build", str(crate_path)])
    package_path = crate_path / "pkg"
    shutil.copyfile(str(ROOT / "LICENSE"), str(package_path / "LICENSE"))
    with open(package_path / "package.json", "r+") as package_file:
        package = json.load(package_file)
        # Since all packages are scoped to the MaterializeInc org, names don't need prefixes
        package["name"] = package["name"].replace("/mz-", "/")
        package["version"] = version.node
        package["license"] = "SEE LICENSE IN 'LICENSE'"
        package["repository"] = "github:MaterializeInc/materialize"
        package_file.seek(0)
        json.dump(package, package_file, indent=2)
    return package_path


def release_package(version: Version, package_path: Path) -> None:
    with open(package_path / "package.json", "r") as package_file:
        package = json.load(package_file)
    name = package["name"]
    dist_tag = "dev" if version.is_development else "latest"
    if version_exists_in_npm(name, version):
        logger.warning("%s %s already released, skipping.", name, version.node)
        return
    else:
        logger.info("Releasing %s %s", name, version.node)
        set_npm_credentials(package_path)
        spawn.runv(
            ["npm", "publish", "--access", "public", "--tag", dist_tag],
            cwd=package_path,
        )


def build_all(
    workspace: cargo.Workspace, version: Version, *, do_release: bool = True
) -> None:
    for crate_name in PUBLISH_CRATES:
        crate_path = workspace.crates[crate_name].path
        logger.info("Building %s @ %s", crate_path, version.node)
        package_path = build_package(version, crate_path)
        logger.info("Built %s", crate_path)
        if do_release:
            release_package(version, package_path)
            logger.info("Released %s", package_path)
        else:
            logger.info("Skipping release for %s", package_path)


def version_exists_in_npm(name: str, version: Version) -> bool:
    quoted = urllib.parse.quote(name)
    res = requests.get(f"https://registry.npmjs.org/{quoted}/{version.node}")
    if res.status_code == 404:
        # This is a new package
        return False
    res.raise_for_status()
    return True


def set_npm_credentials(package_path: Path) -> None:
    (package_path / ".npmrc").write_text(
        "//registry.npmjs.org/:_authToken=${NPM_TOKEN}\n"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        "npm.py", description="build and publish NPM packages"
    )
    parser.add_argument(
        "-v,--verbose",
        action="store_true",
        dest="verbose",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--release",
        action=argparse.BooleanOptionalAction,
        dest="do_release",
        default=True,
        help="Whether or not the built package should be released",
    )
    parser.add_argument(
        "--build-id",
        type=int,
        help="An optional build identifier. Used in pre-release version numbers",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    build_id = args.build_id
    if os.environ.get("BUILDKITE_BUILD_NUMBER") is not None:
        if build_id is not None:
            logger.warning(
                "Build ID specified via both envvar and CLI arg. Using CLI value"
            )
        else:
            build_id = int(os.environ["BUILDKITE_BUILD_NUMBER"])
    if args.do_release and "NPM_TOKEN" not in os.environ:
        raise ValueError("'NPM_TOKEN' must be set")
    workspace = cargo.Workspace(ROOT)
    crate_version = workspace.crates["mz-environmentd"].version
    version = generate_version(crate_version, build_id)
    build_all(workspace, version, do_release=args.do_release)
