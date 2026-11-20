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
from typing import cast

import requests
from semver.version import VersionInfo

from materialize import MZ_ROOT, cargo, spawn

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=os.environ.get("MZ_DEV_LOG", "INFO").upper(),
)
logger = logging.getLogger(__name__)

PUBLISH_CRATES = ["mz-sql-lexer-wasm", "mz-sql-parser-wasm", "mz-sql-pretty-wasm"]


@dataclass(frozen=True)
class NpmPackageVersion:
    rust: VersionInfo
    node: str
    is_development: bool


def generate_version(
    crate_version: VersionInfo, build_identifier: int | None, git_tag: str | None
) -> NpmPackageVersion:
    node_version = str(crate_version)
    is_development = False
    if crate_version.prerelease:
        if build_identifier is None:
            raise ValueError(
                "a build identifier must be provided for prerelease builds"
            )
        node_version = str(
            crate_version.replace(
                prerelease=f"{crate_version.prerelease}.{build_identifier}"
            )
        )
        is_development = True
    else:
        # For self-managed branch the tag is not set, but we are still not in a prerelease version
        if git_tag:
            # git_tag starts with a 'v' and node_version does not.
            assert (
                git_tag == f"v{node_version}"
            ), f"Git tag ({git_tag}) does not match environmentd version ({crate_version})"
    return NpmPackageVersion(
        rust=crate_version, node=node_version, is_development=is_development
    )


def build_package(version: NpmPackageVersion, crate_path: Path) -> Path:
    spawn.runv(["bin/wasm-build", str(crate_path)])
    package_path = crate_path / "pkg"
    shutil.copyfile(str(MZ_ROOT / "LICENSE"), str(package_path / "LICENSE"))
    with open(package_path / "package.json", "r+") as package_file:
        package = json.load(package_file)
        # Since all packages are scoped to the MaterializeInc org, names don't need prefixes
        package["name"] = package["name"].replace("/mz-", "/")
        # Remove any -wasm suffixes.
        package["name"] = package["name"].removesuffix("-wasm")
        package["version"] = version.node
        package["license"] = "SEE LICENSE IN 'LICENSE'"
        package["repository"] = {
            "type": "git",
            "url": "git+https://github.com/MaterializeInc/materialize.git",
        }
        package_file.seek(0)
        json.dump(package, package_file, indent=2)
    return package_path


def stage_package(
    version: NpmPackageVersion, package_path: Path, out_dir: Path
) -> None:
    """Pack the package into `out_dir` and append it to `out_dir/plan.txt`.

    Each plan line is `<tarball> <publish tag> <name@version> [<extra dist-tag>]`,
    consumed by the publish job in .github/workflows/publish.yml.
    """
    with open(package_path / "package.json") as package_file:
        package = json.load(package_file)
    name = package["name"]
    if version_exists_in_npm(name, version):
        logger.warning("%s %s already released, skipping.", name, version.node)
        return
    else:
        dist_tag: str | None = "dev" if version.is_development else "latest"
        branch_tag: str | None = None
        if dist_tag == "latest":
            branch_tag = f"latest-{version.rust.major}.{version.rust.minor}"
            latest_published = get_latest_version(name)
            if latest_published and latest_published > version.node:
                logger.info(
                    "Latest version of %s on npm (%s) is newer than %s. Skipping tag.",
                    name,
                    latest_published,
                    version.node,
                )
                dist_tag = None
        # If we do not specify a dist tag, this automatically is tagged as
        # `latest`. So, force a dist tag for release builds that are lower than
        # the stable version. This usually happens when we cut a hotfix release
        # for the in-production version after a release has been cut for the
        # next version.
        publish_tag = cast(str, dist_tag or branch_tag)
        # If we didn't tag the release with the branch tag, add it after publishing.
        extra_tag = branch_tag if dist_tag == "latest" and branch_tag else ""
        out_dir.mkdir(parents=True, exist_ok=True)
        packed = json.loads(
            spawn.capture(
                ["npm", "pack", "--json", "--pack-destination", str(out_dir.resolve())],
                cwd=package_path,
            )
        )
        tarball = packed[0]["filename"]
        logger.info("Staged %s %s as %s", name, version.node, tarball)
        with open(out_dir / "plan.txt", "a") as plan:
            plan.write(f"{tarball} {publish_tag} {name}@{version.node} {extra_tag}\n")


def build_all(
    workspace: cargo.Workspace,
    version: NpmPackageVersion,
    *,
    do_release: bool = True,
    out_dir: Path,
) -> None:
    for crate_name in PUBLISH_CRATES:
        crate_path = workspace.all_crates[crate_name].path
        logger.info("Building %s @ %s", crate_path, version.node)
        package_path = build_package(version, crate_path)
        logger.info("Built %s", crate_path)
        if do_release:
            stage_package(version, package_path, out_dir)
        else:
            logger.info("Skipping release for %s", package_path)


def _query_npm_version(name: str, version: str) -> requests.Response:
    """Queries NPM for a specific version of the package."""
    quoted = urllib.parse.quote(name)
    return requests.get(f"https://registry.npmjs.org/{quoted}/{version}")


def get_latest_version(name: str) -> VersionInfo | None:
    res = _query_npm_version(name, "latest")
    if res.status_code == 404:
        # This is a new package
        return None
    res.raise_for_status()
    data = res.json()
    version = data["version"]
    return VersionInfo.parse(version)


def version_exists_in_npm(name: str, version: NpmPackageVersion) -> bool:
    res = _query_npm_version(name, version.node)
    if res.status_code == 404:
        # This is a new package
        return False
    res.raise_for_status()
    return True


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
        help="Whether to stage the built packages for publishing",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=MZ_ROOT / "npm-dist",
        help="Where to write the packed tarballs and publish plan",
    )
    parser.add_argument(
        "--git-tag",
        default=os.environ.get("BUILDKITE_TAG"),
        help="The release tag being built, checked against the crate version",
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
    root_workspace = cargo.Workspace(MZ_ROOT)
    wasm_workspace = cargo.Workspace(MZ_ROOT / "misc" / "wasm")
    crate_version = VersionInfo.parse(
        root_workspace.crates["mz-environmentd"].version_string
    )
    version = generate_version(crate_version, build_id, args.git_tag)
    build_all(wasm_workspace, version, do_release=args.do_release, out_dir=args.out_dir)
