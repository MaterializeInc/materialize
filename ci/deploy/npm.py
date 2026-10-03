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
import tempfile
import urllib.parse
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import requests
from semver.version import VersionInfo

from materialize import MZ_ROOT, buildkite, cargo, github, spawn

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=os.environ.get("MZ_DEV_LOG", "INFO").upper(),
)
logger = logging.getLogger(__name__)

PUBLISH_CRATES = ["mz-sql-lexer-wasm", "mz-sql-parser-wasm", "mz-sql-pretty-wasm"]

# The GitHub Actions workflow that performs the actual publish. Each package's
# trusted publisher on npmjs.com is pinned to this filename, so renaming the
# workflow requires updating those configurations too.
PUBLISH_WORKFLOW = "publish-npm.yml"


@dataclass(frozen=True)
class NpmPackageVersion:
    rust: VersionInfo
    node: str
    is_development: bool


def generate_version(
    crate_version: VersionInfo, build_identifier: int | None
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
        buildkite_tag = os.environ.get("BUILDKITE_TAG")
        # For self-managed branch the buildkite tag is not set, but we are still not in a prerelease version
        if buildkite_tag:
            # buildkite_tag starts with a 'v' and node_version does not.
            assert (
                buildkite_tag == f"v{node_version}"
            ), f"Buildkite tag ({buildkite_tag}) does not match environmentd version ({crate_version})"
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
        package["repository"] = "github:MaterializeInc/materialize"
        package_file.seek(0)
        json.dump(package, package_file, indent=2)
    return package_path


def stage_package(
    version: NpmPackageVersion, package_path: Path, staging_path: Path
) -> dict[str, Any] | None:
    """Pack a built package for publishing, returning its manifest entry.

    Returns None if the version is already on npm, in which case there is
    nothing to publish.
    """
    with open(package_path / "package.json") as package_file:
        package = json.load(package_file)
    name = package["name"]
    if version_exists_in_npm(name, version):
        logger.warning("%s %s already released, skipping.", name, version.node)
        return None
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
        logger.info("Packing %s %s", name, version.node)
        before = set(staging_path.glob("*.tgz"))
        spawn.runv(
            ["npm", "pack", "--pack-destination", str(staging_path)],
            cwd=package_path,
        )
        packed = set(staging_path.glob("*.tgz")) - before
        if len(packed) != 1:
            raise RuntimeError(
                f"expected npm pack to produce one tarball for {name}, "
                f"got {sorted(p.name for p in packed)}"
            )
        tarball = packed.pop().name
        # If we do not specify a dist tag, npm automatically tags the publish
        # as `latest`. So, force a dist tag for release builds that are lower
        # than the stable version. This usually happens when we cut a hotfix
        # release for the in-production version after a release has been cut
        # for the next version.
        return {
            "name": name,
            "version": version.node,
            "tarball": tarball,
            "tag": cast(str, dist_tag or branch_tag),
            # The branch tag has to be applied after the publish, because a
            # publish can only set one tag.
            "extra_tags": [branch_tag] if dist_tag == "latest" and branch_tag else [],
        }


def publish_via_github(packages: list[dict[str, Any]], staging_path: Path) -> None:
    """Hand the packed tarballs off to GitHub Actions to publish.

    npm's trusted publishing does not accept OIDC tokens from Buildkite, so
    the actual `npm publish` runs in the publish-npm workflow, which
    authenticates to npm with an OIDC token instead of a long-lived one. This
    uploads the tarballs to a staging bucket that the workflow's AWS role can
    read, triggers the workflow, and fails this step if the workflow fails, so
    that a broken publish still turns the Buildkite pipeline red.
    """
    bucket = os.environ["NPM_STAGING_BUCKET"]
    # Unique per job, and reused as the handle for finding the run that this
    # dispatch creates.
    dispatch_id = os.environ.get("BUILDKITE_JOB_ID") or str(uuid.uuid4())
    staging_uri = f"s3://{bucket}/npm/{dispatch_id}/"
    sha = os.environ.get("BUILDKITE_COMMIT", "")
    build_url = os.environ.get("BUILDKITE_BUILD_URL", "")

    with open(staging_path / "manifest.json", "w") as manifest_file:
        json.dump(
            {
                "dispatch_id": dispatch_id,
                "sha": sha,
                "build_url": build_url,
                "packages": packages,
            },
            manifest_file,
            indent=2,
        )

    spawn.runv(
        [
            "aws",
            "s3",
            "cp",
            "--recursive",
            "--only-show-errors",
            str(staging_path),
            staging_uri,
        ],
    )
    logger.info("Staged %d package(s) at %s", len(packages), staging_uri)

    github.repository_dispatch(
        "publish-npm",
        {
            "staging_uri": staging_uri,
            "dispatch_id": dispatch_id,
            "sha": sha,
            "build_url": build_url,
        },
    )
    logger.info("Dispatched publish-npm, waiting for the workflow run")
    # Comfortably inside the Buildkite step's timeout, so that a workflow that
    # hangs surfaces as an annotated failure here rather than as a killed step.
    run = github.wait_for_workflow_run(PUBLISH_WORKFLOW, staging_uri, timeout_secs=1200)

    summary = "\n".join(
        f"- `{p['name']}@{p['version']}` ({p['tag']})" for p in packages
    )
    if buildkite.is_in_buildkite():
        buildkite.add_annotation(
            "info" if run["conclusion"] == "success" else "error",
            f"npm publish {run['conclusion']}: {run['html_url']}",
            summary,
        )
    if run["conclusion"] != "success":
        raise RuntimeError(
            f"publish-npm workflow {run['conclusion']}: {run['html_url']}"
        )
    logger.info("Published %d package(s): %s", len(packages), run["html_url"])


def build_all(
    workspace: cargo.Workspace, version: NpmPackageVersion, *, do_release: bool = True
) -> None:
    with tempfile.TemporaryDirectory() as staging_dir:
        staging_path = Path(staging_dir)
        packages: list[dict[str, Any]] = []
        for crate_name in PUBLISH_CRATES:
            crate_path = workspace.all_crates[crate_name].path
            logger.info("Building %s @ %s", crate_path, version.node)
            package_path = build_package(version, crate_path)
            logger.info("Built %s", crate_path)
            if not do_release:
                logger.info("Skipping release for %s", package_path)
                continue
            package = stage_package(version, package_path, staging_path)
            if package is not None:
                packages.append(package)
        if packages:
            publish_via_github(packages, staging_path)
        elif do_release:
            logger.info("Nothing to publish; all versions already exist on npm")


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
    if args.do_release:
        for var in ["GITHUB_TOKEN", "NPM_STAGING_BUCKET"]:
            if var not in os.environ:
                raise ValueError(f"{var!r} must be set")
    root_workspace = cargo.Workspace(MZ_ROOT)
    wasm_workspace = cargo.Workspace(MZ_ROOT / "misc" / "wasm")
    crate_version = VersionInfo.parse(
        root_workspace.crates["mz-environmentd"].version_string
    )
    version = generate_version(crate_version, build_id)
    build_all(wasm_workspace, version, do_release=args.do_release)
