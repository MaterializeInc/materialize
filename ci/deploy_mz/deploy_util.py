# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import tarfile
import tempfile
import time
from pathlib import Path

import boto3
import humanize
import semver

from materialize import git

APT_BUCKET = "materialize-apt"
BINARIES_BUCKET = "materialize-binaries"
TAG = os.environ["BUILDKITE_TAG"]
VERSION = semver.VersionInfo.parse(TAG.removeprefix("mz-v"))


def _tardir(name: str) -> tarfile.TarInfo:
    d = tarfile.TarInfo(name)
    d.mtime = int(time.time())
    d.mode = 0o40755
    d.type = tarfile.DIRTYPE
    return d


def _sanitize_tarinfo(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo:
    tarinfo.uid = tarinfo.gid = 0
    tarinfo.uname = tarinfo.gname = "root"
    return tarinfo


def upload_tarball(tarball: Path, platform: str, version: str) -> None:
    s3_object = f"mz-{version}-{platform}.tar.gz"
    boto3.client("s3").upload_file(
        Filename=str(tarball),
        Bucket=BINARIES_BUCKET,
        Key=s3_object,
    )
    if "aarch64" in platform:
        upload_redirect(
            f"mz-{version}-{platform.replace('aarch64', 'arm64')}.tar.gz",
            f"/{s3_object}",
        )


def upload_redirect(key: str, to: str) -> None:
    with tempfile.NamedTemporaryFile() as empty:
        boto3.client("s3").upload_fileobj(
            Fileobj=empty,
            Bucket=BINARIES_BUCKET,
            Key=key,
            ExtraArgs={"WebsiteRedirectLocation": to},
        )


def upload_latest_redirect(platform: str, version: str) -> None:
    upload_redirect(
        f"mz-latest-{platform}.tar.gz",
        f"/mz-{version}-{platform}.tar.gz",
    )
    if "aarch64" in platform:
        upload_latest_redirect(platform.replace("aarch64", "arm64"), version)


def deploy_tarball(platform: str, mz: Path) -> None:
    tar_path = Path(f"mz-{platform}.tar.gz")
    with tarfile.open(str(tar_path), "x:gz") as f:
        f.addfile(_tardir("mz"))
        f.addfile(_tardir("mz/bin"))
        f.add(
            str(mz),
            arcname="mz/bin/mz",
            filter=_sanitize_tarinfo,
        )

    size = humanize.naturalsize(os.lstat(tar_path).st_size)
    print(f"Tarball size: {size}")

    upload_tarball(tar_path, platform, f"v{VERSION}")
    if is_latest_version():
        upload_latest_redirect(platform, f"v{VERSION}")


def is_latest_version() -> bool:
    latest_version = next(
        t for t in git.get_version_tags(prefix="mz-v") if t.prerelease is None
    )
    return VERSION == latest_version
