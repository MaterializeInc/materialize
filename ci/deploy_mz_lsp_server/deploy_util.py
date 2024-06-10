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

from materialize import git
from materialize.mz_version import MzLspServerVersion

BINARIES_BUCKET = "materialize-binaries"
TAG = os.environ["BUILDKITE_TAG"]
MZ_LSP_SERVER_VERSION = MzLspServerVersion.parse(TAG)


def _tardir(name: str) -> tarfile.TarInfo:
    """Takes a dir path and creates a TarInfo. It sets the
    modification time, mode, and type of the dir."""

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
    """Uploads the tarball (.tar) to the S3 binaries bucket."""
    s3_object = f"mz-lsp-server-{version}-{platform}.tar.gz"
    boto3.client("s3").upload_file(
        Filename=str(tarball),
        Bucket=BINARIES_BUCKET,
        Key=s3_object,
    )
    if "aarch64" in platform:
        upload_redirect(
            f"mz-lsp-server-{version}-{platform.replace('aarch64', 'arm64')}.tar.gz",
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
    """Uploads an S3 object containing the latest version number of the package,
    not the package itself. This is useful for determining the
    latest available version number of the package.
    As an example, mz (CLI) uses this information to check if there is a new update available.
    """
    upload_redirect(
        f"mz-lsp-server-latest-{platform}.tar.gz",
        f"/mz-lsp-server-{version}-{platform}.tar.gz",
    )
    if "aarch64" in platform:
        upload_latest_redirect(platform.replace("aarch64", "arm64"), version)


def deploy_tarball(platform: str, lsp: Path) -> None:
    tar_path = Path(f"mz-lsp-server-{platform}.tar.gz")
    with tarfile.open(str(tar_path), "x:gz") as f:
        f.addfile(_tardir("mz-lsp-server"))
        f.addfile(_tardir("mz/bin"))
        f.add(
            str(lsp),
            arcname="mz/bin/mz-lsp-server",
            filter=_sanitize_tarinfo,
        )

    size = humanize.naturalsize(os.lstat(tar_path).st_size)
    print(f"Tarball size: {size}")

    upload_tarball(tar_path, platform, f"v{MZ_LSP_SERVER_VERSION.str_without_prefix()}")
    if is_latest_version():
        upload_latest_redirect(
            platform, f"v{MZ_LSP_SERVER_VERSION.str_without_prefix()}"
        )


def is_latest_version() -> bool:
    latest_version = max(
        t
        for t in git.get_version_tags(version_type=MzLspServerVersion)
        if t.prerelease is None
    )
    return MZ_LSP_SERVER_VERSION == latest_version
