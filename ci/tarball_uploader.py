# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This file contains a class that can be used to upload a tarball to the materialize-binaries S3 bucket.

import os
import subprocess
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Any

import boto3
import humanize

from materialize import git
from materialize.mz_version import TypedVersionBase

BINARIES_BUCKET = "materialize-binaries"

# When set, uploads use short-lived credentials for this role, obtained by
# exchanging the job's Buildkite OIDC token, rather than the ambient AWS
# credentials.
UPLOAD_ROLE_ARN_ENV = "MZ_BINARIES_UPLOAD_ROLE_ARN"


def _s3_client() -> Any:
    role_arn = os.environ.get(UPLOAD_ROLE_ARN_ENV)
    if not role_arn:
        return boto3.client("s3")

    # Requested immediately before use: OIDC tokens are short-lived and the
    # build that precedes the upload can outlast one.
    token = subprocess.check_output(
        [
            "buildkite-agent",
            "oidc",
            "request-token",
            "--audience",
            "sts.amazonaws.com",
        ],
        text=True,
    ).strip()
    session = boto3.Session()
    creds = session.client("sts").assume_role_with_web_identity(
        RoleArn=role_arn,
        RoleSessionName=f"buildkite-{os.environ['BUILDKITE_JOB_ID']}"[:64],
        WebIdentityToken=token,
        DurationSeconds=3600,
    )["Credentials"]
    return boto3.client(
        "s3",
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )


class TarballUploader:
    def __init__(self, package_name: str, version: TypedVersionBase):
        self.package_name = package_name
        self.version = version
        self.version_str = f"v{version.str_without_prefix()}"
        self._s3: Any = None

    def _client(self) -> Any:
        if self._s3 is None:
            self._s3 = _s3_client()
        return self._s3

    def _create_tardir(self, name: str) -> tarfile.TarInfo:
        """Takes a dir path and creates a TarInfo. Sets the
        modification time, mode, and type of the dir."""
        d = tarfile.TarInfo(name)
        d.mtime = int(time.time())
        d.mode = 0o40755
        d.type = tarfile.DIRTYPE
        return d

    def _sanitize_tarinfo(self, tarinfo: tarfile.TarInfo) -> tarfile.TarInfo:
        tarinfo.uid = tarinfo.gid = 0
        tarinfo.uname = tarinfo.gname = "root"
        return tarinfo

    def _upload_redirect(self, key: str, to: str) -> None:
        with tempfile.NamedTemporaryFile() as empty:
            self._client().upload_fileobj(
                Fileobj=empty,
                Bucket=BINARIES_BUCKET,
                Key=key,
                ExtraArgs={"WebsiteRedirectLocation": to},
            )

    def _upload_tarball(self, tarball: Path, platform: str) -> None:
        """Uploads the tarball (.tar) to the S3 bucket."""
        s3_object = f"{self.package_name}-{self.version_str}-{platform}.tar.gz"
        self._client().upload_file(
            Filename=str(tarball),
            Bucket=BINARIES_BUCKET,
            Key=s3_object,
        )
        if "aarch64" in platform:
            self._upload_redirect(
                f"{self.package_name}-{self.version_str}-{platform.replace('aarch64', 'arm64')}.tar.gz",
                f"/{s3_object}",
            )

    def _upload_latest_redirect(self, platform: str) -> None:
        """Uploads an S3 object for the latest version redirect."""
        self._upload_redirect(
            f"{self.package_name}-latest-{platform}.tar.gz",
            f"/{self.package_name}-{self.version_str}-{platform}.tar.gz",
        )
        if "aarch64" in platform:
            self._upload_latest_redirect(platform.replace("aarch64", "arm64"))

    def deploy_tarball(self, platform: str, binary_path: Path) -> None:
        """Creates and uploads a tarball containing the binary."""
        tar_path = Path(f"{self.package_name}-{platform}.tar.gz")
        with tarfile.open(str(tar_path), "x:gz") as f:
            f.addfile(self._create_tardir("mz/bin"))
            f.add(
                str(binary_path),
                arcname=f"mz/bin/{binary_path.name}",
                filter=self._sanitize_tarinfo,
            )

        size = humanize.naturalsize(os.lstat(tar_path).st_size)
        print(f"Tarball size: {size}")

        self._upload_tarball(tar_path, platform)
        self._upload_latest_redirect(platform)


def is_latest_version(version: TypedVersionBase) -> bool:
    latest_version = max(
        t
        for t in git.get_version_tags(version_type=type(version))
        if t.prerelease is None
    )
    return version == latest_version
