# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize import git
from materialize import spawn
from pathlib import Path
import humanize
import os
import tarfile
import tempfile
import time


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
    s3_url = f"s3://downloads.mtrlz.dev/materialized-{version}-{platform}.tar.gz"
    spawn.runv(["aws", "s3", "cp", "--acl=public-read", tarball, s3_url])


def set_latest_redirect(platform: str, version: str) -> None:
    with tempfile.NamedTemporaryFile() as empty:
        target = f"/materialized-{version}-{platform}.tar.gz"
        s3_url = f"s3://downloads.mtrlz.dev/materialized-latest-{platform}.tar.gz"
        spawn.runv(
            [
                "aws",
                "s3",
                "cp",
                "--acl=public-read",
                "--cache-control=no-cache",
                "--metadata-directive=REPLACE",
                f"--website-redirect={target}",
                empty.name,
                s3_url,
            ]
        )


def deploy_tarball(platform: str, materialized: Path) -> None:
    tar_path = Path("materialized.tar.gz")
    with tarfile.open(str(tar_path), "x:gz") as f:
        f.addfile(_tardir("materialized"))
        f.addfile(_tardir("materialized/bin"))
        f.add(
            str(materialized),
            arcname="materialized/bin/materialized",
            filter=_sanitize_tarinfo,
        )
        f.addfile(_tardir("materialized/etc/materialized"))

    size = humanize.naturalsize(os.lstat(tar_path).st_size)
    print(f"Tarball size: {size}")

    if os.environ["BUILDKITE_TAG"]:
        upload_tarball(tar_path, platform, os.environ["BUILDKITE_TAG"])
    else:
        commit_sha = git.rev_parse("HEAD")
        upload_tarball(tar_path, platform, commit_sha)
        set_latest_redirect(platform, commit_sha)
