# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path

import boto3

from materialize import elf

# The S3 bucket in which to store debuginfo.
DEBUGINFO_S3_BUCKET = "materialize-debuginfo"

# The binaries for which debuginfo should be uploaded to S3 and Polar Signals.
DEBUGINFO_BINS = {"environmentd", "clusterd", "balancerd", "materialized"}


def upload_debuginfo_to_s3(bin_path: Path, dbg_path: Path, is_tag_build: bool) -> str:
    s3 = boto3.client("s3")

    with open(bin_path, "rb") as exe, open(dbg_path, "rb") as dbg:
        build_id = elf.get_build_id(exe)
        assert build_id.isalnum()
        assert len(build_id) > 0

        dbg_build_id = elf.get_build_id(dbg)
        assert build_id == dbg_build_id

        for fileobj, name in [
            (exe, "executable"),
            (dbg, "debuginfo"),
        ]:
            key = f"buildid/{build_id}/{name}"
            print(f"Uploading {name} to s3://{DEBUGINFO_S3_BUCKET}/{key}...")
            fileobj.seek(0)
            s3.upload_fileobj(
                Fileobj=fileobj,
                Bucket=DEBUGINFO_S3_BUCKET,
                Key=key,
                ExtraArgs={
                    "Tagging": f"ephemeral={'false' if is_tag_build else 'true'}",
                },
            )

        return build_id
