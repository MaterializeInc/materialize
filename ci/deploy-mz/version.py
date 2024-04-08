# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import boto3

from .deploy_util import BINARIES_BUCKET, MZ_CLI_VERSION


def main() -> None:
    print("--- Uploading version file")
    boto3.client("s3").put_object(
        Body=f"{MZ_CLI_VERSION.str_without_prefix()}",
        Bucket=BINARIES_BUCKET,
        Key="mz-latest.version",
        ContentType="text/plain",
    )


if __name__ == "__main__":
    main()
