# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize import bintray
import dateutil.parser
import os
from typing import List, Dict
from datetime import datetime, timedelta, timezone


def get_old_versions(pc: bintray.PackageClient) -> List[str]:
    version_strs = pc.get_metadata().json()["versions"]
    versions = (pc.get_version(v).json() for v in version_strs)

    def is_old(v: Dict[str, str]) -> bool:
        return datetime.now() - datetime.strptime(
            v["created"],
            # match the `Z` literally to work around
            # python3.6 timezone parsing bugs.
            #
            # If Bintray ever sends us dates in a
            # different time zone than `Z`, this
            # will break.
            "%Y-%m-%dT%H:%M:%S.%fZ",
        ) > timedelta(days=14)

    old_versions = [
        v["name"] for v in versions if dateutil.parser.parse(v["created"]) if is_old(v)
    ]
    return old_versions


def main() -> None:
    pc = (
        bintray.Client(
            "materialize", user="ci@materialize", api_key=os.environ["BINTRAY_API_KEY"]
        )
        .repo("apt")
        .package("materialized-unstable")
    )
    old_versions = get_old_versions(pc)
    print(f"Will delete {len(old_versions)} old versions")
    for v in old_versions:
        print(f"Deleting version {v}")
        pc.delete_version(v)


if __name__ == "__main__":
    main()
