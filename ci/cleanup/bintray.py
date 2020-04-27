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
import json
from datetime import datetime, timedelta, timezone


def get_old_versions(bt: bintray.Client) -> List[str]:
    pc = bt.repo("apt").package("materialized-unstable")
    version_strs = json.loads(pc.get_metadata().content)["versions"]
    versions = (json.loads(pc.get_version(v).content) for v in version_strs)

    def is_old(v: Dict[str, str]) -> bool:
        return datetime.now(tz=timezone.utc) - dateutil.parser.parse(
            v["created"]
        ) > timedelta(days=14)

    old_versions = [
        v["name"] for v in versions if dateutil.parser.parse(v["created"]) if is_old(v)
    ]
    return old_versions


def main() -> None:
    bt = bintray.Client(
        "materialize", user="ci@materialize", api_key=os.environ["BINTRAY_API_KEY"]
    )
    old_versions = get_old_versions(bt)
    pc = bt.repo("apt").package("materialized-unstable")
    print(f"Will delete {len(old_versions)} old versions")
    for v in old_versions:
        print(f"Deleting version {v}")
        pc.delete_version(v)


if __name__ == "__main__":
    main()
