# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize import spawn
import json
from typing import Dict, List
from datetime import datetime, timedelta


def get_old_stream_names() -> List[str]:
    stream_names = json.loads(
        spawn.capture(
            ["aws", "kinesis", "list-streams", "--region=us-east-2"], unicode=True
        )
    )
    if "StreamNames" not in stream_names:
        print(f"Expected a list of stream names, found ", stream_names)
        raise

    stream_descriptions = [
        json.loads(
            spawn.capture(
                [
                    "aws",
                    "kinesis",
                    "describe-stream",
                    "--stream-name"
                    stream_name,
                    "--region=us-east-2",
                ],
                unicode=True,
            )
        )
        for stream_name in stream_names["StreamNames"]
    ]

    def is_old(desc: Dict) -> bool:
        if "StreamCreationTimestamp" in desc:
            return datetime.now() - datetime.utcfromtimestamp(
                desc["StreamCreationTimestamp"]
            ) > timedelta(hours=1)
        else:
            return False

    old_stream_names = [
        desc["StreamName"] for desc in stream_descriptions if is_old(desc)
    ]
    return old_stream_names


def main() -> None:
    old_stream_names = get_old_stream_names()
    print(f"Will delete {len(old_stream_names)} old Kinesis streams")
    for stream_name in old_stream_names:
        print(f"Deleting stream {stream_name}")
        spawn.capture(
            [
                "aws",
                "kinesis",
                "delete",
                "--stream-name",
                stream_name,
                "--region=us-east-2",
            ]
        )

    print(f"All deleted")


if __name__ == "__main__":
    main()
