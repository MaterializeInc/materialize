# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
from subprocess import Popen, PIPE
from typing import Dict, List
from datetime import datetime, timedelta


def get_old_stream_names() -> List[str]:
    process = Popen(
        ["aws", "kinesis", "list-streams", "--region=us-east-2"],
        stdout=PIPE,
        stderr=PIPE,
    )
    stdout, stderr = process.communicate()
    if stderr:
        print(f"Hit error listing Kinesis streams: ", stderr)
        raise

    stream_names = json.loads(stdout)
    if "StreamNames" not in stream_names:
        print(f"Expected a list of stream names, found ", stream_names)
        raise

    stream_descriptions = []
    for stream_name in stream_names:
        process = Popen(
            ["aws", "kinesis", "describe-stream", stream_name, "--region=us-east-2"]
        )
        stdout, stderr = process.communicate()
        if stderr:
            print(f"Hit error describing Kinesis stream ", stream_name, stderr)
        else:
            stream_descriptions.append(json.loads(stdout))

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
        process = Popen(
            [
                "aws",
                "kinesis",
                "delete",
                "--stream-name",
                stream_name,
                "--region=us-east-2",
            ]
        )
        stdout, stderr = process.communicate()
        if stderr:
            print(f"Hit error deleting Kinesis stream ", stream_name, stderr)
            raise

    print(f"All deleted")


if __name__ == "__main__":
    main()
