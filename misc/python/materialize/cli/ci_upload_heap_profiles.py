# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# ci_upload_heap_profiles.py - Upload memory heap profiles during an mzcompose run

import argparse
import json
import re
import subprocess
import sys
import time
from threading import Thread

CLUSTERD_COMMAND_RE = re.compile(
    r"--internal-http-listen-addr=(?P<socket>[^ ]*).*--log-prefix=(?P<logprefix>[^ ]*)"
)


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="ci-upload-heap-profiles",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="ci-upload-heap-profiles uploads memory heap profiles during an mzcompose run",
    )

    parser.add_argument("composition", type=str)
    parser.add_argument("--upload", action=argparse.BooleanOptionalAction, default=True)

    args = parser.parse_args()
    mzcompose = ["bin/mzcompose", "--find", args.composition, "--mz-quiet"]
    time_str = time.strftime("%Y-%m-%d_%H_%M_%S")
    threads = []

    def run(service: str, backend: list[str], suffix: str = ""):
        heap_profile = subprocess.run(
            mzcompose + ["exec", service, "curl", "--silent"] + backend,
            text=False,
            capture_output=True,
        ).stdout

        if not heap_profile:
            return

        filename = f"prof-{service}{suffix}-{time_str}.pb.gz"
        with open(filename, "wb") as f:
            f.write(heap_profile)
        if args.upload:
            subprocess.run(
                [
                    "buildkite-agent",
                    "artifact",
                    "upload",
                    "--log-level",
                    "error",
                    filename,
                ],
                capture_output=True,
            )

    services = json.loads(
        subprocess.run(
            mzcompose + ["ps", "--format", "json"], text=True, capture_output=True
        ).stdout
    )
    for service in services:
        if service["Image"].startswith("materialize/clusterd:"):
            threads.append(
                Thread(
                    target=run,
                    args=(service["Service"], ["http://127.0.0.1:6878/heap"]),
                )
            )
        elif service["Image"].startswith("materialize/materialized:"):
            threads.append(
                Thread(
                    target=run,
                    args=(service["Service"], ["http://127.0.0.1:6878/prof/heap"]),
                )
            )

            for line in subprocess.run(
                mzcompose + ["exec", service["Service"], "ps", "aux"],
                text=True,
                capture_output=True,
            ).stdout.splitlines():
                if match := CLUSTERD_COMMAND_RE.search(line):
                    threads.append(
                        Thread(
                            target=run,
                            args=(
                                service["Service"],
                                [
                                    "--unix-socket",
                                    match.group("socket"),
                                    "http:/prof/heap",
                                ],
                                f"-{match.group('logprefix')}",
                            ),
                        )
                    )

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    return 0


if __name__ == "__main__":
    sys.exit(main())
