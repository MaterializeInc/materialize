# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# ci_shards.py - Show which workflows/files the sharded jobs of a build ran.

import argparse
import json
import re
import subprocess
import sys


def fetch_build(pipeline: str, build_number: str) -> dict:
    try:
        output = subprocess.check_output(
            ["bk", "api", f"/pipelines/{pipeline}/builds/{build_number}", "--no-pager"]
        )
    except FileNotFoundError:
        sys.exit("error: bk CLI not found, install it and run `bk configure`")
    except subprocess.CalledProcessError as error:
        sys.exit(f"error: bk api failed with exit code {error.returncode}")
    return json.loads(output)


def natural_sort_key(label: str) -> list:
    return [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", label)]


def hyperlink(url: str, text: str) -> str:
    # OSC 8 terminal hyperlink, makes `text` clickable in iTerm2, kitty, etc.
    return f"\033]8;;{url}\033\\{text}\033]8;;\033\\"


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="ci-shards",
        description="Show which workflows/files the sharded jobs of a Buildkite "
        "build ran, based on the `Shard for <job>` build meta-data that "
        "`buildkite.shard_list` uploads. With a job link (build URL with "
        "`#<job-id>` fragment), show only that job's items.",
    )
    parser.add_argument(
        "url",
        help="Buildkite build or job URL, e.g. "
        "https://buildkite.com/materialize/test/builds/129455 or "
        "https://buildkite.com/materialize/test/builds/129455#<job-id>",
    )
    parser.add_argument(
        "item",
        nargs="?",
        help="only show items containing this substring, e.g. a test file or "
        "scenario name, to find the job(s) that ran it",
    )
    parser.add_argument(
        "--no-url",
        action="store_true",
        help="do not print a link to each job",
    )
    args = parser.parse_args()

    match = re.fullmatch(
        r"https://buildkite\.com/[^/]+/([^/]+)/builds/(\d+)(?:#(.+))?",
        args.url,
    )
    if not match:
        sys.exit(f"error: unrecognized Buildkite build URL: {args.url}")
    pipeline, build_number, job_id = match.groups()

    build = fetch_build(pipeline, build_number)
    shards = {
        key.removeprefix("Shard for "): value.split(", ")
        for key, value in build.get("meta_data", {}).items()
        if key.startswith("Shard for ")
    }
    if args.item:
        # Accept the Python function name of a workflow too: mzcompose records
        # `workflow_foo_bar` under the name `foo-bar`.
        needles = {args.item}
        if args.item.startswith("workflow_"):
            needles.add(args.item.removeprefix("workflow_").replace("_", "-"))
        shards = {
            label: matching
            for label, items in shards.items()
            if (
                matching := [
                    item for item in items if any(needle in item for needle in needles)
                ]
            )
        }
        if not shards:
            sys.exit(f"error: no shard in build {build_number} ran {args.item!r}")

    if job_id:
        job = next((j for j in build["jobs"] if j.get("id") == job_id), None)
        if job is None:
            sys.exit(f"error: no job {job_id} in build {build_number}")
        items = shards.get(job.get("name"))
        if items is None:
            sys.exit(
                f"error: no shard info recorded for job {job.get('name')!r}, "
                "only sharded jobs upload it"
            )
        print("\n".join(items))
    else:
        if not shards:
            sys.exit(f"error: no shard info recorded in build {build_number}")
        jobs_by_name = {job["name"]: job for job in build["jobs"] if "name" in job}
        for label in sorted(shards, key=natural_sort_key):
            items = ", ".join(shards[label])
            url = jobs_by_name.get(label, {}).get("web_url")
            if args.no_url or not url:
                print(f"{label}: {items}")
            elif sys.stdout.isatty():
                print(f"{hyperlink(url, label)}: {items}")
            else:
                # Piped output gets the plain URL so it stays greppable and
                # scripts can extract the job ID from the `#` fragment.
                print(f"{label}: {items}")
                print(f"  {url}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
