# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from materialize.buildkite_insights.util.data_io import (
    SimpleFilePath,
    read_results_from_file,
)

# https://instances.vantage.sh/aws/ec2
aws_instance_cost = {
    "c5.2xlarge": 0.340,
    "c5.12xlarge": 2.040,
    "c5a.2xlarge": 0.308,
    "c5a.8xlarge": 1.232,
    "c6a.large": 0.0765,
    "c6a.xlarge": 0.153,
    "c6a.2xlarge": 0.306,
    "c6a.4xlarge": 0.612,
    "c6a.8xlarge": 1.224,
    "c6a.12xlarge": 1.836,
    "c7a.large": 0.1026,
    "c7a.xlarge": 0.2053,
    "c7a.2xlarge": 0.4106,
    "c7a.4xlarge": 0.8211,
    "c7a.8xlarge": 1.642,
    "c7a.12xlarge": 2.463,
    "c7g.large": 0.0725,
    "c8g.large": 0.0798,
    "c6g.xlarge": 0.1360,
    "c7g.xlarge": 0.1450,
    "c8g.xlarge": 0.1595,
    "c6g.2xlarge": 0.272,
    "c7g.2xlarge": 0.290,
    "c8g.2xlarge": 0.319,
    "c6g.4xlarge": 0.544,
    "c7g.4xlarge": 0.580,
    "c8g.4xlarge": 0.6381,
    "c6g.8xlarge": 1.088,
    "c6g.12xlarge": 1.632,
    "c7g.12xlarge": 1.740,
    "c8g.12xlarge": 1.914,
    "c7g.16xlarge": 2.320,
    "c8g.16xlarge": 2.552,
    "m5.4xlarge": 0.768,
    "m5a.8xlarge": 1.376,
    "m6a.8xlarge": 1.382,
    "m7a.8xlarge": 1.855,
    "m6a.12xlarge": 2.074,
    "m7a.12xlarge": 2.782,
    "m6a.16xlarge": 2.7648,
    "m7a.16xlarge": 3.7094,
    "m6a.24xlarge": 4.1472,
    "m7a.24xlarge": 5.5642,
    "m6a.32xlarge": 5.5296,
    "m7a.32xlarge": 7.4189,
    "m6a.48xlarge": 8.2944,
    "m7a.48xlarge": 11.1283,
    "m6g.4xlarge": 0.616,
    "m6g.8xlarge": 1.232,
    "m7g.8xlarge": 1.306,
    "m6g.12xlarge": 1.848,
    "m7g.12xlarge": 1.958,
    "m7g.16xlarge": 2.6112,
    "m8g.12xlarge": 2.154,
    "m8g.16xlarge": 2.8723,
    "m8g.24xlarge": 4.3085,
    "m8g.48xlarge": 8.617,
    "m6i.4xlarge": 0.768,
    "m6i.12xlarge": 2.304,
    "m7i.8xlarge": 1.613,
    "r7g.8xlarge": 1.714,
    "r8g.8xlarge": 1.885,
}

# https://www.hetzner.com/cloud/
hetzner_instance_cost = {
    "aarch64-2cpu-4gb": 0.0059,
    "aarch64-4cpu-8gb": 0.0101,
    "aarch64-8cpu-16gb": 0.0202,
    "aarch64-16cpu-32gb": 0.0395,
    "x86-64-2cpu-4gb": 0.0060,
    "x86-64-4cpu-8gb": 0.0113,
    "x86-64-8cpu-16gb": 0.0273,
    "x86-64-16cpu-32gb": 0.0540,
    "x86-64-dedi-2cpu-8gb": 0.0200,
    "x86-64-dedi-4cpu-16gb": 0.0392,
    "x86-64-dedi-8cpu-32gb": 0.0777,
    "x86-64-dedi-16cpu-64gb": 0.1546,
    "x86-64-dedi-32cpu-128gb": 0.3085,
    "x86-64-dedi-48cpu-192gb": 0.4623,
    "x86-64": 0,  # local experiments
}


@dataclass
class Failures:
    failures: int
    total: int


def main() -> None:
    job_costs = defaultdict(lambda: defaultdict(float))
    pipeline_costs = defaultdict(lambda: defaultdict(float))
    job_counts = defaultdict(lambda: defaultdict(int))
    pipeline_counts = defaultdict(lambda: defaultdict(int))
    job_failures = defaultdict(
        lambda: defaultdict(lambda: Failures(failures=0, total=0))
    )
    job_to_pipeline = {}
    build_durations = defaultdict(lambda: defaultdict(float))
    build_counts = defaultdict(lambda: defaultdict(int))

    data = read_results_from_file(SimpleFilePath("data.json"))

    for build in data:
        pipeline_name = build["pipeline"]["name"]
        created = datetime.fromisoformat(build["created_at"])
        year_month = f"{created.year}-{created.month:02}"
        pipeline_counts[year_month][pipeline_name] += 1

        if build["started_at"] and build["finished_at"]:
            if not build["state"] in ("passed", "failed"):
                continue
            pipeline = build["pipeline"]["slug"]
            if pipeline in ("test", "nightly", "release-qualification"):
                if "CI_SANITIZER" in build["env"]:
                    continue
                if "CI_COVERAGE_ENABLED" in build["env"]:
                    continue
                if any(job.get("retries_count") for job in build["jobs"]):
                    continue
                year_month_day = f"{created.year}-{created.month:02}-{created.day:02}"
                start = datetime.fromisoformat(build["started_at"])
                finished = datetime.fromisoformat(build["finished_at"])
                duration = (finished - start).total_seconds()
                is_main = build["branch"] == "main"
                with_build = any(
                    job.get("step_key")
                    in (
                        "build-x86_64",
                        "build-aarch64",
                        "build-x86_64-lto",
                        "build-aarch64-lto",
                    )
                    and job["state"] == "passed"
                    for job in build["jobs"]
                )
                build_durations[(pipeline, is_main, with_build)][
                    year_month_day
                ] += duration
                build_counts[(pipeline, is_main, with_build)][year_month_day] += 1

        for job in build["jobs"]:
            if (
                not job.get("agent")
                or not job.get("started_at")
                or not job.get("finished_at")
            ):
                continue

            job_name = job["name"] or "None"

            if not job_name in job_to_pipeline:
                job_to_pipeline[job_name] = pipeline_name

            for metadata in job["agent"]["meta_data"]:
                if metadata.startswith("aws:instance-type="):
                    cost = aws_instance_cost[
                        metadata.removeprefix("aws:instance-type=")
                    ]
                    break
                if metadata.startswith("queue=hetzner-"):
                    name = metadata.removeprefix("queue=hetzner-")
                    if "gb-" in name:
                        name = name[: name.index("gb-") + 2]
                    cost = hetzner_instance_cost[name]
                    break
            else:
                # Can't calculate cost for mac-aarch64
                cost = 0

            start = datetime.fromisoformat(job["started_at"])
            finished = datetime.fromisoformat(job["finished_at"])
            duration = (finished - start).total_seconds()

            total_cost = cost * duration / 3600

            job_costs[year_month][job_name] += total_cost
            pipeline_costs[year_month][pipeline_name] += total_cost
            job_counts[year_month][job_name] += 1
            if job["state"] in ("failed", "broken"):
                job_failures[year_month][job_name].failures += 1
            if job["state"] in ("passed", "failed", "broken"):
                job_failures[year_month][job_name].total += 1

    def print_stats_day(
        name,
        data,
        print_fn=lambda x, key: "" if key not in x else f"{x.get(key, 0):.2f}",
    ):
        keys = set()
        for ps in data.values():
            for p in ps.keys():
                keys.add(p)
        keys = sorted(keys)

        year_month_days = sorted(data.keys())

        additional_keys = [name]
        print(
            ",".join(
                additional_keys
                + [
                    f"{ymd} ({'main' if is_main else 'PR'} {'with build' if with_build else 'without build'})"
                    for ymd, is_main, with_build in year_month_days
                ]
            )
        )

        for key in keys:
            additional_values = [f'"{key}"']
            print(
                ",".join(
                    additional_values
                    + [print_fn(data[day], key) for day in year_month_days]
                )
            )

    def print_stats(
        name,
        data,
        include_pipeline=False,
        print_fn=lambda x, key: f"{x.get(key, 0):.2f}",
    ):
        keys = set()
        for ps in data.values():
            for p in ps.keys():
                keys.add(p)
        keys = sorted(keys)

        year_months = sorted(data.keys(), reverse=True)

        additional_keys = [name] + (["Pipeline"] if include_pipeline else [])
        print(",".join(additional_keys + year_months))

        for key in keys:
            additional_values = [f'"{key}"'] + (
                [f'"{job_to_pipeline[key]}"'] if include_pipeline else []
            )
            print(
                ",".join(
                    additional_values
                    + [print_fn(data[year_month], key) for year_month in year_months]
                )
            )

    job_cost_per_run = {
        key: {key2: value2 / job_counts[key][key2] for key2, value2 in value.items()}
        for key, value in job_costs.items()
    }
    pipeline_cost_per_run = {
        key: {
            key2: value2 / pipeline_counts[key][key2] for key2, value2 in value.items()
        }
        for key, value in pipeline_costs.items()
    }

    build_durations_per_run = {
        key: {key2: value2 / build_counts[key][key2] for key2, value2 in value.items()}
        for key, value in build_durations.items()
    }

    print_stats_day("Runtime [s/run]", build_durations_per_run)
    print()
    print_stats("Pipeline [$]", pipeline_costs)
    print()
    print_stats("Pipeline [$/run]", pipeline_cost_per_run)
    print()
    print_stats("Job [$]", job_costs, include_pipeline=True)
    print()
    print_stats("Job [$/run]", job_cost_per_run, include_pipeline=True)
    print()
    print_stats(
        "Job [% failed]",
        job_failures,
        include_pipeline=True,
        print_fn=lambda x, key: (
            f"{x[key].failures * 100 / x[key].total:.2f}" if x[key].total else ""
        ),
    )


if __name__ == "__main__":
    main()
