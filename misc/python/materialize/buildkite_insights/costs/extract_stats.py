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
    "c7g.large": 0.0725,
    "c6g.xlarge": 0.1360,
    "c7g.xlarge": 0.1450,
    "c6g.2xlarge": 0.272,
    "c7g.2xlarge": 0.290,
    "c6g.4xlarge": 0.544,
    "c7g.4xlarge": 0.580,
    "c6g.8xlarge": 1.088,
    "c6g.12xlarge": 1.632,
    "c7g.12xlarge": 1.740,
    "c7g.16xlarge": 2.320,
    "m5.4xlarge": 0.768,
    "m5a.8xlarge": 1.376,
    "m6a.8xlarge": 1.382,
    "m7a.8xlarge": 1.855,
    "m6g.4xlarge": 0.616,
    "m6g.8xlarge": 1.232,
    "m7g.8xlarge": 1.306,
    "m6g.12xlarge": 1.848,
    "m7g.12xlarge": 1.958,
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

    data = read_results_from_file(SimpleFilePath("data.json"))

    for build in data:
        pipeline_name = build["pipeline"]["name"]
        created = datetime.fromisoformat(build["created_at"])
        year_month = f"{created.year}-{created.month:02}"
        pipeline_counts[year_month][pipeline_name] += 1

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
                    cost = hetzner_instance_cost[
                        metadata.removeprefix("queue=hetzner-")
                    ]
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
