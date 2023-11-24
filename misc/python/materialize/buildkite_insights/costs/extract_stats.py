#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from collections import defaultdict
from datetime import datetime

from materialize.buildkite_insights.util.io import read_results_from_file

# https://instances.vantage.sh/aws/ec2
instance_cost = {
    "c5.2xlarge": 0.3400,
    "c5.12xlarge": 2.0400,
    "c5a.2xlarge": 0.3080,
    "c5a.8xlarge": 1.2320,
    "c6a.2xlarge": 0.3060,
    "c6a.8xlarge": 1.2240,
    "c6a.12xlarge": 1.8360,
    "c6g.2xlarge": 0.2720,
    "c6g.8xlarge": 1.0880,
    "m5.4xlarge": 0.7680,
    "m5a.8xlarge": 1.3760,
    "m6a.8xlarge": 1.3820,
    "m6g.4xlarge": 0.6160,
    "m6g.8xlarge": 1.2320,
    "m6i.4xlarge": 0.7680,
    "m6i.12xlarge": 2.3040,
}


def main() -> None:
    job_costs = defaultdict(lambda: defaultdict(float))
    pipeline_costs = defaultdict(lambda: defaultdict(float))
    job_counts = defaultdict(lambda: defaultdict(int))
    pipeline_counts = defaultdict(lambda: defaultdict(int))
    job_to_pipeline = {}

    data = read_results_from_file("data.json")

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
                    cost = instance_cost[metadata.removeprefix("aws:instance-type=")]
                    break
            else:
                raise ValueError("jo instance type found")

            start = datetime.fromisoformat(job["started_at"])
            finished = datetime.fromisoformat(job["finished_at"])
            duration = (finished - start).total_seconds()

            total_cost = cost * duration / 3600

            job_costs[year_month][job_name] += total_cost
            pipeline_costs[year_month][pipeline_name] += total_cost
            job_counts[year_month][job_name] += 1

    def print_stats(name, data, include_pipeline=False):
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
                    + [
                        f"{data[year_month].get(key, 0):.2f}"
                        for year_month in year_months
                    ]
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


if __name__ == "__main__":
    main()
