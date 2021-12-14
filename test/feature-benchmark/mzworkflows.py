# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import sys
import time

import numpy as np
from scenarios import *
from scipy import stats

from materialize.feature_benchmark.aggregation import MinAggregation
from materialize.feature_benchmark.benchmark import Benchmark, Report
from materialize.feature_benchmark.comparator import RelativeThresholdComparator
from materialize.feature_benchmark.executor import Docker
from materialize.feature_benchmark.filter import NoFilter, RemoveOutliers
from materialize.feature_benchmark.termination import (
    NormalDistributionOverlap,
    ProbForMin,
)
from materialize.mzcompose import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Workflow,
    Zookeeper,
)

#
# Global feature benchmark thresholds and termination conditions
#

filter_class = NoFilter

termination_conditions = [
    {"class": NormalDistributionOverlap, "threshold": 0.99},
    {"class": ProbForMin, "threshold": 0.95},
]

aggregation_class = MinAggregation

comparator_class = {"class": RelativeThresholdComparator, "threshold": 0.10}

confluents = [Zookeeper(), Kafka(), SchemaRegistry()]

this_image = os.getenv("THIS_IMAGE", None)
this_options = os.getenv("THIS_OPTIONS", None)

other_image = os.getenv("OTHER_IMAGE", None)
other_options = os.getenv("OTHER_OPTIONS", None)

default_timeout = 120

mzs = {
    "this": Materialized(
        name="materialized_this", image=this_image, options=this_options
    ),
    "other": Materialized(
        name="materialized_other", image=other_image, options=other_options
    ),
}


tds = {
    "this": Testdrive(
        name="testdrive_this",
        materialized_url=f"postgres://materialize@materialized_this:6875",
        validate_catalog=False,
        default_timeout=120,
    ),
    "other": Testdrive(
        name="testdrive_other",
        materialized_url=f"postgres://materialize@materialized_other:6875",
        validate_catalog=False,
        default_timeout=120,
    ),
}

services = confluents + list(mzs.values()) + list(tds.values())


def run_one_scenario(w: Workflow, scenario: Scenario):
    name = scenario.__name__
    print(f"Now benchmarking {name} ...")
    comparator = comparator_class["class"](
        name=name, threshold=comparator_class["threshold"]
    )
    common_seed = round(time.time())

    for mz_id, revision in enumerate(["this", "other"]):

        mz_service_name = mzs[revision].name
        td_service_name = tds[revision].name

        w.start_and_wait_for_tcp(services=[mzs[revision]])
        w.wait_for_mz(service=mz_service_name)

        executor = Docker(
            workflow=w,
            mz_service=mzs[revision],
            td_service=tds[revision],
            seed=common_seed,
        )

        benchmark = Benchmark(
            mz_id=mz_id,
            scenario=scenario,
            executor=executor,
            filter=filter_class(),
            termination_conditions=[
                c["class"](threshold=c["threshold"]) for c in termination_conditions
            ],
            aggregation=aggregation_class(),
        )

        outcome, iterations = benchmark.run()
        comparator.append(outcome)

        w.kill_services(services=[mz_service_name])
        w.remove_services(services=[mz_service_name, td_service_name])
        w.remove_volumes(volumes=["mzdata"])

    return comparator


def workflow_feature_benchmark(w: Workflow):
    w.start_and_wait_for_tcp(services=confluents)

    report = Report()
    has_regressions = False

    root_scenario = globals()[os.getenv("FB_SCENARIO", "Scenario")]
    scenarios = []

    if root_scenario.__subclasses__():
        for scenario in root_scenario.__subclasses__():
            has_children = False
            for s in scenario.__subclasses__():
                has_children = True
                scenarios.append(s)

            if not has_children:
                scenarios.append(scenario)
    else:
        scenarios.append(root_scenario)

    print(
        f"will run the following scenarios: {', '.join([scenario.__name__ for scenario in scenarios])}"
    )

    for scenario in scenarios:
        comparison = run_one_scenario(w, scenario)
        report.append(comparison)

        if comparison.is_regression():
            has_regressions = True

        report.dump()

    sys.exit(1 if has_regressions else 0)
