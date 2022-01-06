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
from typing import List

from scenarios import *

from materialize.feature_benchmark.aggregation import Aggregation, MinAggregation
from materialize.feature_benchmark.benchmark import Benchmark, Report
from materialize.feature_benchmark.comparator import (
    Comparator,
    RelativeThresholdComparator,
)
from materialize.feature_benchmark.executor import Docker
from materialize.feature_benchmark.filter import Filter, NoFilter
from materialize.feature_benchmark.termination import (
    NormalDistributionOverlap,
    ProbForMin,
    TerminationCondition,
)
from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

#
# Global feature benchmark thresholds and termination conditions
#


def make_filter() -> Filter:
    return NoFilter()


def make_termination_conditions() -> List[TerminationCondition]:
    return [
        NormalDistributionOverlap(threshold=0.99),
        ProbForMin(threshold=0.95),
    ]


def make_aggregation() -> Aggregation:
    return MinAggregation()


def make_comparator(name: str) -> Comparator:
    return RelativeThresholdComparator(name, threshold=0.10)


this_image = os.getenv("THIS_IMAGE", None)
this_options = os.getenv("THIS_OPTIONS", None)

other_image = os.getenv("OTHER_IMAGE", None)
other_options = os.getenv("OTHER_OPTIONS", None)

default_timeout = "5m"

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
        default_timeout=default_timeout,
    ),
    "other": Testdrive(
        name="testdrive_other",
        materialized_url=f"postgres://materialize@materialized_other:6875",
        validate_catalog=False,
        default_timeout=default_timeout,
    ),
}

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    *mzs.values(),
    *tds.values(),
]


def run_one_scenario(c: Composition, scenario: Scenario) -> Comparator:
    name = scenario.__name__
    print(f"Now benchmarking {name} ...")
    comparator = make_comparator(name)
    common_seed = round(time.time())

    for mz_id, revision in enumerate(["this", "other"]):

        mz_service_name = mzs[revision].name
        td_service_name = tds[revision].name

        c.start_and_wait_for_tcp(services=[mzs[revision].name])
        c.wait_for_mz(service=mz_service_name)

        executor = Docker(
            composition=c,
            mz_service=mzs[revision],
            td_service=tds[revision],
            seed=common_seed,
        )

        benchmark = Benchmark(
            mz_id=mz_id,
            scenario=scenario,
            executor=executor,
            filter=make_filter(),
            termination_conditions=make_termination_conditions(),
            aggregation=make_aggregation(),
        )

        outcome, iterations = benchmark.run()
        comparator.append(outcome)

        c.kill_services(services=[mz_service_name])
        c.remove_services(services=[mz_service_name, td_service_name])
        c.remove_volumes(volumes=["mzdata"])

    return comparator


def workflow_feature_benchmark(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=["zookeeper", "kafka", "schema-registry"])

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
        comparison = run_one_scenario(c, scenario)
        report.append(comparison)

        if comparison.is_regression():
            has_regressions = True

        report.dump()

    sys.exit(1 if has_regressions else 0)
