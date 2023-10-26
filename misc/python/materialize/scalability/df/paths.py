# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path

from materialize import MZ_ROOT
from materialize.scalability.workload import Workload

RESULTS_DIR = MZ_ROOT / "test" / "scalability" / "results"


def endpoint_dir(endpoint_name: str) -> Path:
    return RESULTS_DIR / endpoint_name


def df_totals_csv(endpoint_name: str, workload: Workload) -> Path:
    return RESULTS_DIR / endpoint_name / f"{type(workload).__name__}.csv"


def workloads_csv() -> Path:
    return RESULTS_DIR / "workloads.csv"


def df_details_csv(endpoint_name: str, workload: Workload) -> Path:
    return RESULTS_DIR / endpoint_name / f"{type(workload).__name__}_details.csv"


def regressions_csv_name() -> str:
    return "regressions.csv"


def regressions_csv() -> Path:
    return RESULTS_DIR / regressions_csv_name()


def results_csv_rel_path(endpoint_name: str) -> Path:
    return Path(endpoint_name) / "results.csv"


def results_csv(endpoint_name: str) -> Path:
    return RESULTS_DIR / results_csv_rel_path(endpoint_name)
