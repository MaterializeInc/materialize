# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
from pathlib import Path

from materialize import MZ_ROOT

RESULTS_DIR = MZ_ROOT / "test" / "scalability" / "results"


def endpoint_dir(endpoint_name: str) -> Path:
    return RESULTS_DIR / endpoint_name


def df_totals_csv(endpoint_name: str, workload_name: str) -> Path:
    return RESULTS_DIR / endpoint_name / f"{workload_name}.csv"


def workloads_csv() -> Path:
    return RESULTS_DIR / "workloads.csv"


def df_details_csv(endpoint_name: str, workload_name: str) -> Path:
    return RESULTS_DIR / endpoint_name / f"{workload_name}_details.csv"


def regressions_csv() -> Path:
    return RESULTS_DIR / "regressions.csv"


def significant_improvements_csv() -> Path:
    return RESULTS_DIR / "improvements.csv"


def results_csv(endpoint_name: str) -> Path:
    return RESULTS_DIR / Path(endpoint_name) / "results.csv"


def plot_dir() -> Path:
    return RESULTS_DIR / "plots"


def plot_png(plot_type: str, workload_name: str) -> Path:
    return plot_dir() / f"{plot_type}_{workload_name}.png"


def get_endpoint_names_from_results_dir() -> list[str]:
    directories = next(os.walk(RESULTS_DIR))[1]
    endpoints = [entry for entry in directories if not entry.startswith(".")]
    return endpoints
