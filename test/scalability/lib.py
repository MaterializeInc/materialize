# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

import pandas as pd
from matplotlib import pyplot as plt

from materialize.scalability.df.df_details import DfDetails
from materialize.scalability.df.df_totals import DfTotals
from materialize.scalability.io import paths
from materialize.scalability.plot.plot import (
    plot_duration_by_connections_for_workload,
    plot_duration_by_endpoints_for_workload,
    plot_tps_per_connections,
)


def plotit(workload_name: str, include_zero_in_y_axis: bool = True) -> None:
    fig = plt.figure(layout="constrained", figsize=(16, 22))
    (
        tps_figure,
        duration_per_connections_figure,
        duration_per_endpoints_figure,
    ) = fig.subfigures(3, 1)

    df_totals_by_endpoint_name, df_details_by_endpoint_name = load_data_from_filesystem(
        workload_name
    )

    plot_tps_per_connections(
        workload_name,
        tps_figure,
        df_totals_by_endpoint_name,
        baseline_version_name=None,
        include_zero_in_y_axis=include_zero_in_y_axis,
    )

    plot_duration_by_connections_for_workload(
        workload_name,
        duration_per_connections_figure,
        df_details_by_endpoint_name,
        include_zero_in_y_axis=include_zero_in_y_axis,
    )
    plot_duration_by_endpoints_for_workload(
        workload_name,
        duration_per_endpoints_figure,
        df_details_by_endpoint_name,
        include_zero_in_y_axis=include_zero_in_y_axis,
    )


def load_data_from_filesystem(
    workload_name: str,
) -> tuple[dict[str, DfTotals], dict[str, DfDetails]]:
    endpoint_names = paths.get_endpoint_names_from_results_dir()
    endpoint_names.sort()

    df_totals_by_endpoint_name = dict()
    df_details_by_endpoint_name = dict()

    for i, endpoint_name in enumerate(endpoint_names):
        totals_data_path = paths.df_totals_csv(endpoint_name, workload_name)
        details_data_path = paths.df_details_csv(endpoint_name, workload_name)

        if not os.path.exists(totals_data_path):
            print(
                f"Skipping {workload_name} for endpoint {endpoint_name} (data not present)"
            )
            continue

        assert os.path.exists(details_data_path)

        df_totals_by_endpoint_name[endpoint_name] = DfTotals(
            pd.read_csv(totals_data_path)
        )
        df_details_by_endpoint_name[endpoint_name] = DfDetails(
            pd.read_csv(details_data_path)
        )

    return df_totals_by_endpoint_name, df_details_by_endpoint_name
