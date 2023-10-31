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

from materialize.scalability.df import paths
from materialize.scalability.plot.plot import (
    boxplot_latency_per_connections,
    plot_latency_per_connections,
    plot_tps_per_connections,
)

USE_BOXPLOT = True


def plotit(workload_name: str) -> None:
    fig = plt.figure(layout="constrained", figsize=(16, 10))
    (tps_figure, latency_figure) = fig.subfigures(2, 1)

    df_totals_by_endpoint_name, df_details_by_endpoint_name = load_data_from_filesystem(
        workload_name
    )

    plot_tps_per_connections(tps_figure, df_totals_by_endpoint_name)

    if USE_BOXPLOT:
        boxplot_latency_per_connections(latency_figure, df_details_by_endpoint_name)
    else:
        plot_latency_per_connections(latency_figure, df_details_by_endpoint_name)


def load_data_from_filesystem(
    workload_name: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    endpoint_names = paths.get_endpoint_names_from_results_dir()

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

        df_totals_by_endpoint_name[endpoint_name] = pd.read_csv(totals_data_path)
        df_details_by_endpoint_name[endpoint_name] = pd.read_csv(details_data_path)

    return df_totals_by_endpoint_name, df_details_by_endpoint_name
