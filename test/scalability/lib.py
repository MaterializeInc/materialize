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
from matplotlib import pyplot as plt  # type: ignore

from materialize.scalability.df import df_details_cols, df_totals_cols, paths
from materialize.scalability.endpoints import endpoint_name_to_description


def plotit(workload_name: str) -> None:
    endpoint_names = paths.get_endpoint_names_from_results_dir()
    legend = []
    plt.rcParams["figure.figsize"] = (16, 10)
    fig, (summary_subplot, details_subplot) = plt.subplots(2, 1)
    for i, endpoint_name in enumerate(endpoint_names):
        totals_data_path = paths.df_totals_csv(endpoint_name, workload_name)
        details_data_path = paths.df_details_csv(endpoint_name, workload_name)

        if not os.path.exists(totals_data_path):
            print(
                f"Skipping {workload_name} for endpoint {endpoint_name} (data not present)"
            )
            continue

        assert os.path.exists(details_data_path)

        legend.append(endpoint_name_to_description(endpoint_name))

        df = pd.read_csv(totals_data_path)
        summary_subplot.scatter(
            df[df_totals_cols.CONCURRENCY],
            df[df_totals_cols.TPS],
            label=df_totals_cols.TPS,
        )

        df_details = pd.read_csv(details_data_path)
        details_subplot.scatter(
            df_details[df_details_cols.CONCURRENCY] + i,
            df_details[df_details_cols.WALLCLOCK],
            alpha=0.25,
        )

    summary_subplot.set_ylabel("Transactions Per Second")
    summary_subplot.set_xlabel("Concurrent SQL Connections")
    summary_subplot.legend(legend)

    details_subplot.set_ylabel("Latency in Seconds")
    details_subplot.set_xlabel("Concurrent SQL Connections")
    details_subplot.legend(legend)
