# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import pandas as pd
from matplotlib.axes._axes import Axes  # type: ignore

from materialize.scalability.df import df_details_cols, df_totals_cols
from materialize.scalability.endpoints import endpoint_name_to_description


def plot_tps_per_connections(
    plot: Axes, df_totals_by_endpoint_name: dict[str, pd.DataFrame]
) -> None:
    legend = []

    for endpoint_name, df_totals in df_totals_by_endpoint_name.items():
        legend.append(endpoint_name_to_description(endpoint_name))

        plot.scatter(
            df_totals[df_totals_cols.CONCURRENCY],
            df_totals[df_totals_cols.TPS],
            label=df_totals_cols.TPS,
        )

    plot.set_ylabel("Transactions Per Second")
    plot.set_xlabel("Concurrent SQL Connections")
    plot.legend(legend)


def plot_latency_per_connections(
    plot: Axes, df_details_by_endpoint_name: dict[str, pd.DataFrame]
) -> None:
    legend = []

    i = 0
    for endpoint_name, df_details in df_details_by_endpoint_name.items():
        legend.append(endpoint_name_to_description(endpoint_name))

        plot.scatter(
            df_details[df_details_cols.CONCURRENCY] + i,
            df_details[df_details_cols.WALLCLOCK],
            alpha=0.25,
        )

        i = i + 1

    plot.set_ylabel("Latency in Seconds")
    plot.set_xlabel("Concurrent SQL Connections")
    plot.legend(legend)
