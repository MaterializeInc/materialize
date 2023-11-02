# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import math

import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import SubFigure

from materialize.scalability.df import df_details_cols, df_totals_cols
from materialize.scalability.endpoints import endpoint_name_to_description


def scatterplot_tps_per_connections(
    figure: SubFigure, df_totals_by_endpoint_name: dict[str, pd.DataFrame]
) -> None:
    legend = []
    plot: Axes = figure.subplots(1, 1)
    max_concurrency = 1

    for endpoint_name, df_totals in df_totals_by_endpoint_name.items():
        legend.append(endpoint_name_to_description(endpoint_name))

        plot.scatter(
            df_totals[df_totals_cols.CONCURRENCY],
            df_totals[df_totals_cols.TPS],
            label=df_totals_cols.TPS,
        )

        max_concurrency = max(
            max_concurrency, df_totals[df_totals_cols.CONCURRENCY].max()
        )

    plot.set_ylabel("Transactions Per Second (tps)")
    plot.set_xlabel("Concurrent SQL Connections")
    plot.legend(legend)


def scatterplot_latency_per_connections(
    figure: SubFigure, df_details_by_endpoint_name: dict[str, pd.DataFrame]
) -> None:
    legend = []
    plot: Axes = figure.subplots(1, 1)

    i = 0
    for endpoint_name, df_details in df_details_by_endpoint_name.items():
        endpoint_offset = i / 40.0
        legend.append(endpoint_name_to_description(endpoint_name))

        plot.scatter(
            df_details[df_details_cols.CONCURRENCY] + endpoint_offset,
            df_details[df_details_cols.WALLCLOCK],
            alpha=0.25,
        )

        i = i + 1

    plot.set_ylabel("Latency in Seconds")
    plot.set_xlabel("Concurrent SQL Connections")
    plot.legend(legend)


def boxplot_latency_per_connections(
    figure: SubFigure, df_details_by_endpoint_name: dict[str, pd.DataFrame]
) -> None:
    if len(df_details_by_endpoint_name) == 0:
        return

    concurrencies = next(iter(df_details_by_endpoint_name.values()))[
        df_details_cols.CONCURRENCY
    ].unique()

    endpoint_names = df_details_by_endpoint_name.keys()
    use_short_names = len(endpoint_names) > 2

    fig_rows = 1 if len(concurrencies) < 5 else 2
    fig_cols = math.ceil(len(concurrencies) / fig_rows)
    subplots = figure.subplots(fig_rows, fig_cols, sharey=False)

    for concurrency_index, concurrency in enumerate(concurrencies):
        plot: Axes = subplots[concurrency_index]
        legend = []

        wallclocks_of_endpoints: list[list[float]] = []

        for endpoint_name, df_details in df_details_by_endpoint_name.items():
            formatted_endpoint_name = (
                endpoint_name
                if not use_short_names
                else shorten_endpoint_name(endpoint_name)
            )
            legend.append(formatted_endpoint_name)

            df_details_of_concurrency = df_details.loc[
                df_details[df_details_cols.CONCURRENCY] == concurrency
            ]
            wallclocks_of_concurrency = df_details_of_concurrency[
                df_details_cols.WALLCLOCK
            ]
            wallclocks_of_endpoints.append(wallclocks_of_concurrency)

        plot.boxplot(wallclocks_of_endpoints, labels=legend)

        if concurrency_index == 0:
            plot.set_ylabel("Latency in Seconds")

        plot.set_title(f"# connections: {concurrency}")


def shorten_endpoint_name(endpoint_name: str) -> str:
    if " " not in endpoint_name:
        return endpoint_name

    return endpoint_name.split(" ")[0]
