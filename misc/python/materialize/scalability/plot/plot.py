# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import math
from typing import cast

import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import SubFigure
from matplotlib.markers import MarkerStyle

from materialize.scalability.df import df_details_cols, df_totals_cols
from materialize.scalability.endpoints import endpoint_name_to_description

PLOT_MARKER_POINT = MarkerStyle("o")
PLOT_MARKER_SQUARE = MarkerStyle(",")


def scatterplot_tps_per_connections(
    figure: SubFigure,
    df_totals_by_endpoint_name: dict[str, pd.DataFrame],
    baseline_version_name: str | None,
    include_zero_in_y_axis: bool,
) -> None:
    legend = []
    plot: Axes = figure.subplots(1, 1)
    max_concurrency = 1

    for endpoint_version_name, df_totals in df_totals_by_endpoint_name.items():
        legend.append(endpoint_name_to_description(endpoint_version_name))

        plot.scatter(
            df_totals[df_totals_cols.CONCURRENCY],
            df_totals[df_totals_cols.TPS],
            label=df_totals_cols.TPS,
            marker=_get_plot_marker(endpoint_version_name, baseline_version_name),
        )

        max_concurrency = max(
            max_concurrency, df_totals[df_totals_cols.CONCURRENCY].max()
        )

    plot.set_ylabel("Transactions Per Second (tps)")
    plot.set_xlabel("Concurrent SQL Connections")

    if include_zero_in_y_axis:
        plot.set_ylim(ymin=0)

    plot.legend(legend)


def scatterplot_latency_per_connections(
    figure: SubFigure,
    df_details_by_endpoint_name: dict[str, pd.DataFrame],
    baseline_version_name: str | None,
    include_zero_in_y_axis: bool,
) -> None:
    legend = []
    plot: Axes = figure.subplots(1, 1)

    i = 0
    for endpoint_version_name, df_details in df_details_by_endpoint_name.items():
        endpoint_offset = i / 40.0
        legend.append(endpoint_name_to_description(endpoint_version_name))

        plot.scatter(
            df_details[df_details_cols.CONCURRENCY] + endpoint_offset,
            df_details[df_details_cols.WALLCLOCK],
            alpha=0.25,
            marker=_get_plot_marker(endpoint_version_name, baseline_version_name),
        )

        i = i + 1

    plot.set_ylabel("Latency in Seconds")
    plot.set_xlabel("Concurrent SQL Connections")

    if include_zero_in_y_axis:
        plot.set_ylim(ymin=0)

    plot.legend(legend)


def boxplot_latency_per_connections(
    figure: SubFigure,
    df_details_by_endpoint_name: dict[str, pd.DataFrame],
    include_zero_in_y_axis: bool,
) -> None:
    if len(df_details_by_endpoint_name) == 0:
        return

    concurrencies = next(iter(df_details_by_endpoint_name.values()))[
        df_details_cols.CONCURRENCY
    ].unique()

    endpoint_names = df_details_by_endpoint_name.keys()
    use_short_names = len(endpoint_names) > 2

    num_rows = math.ceil(len(concurrencies) / 3)
    num_cols = math.ceil(len(concurrencies) / num_rows)
    use_no_grid = num_rows == 1 and num_cols == 1
    use_single_row = num_rows == 1 and num_cols > 1

    subplots = figure.subplots(num_rows, num_cols, sharey=False)

    for concurrency_index, concurrency in enumerate(concurrencies):
        if use_no_grid:
            plot: Axes = cast(Axes, subplots)
            is_in_first_column = True
        elif use_single_row:
            plot: Axes = subplots[concurrency_index]
            is_in_first_column = concurrency_index == 0
        else:
            row = math.floor(concurrency_index / num_cols)
            column = concurrency_index % num_cols
            plot: Axes = cast(list[Axes], subplots[row])[column]
            is_in_first_column = column == 0

        assert type(plot) == Axes
        legend = []

        wallclocks_of_endpoints: list[list[float]] = []

        for endpoint_name, df_details in df_details_by_endpoint_name.items():
            formatted_endpoint_name = (
                endpoint_name
                if not use_short_names
                else _shorten_endpoint_name(endpoint_name)
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

        if is_in_first_column:
            plot.set_ylabel("Latency in Seconds")

        if include_zero_in_y_axis:
            plot.set_ylim(ymin=0)

        plot.set_title(f"# connections: {concurrency}")


def _shorten_endpoint_name(endpoint_name: str) -> str:
    if " " not in endpoint_name:
        return endpoint_name

    return endpoint_name.split(" ")[0]


def _get_plot_marker(
    endpoint_version_name: str, baseline_version_name: str | None
) -> MarkerStyle:
    if (
        baseline_version_name is not None
        and endpoint_version_name == baseline_version_name
    ):
        return PLOT_MARKER_SQUARE

    return PLOT_MARKER_POINT
