# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import math
from typing import Any

import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import SubFigure
from matplotlib.markers import MarkerStyle

from materialize.scalability.df import df_details_cols, df_totals_cols
from materialize.scalability.endpoints import endpoint_name_to_description

PLOT_MARKER_POINT = MarkerStyle("o")
PLOT_MARKER_SQUARE = MarkerStyle(",")


def scatterplot_tps_per_connections(
    workload_name: str,
    figure: SubFigure,
    df_totals_by_endpoint_name: dict[str, pd.DataFrame],
    baseline_version_name: str | None,
    include_zero_in_y_axis: bool,
    include_workload_in_title: bool = False,
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

    if include_workload_in_title:
        plot.set_title(workload_name)

    plot.legend(legend)


def scatterplot_duration_per_connections(
    workload_name: str,
    figure: SubFigure,
    df_details_by_endpoint_name: dict[str, pd.DataFrame],
    baseline_version_name: str | None,
    include_zero_in_y_axis: bool,
    include_workload_in_title: bool = False,
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

    plot.set_ylabel("Duration in Seconds")
    plot.set_xlabel("Concurrent SQL Connections")

    if include_zero_in_y_axis:
        plot.set_ylim(ymin=0)

    if include_workload_in_title:
        plot.set_title(workload_name)

    plot.legend(legend)


def boxplot_duration_by_connections_for_workload(
    workload_name: str,
    figure: SubFigure,
    df_details_by_endpoint_name: dict[str, pd.DataFrame],
    include_zero_in_y_axis: bool,
    include_workload_in_title: bool = False,
) -> None:
    if len(df_details_by_endpoint_name) == 0:
        return

    concurrencies = next(iter(df_details_by_endpoint_name.values()))[
        df_details_cols.CONCURRENCY
    ].unique()

    endpoint_version_names = df_details_by_endpoint_name.keys()
    use_short_names = len(endpoint_version_names) > 2

    num_rows, num_cols = _compute_plot_grid(len(concurrencies), 3)

    subplots = figure.subplots(num_rows, num_cols, sharey=False)

    for concurrency_index, concurrency in enumerate(concurrencies):
        plot, is_in_first_column, is_in_last_row = _get_subplot_in_grid(
            subplots, concurrency_index, num_rows, num_cols
        )

        legend = []
        durations: list[list[float]] = []

        for endpoint_version_name, df_details in df_details_by_endpoint_name.items():
            formatted_endpoint_name = (
                endpoint_version_name
                if not use_short_names
                else _shorten_endpoint_version_name(endpoint_version_name)
            )
            legend.append(formatted_endpoint_name)

            df_details_of_concurrency = df_details.loc[
                df_details[df_details_cols.CONCURRENCY] == concurrency
            ]
            durations_of_concurrency = df_details_of_concurrency[
                df_details_cols.WALLCLOCK
            ]
            durations.append(durations_of_concurrency)

        plot.boxplot(durations, labels=legend)

        if is_in_first_column:
            plot.set_ylabel("Duration (seconds)")

        if include_zero_in_y_axis:
            plot.set_ylim(ymin=0)

        title = f"{concurrency} connections"
        if include_workload_in_title:
            title = f"{workload_name}, {title}"
        plot.set_title(title)


def boxplot_duration_by_endpoints_for_workload(
    workload_name: str,
    figure: SubFigure,
    df_details_by_endpoint_name: dict[str, pd.DataFrame],
    include_zero_in_y_axis: bool,
    include_workload_in_title: bool = False,
) -> None:
    if len(df_details_by_endpoint_name) == 0:
        return

    num_rows, num_cols = _compute_plot_grid(len(df_details_by_endpoint_name.keys()), 1)
    subplots = figure.subplots(num_rows, num_cols, sharey=False)

    for endpoint_index, (endpoint_version_name, df_details) in enumerate(
        df_details_by_endpoint_name.items()
    ):
        plot, is_in_first_column, is_in_last_row = _get_subplot_in_grid(
            subplots, endpoint_index, num_rows, num_cols
        )

        concurrencies = df_details[df_details_cols.CONCURRENCY].unique()

        legend = []
        durations: list[list[float]] = []

        for concurrency in concurrencies:
            legend.append(concurrency)

            df_details_of_concurrency = df_details.loc[
                df_details[df_details_cols.CONCURRENCY] == concurrency
            ]
            durations_of_concurrency = df_details_of_concurrency[
                df_details_cols.WALLCLOCK
            ]
            durations.append(durations_of_concurrency)

        plot.boxplot(durations, labels=legend)

        if is_in_first_column and is_in_last_row:
            plot.set_ylabel("Duration (seconds)")

        if is_in_last_row:
            plot.set_xlabel("Concurrencies")

        if include_zero_in_y_axis:
            plot.set_ylim(ymin=0)

        title = endpoint_version_name
        if include_workload_in_title:
            title = f"{workload_name}, {title}"
        plot.set_title(title)


def _shorten_endpoint_version_name(endpoint_version_name: str) -> str:
    if " " not in endpoint_version_name:
        return endpoint_version_name

    return endpoint_version_name.split(" ")[0]


def _get_plot_marker(
    endpoint_version_name: str, baseline_version_name: str | None
) -> MarkerStyle:
    if (
        baseline_version_name is not None
        and endpoint_version_name == baseline_version_name
    ):
        return PLOT_MARKER_SQUARE

    return PLOT_MARKER_POINT


def _compute_plot_grid(num_subplots: int, max_subplots_per_row: int) -> tuple[int, int]:
    num_rows = math.ceil(num_subplots / max_subplots_per_row)
    num_cols = math.ceil(num_subplots / num_rows)
    return num_rows, num_cols


def _get_subplot_in_grid(
    subplots: Any,
    index: int,
    num_rows: int,
    num_cols: int,
) -> tuple[Axes, bool, bool]:
    use_no_grid = num_rows == 1 and num_cols == 1
    use_single_dimension = (num_rows == 1 and num_cols > 1) or (
        num_cols == 1 and num_rows > 1
    )

    if use_no_grid:
        plot: Axes = subplots
        is_in_first_column = True
        is_in_last_row = True
    elif use_single_dimension:
        plot: Axes = subplots[index]
        is_in_first_column = index == 0 or num_cols == 1
        is_in_last_row = index == (num_rows - 1) or num_rows == 1
    else:
        row = math.floor(index / num_cols)
        column = index % num_cols
        plot: Axes = subplots[row][column]
        is_in_first_column = column == 0
        is_in_last_row = row == (num_rows - 1)

    assert type(plot) == Axes

    return plot, is_in_first_column, is_in_last_row
