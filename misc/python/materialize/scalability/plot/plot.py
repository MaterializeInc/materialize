# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import math
from enum import Enum
from typing import Any

import numpy as np
from matplotlib.axes import Axes
from matplotlib.figure import SubFigure
from matplotlib.markers import MarkerStyle

from materialize.scalability.df.df_details import DfDetails
from materialize.scalability.df.df_totals import DfTotals
from materialize.scalability.endpoint.endpoints import endpoint_name_to_description

PLOT_MARKER_POINT = MarkerStyle("o")
PLOT_MARKER_SQUARE = MarkerStyle(",")
PLOT_MARKER_HLINE = MarkerStyle("_")

PLOT_COLOR_DARK_BLUE = "darkblue"


class DistributionPlotType(Enum):
    VIOLIN = 1
    BOX = 2


DEFAULT_DISTRIBUTION_PLOT_TYPE = DistributionPlotType.VIOLIN


def plot_tps_per_connections(
    workload_name: str,
    figure: SubFigure,
    df_totals_by_endpoint_name: dict[str, DfTotals],
    baseline_version_name: str | None,
    include_zero_in_y_axis: bool,
    include_workload_in_title: bool = False,
) -> None:
    """This uses a scatter plot to plot the TPS per connections."""
    legend = []
    plot: Axes = figure.subplots(1, 1)
    max_concurrency = 1

    for endpoint_version_name, df_totals in df_totals_by_endpoint_name.items():
        legend.append(endpoint_name_to_description(endpoint_version_name))

        plot.scatter(
            df_totals.get_concurrency_values(),
            df_totals.get_tps_values(),
            label="tps",
            marker=_get_plot_marker(endpoint_version_name, baseline_version_name),
        )

        max_concurrency = max(max_concurrency, df_totals.get_max_concurrency())

    plot.set_ylabel("Transactions Per Second (tps)")
    plot.set_xlabel("Concurrent SQL Connections")

    if include_zero_in_y_axis:
        plot.set_ylim(ymin=0)

    if include_workload_in_title:
        plot.set_title(workload_name)

    plot.legend(legend)


def plot_duration_by_connections_for_workload(
    workload_name: str,
    figure: SubFigure,
    df_details_by_endpoint_name: dict[str, DfDetails],
    include_zero_in_y_axis: bool,
    include_workload_in_title: bool = False,
    plot_type: DistributionPlotType = DEFAULT_DISTRIBUTION_PLOT_TYPE,
) -> None:
    """This uses a boxplot or violin plot for the distribution of the duration."""
    if len(df_details_by_endpoint_name) == 0:
        return

    concurrencies = next(
        iter(df_details_by_endpoint_name.values())
    ).get_unique_concurrency_values()

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
            df_details_of_concurrency = df_details.to_filtered_by_concurrency(
                concurrency
            )

            if not df_details_of_concurrency.has_values():
                continue

            durations.append(df_details_of_concurrency.get_wallclock_values())

            formatted_endpoint_name = (
                endpoint_version_name
                if not use_short_names
                else _shorten_endpoint_version_name(endpoint_version_name)
            )
            legend.append(formatted_endpoint_name)

        _plot_distribution(plot, data=durations, labels=legend, plot_type=plot_type)

        if is_in_first_column:
            plot.set_ylabel("Duration (seconds)")

        if include_zero_in_y_axis:
            plot.set_ylim(ymin=0)

        title = f"{concurrency} connections"
        if include_workload_in_title:
            title = f"{workload_name}, {title}"
        plot.set_title(title)


def plot_duration_by_endpoints_for_workload(
    workload_name: str,
    figure: SubFigure,
    df_details_by_endpoint_name: dict[str, DfDetails],
    include_zero_in_y_axis: bool,
    include_workload_in_title: bool = False,
    plot_type: DistributionPlotType = DEFAULT_DISTRIBUTION_PLOT_TYPE,
) -> None:
    """This uses a boxplot or violin plot for the distribution of the duration."""

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

        concurrencies = df_details.get_unique_concurrency_values()

        legend = []
        durations: list[list[float]] = []

        for concurrency in concurrencies:
            df_details_of_concurrency = df_details.to_filtered_by_concurrency(
                concurrency
            )

            if not df_details_of_concurrency.has_values():
                continue

            durations.append(df_details_of_concurrency.get_wallclock_values())

            legend.append(concurrency)

        _plot_distribution(plot, data=durations, labels=legend, plot_type=plot_type)

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


def _plot_distribution(
    plot: Axes,
    data: list[list[float]],
    labels: list[str],
    plot_type: DistributionPlotType,
) -> None:
    if plot_type == DistributionPlotType.VIOLIN:
        _plot_violinplot(plot, data, labels)
    elif plot_type == DistributionPlotType.BOX:
        _plot_boxplot(plot, data, labels)
    else:
        raise RuntimeError(f"Unexpected plot type: {plot_type}")


def _plot_violinplot(plot: Axes, data: list[list[float]], labels: list[str]) -> None:
    xpos = np.arange(1, len(data) + 1)

    plot.violinplot(data)
    plot.set_xticks(xpos, labels=labels)

    for i, data_col in enumerate(data):
        quartile1, medians, quartile3 = np.percentile(
            data[i],
            [25, 50, 75],
        )
        # plot median line
        plot.scatter(
            xpos[i],
            medians,
            marker=PLOT_MARKER_HLINE,
            color=PLOT_COLOR_DARK_BLUE,
            s=300,
        )
        # plot 25% - 75% area
        plot.vlines(
            xpos[i],
            quartile1,
            quartile3,
            color=PLOT_COLOR_DARK_BLUE,
            linestyle="-",
            lw=5,
        )


def _plot_boxplot(plot: Axes, data: list[list[float]], labels: list[str]) -> None:
    plot.boxplot(data, labels=labels)
