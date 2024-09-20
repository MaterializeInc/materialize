# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from statistics import median

from materialize.feature_benchmark.measurement import MeasurementType
from materialize.feature_benchmark.report import Report


class BenchmarkResultSelectorBase:

    def choose_report_per_scenario(
        self,
        reports: list[Report],
    ) -> dict[str, Report]:
        assert len(reports) > 0, "No reports"
        all_scenario_names = reports[0].get_scenario_names()

        result = dict()
        for scenario_name in all_scenario_names:
            result[scenario_name] = self.choose_report_of_single_scenario(
                reports, scenario_name
            )

        return result

    def choose_report_of_single_scenario(
        self, reports: list[Report], scenario_name: str
    ) -> Report:
        selectable_reports_by_wallclock_value: dict[float, list[Report]] = dict()
        available_wallclock_values = []

        for report in reports:
            scenario_result = report.get_scenario_result_by_name(scenario_name)

            metric_value = scenario_result.get_metric_by_measurement_type(
                MeasurementType.WALLCLOCK
            )
            if metric_value is None:
                continue

            wallclock_value = metric_value.this()
            if wallclock_value is None:
                continue

            reports_of_wallclock_value = selectable_reports_by_wallclock_value.get(
                wallclock_value, []
            )
            selectable_reports_by_wallclock_value[wallclock_value] = (
                reports_of_wallclock_value
            )
            reports_of_wallclock_value.append(report)

            # store wallclock values separately not to lose identical values
            available_wallclock_values.append(wallclock_value)

        if len(selectable_reports_by_wallclock_value) == 0:
            # pick the first report in this case
            return reports[0]

        return self._select_report_of_single_scenario(
            scenario_name,
            selectable_reports_by_wallclock_value,
            available_wallclock_values,
        )

    def _select_report_of_single_scenario(
        self,
        scenario_name: str,
        selectable_reports_by_wallclock_value: dict[float, list[Report]],
        available_wallclock_values: list[float],
    ) -> Report:
        raise NotImplementedError


class MedianBenchmarkResultSelector(BenchmarkResultSelectorBase):
    """Chooses the report with the median wallclock value for each scenario"""

    def _select_report_of_single_scenario(
        self,
        scenario_name: str,
        selectable_reports_by_wallclock_value: dict[float, list[Report]],
        available_wallclock_values: list[float],
    ) -> Report:
        if len(available_wallclock_values) % 2 == 0:
            # in case of an even number of selectable reports, add zero to the values to get an existing value when computing the median
            available_wallclock_values.append(0)

        median_wallclock_value = median(available_wallclock_values)
        assert (
            median_wallclock_value in selectable_reports_by_wallclock_value.keys()
        ), f"Chosen median is {median_wallclock_value} but available values are {available_wallclock_values}"
        selected_report = selectable_reports_by_wallclock_value[median_wallclock_value][
            0
        ]

        return selected_report


class BestBenchmarkResultSelector(BenchmarkResultSelectorBase):
    """Chooses the report with the minimum wallclock value for each scenario and favors reports without regressions"""

    def _select_report_of_single_scenario(
        self,
        scenario_name: str,
        selectable_reports_by_wallclock_value: dict[float, list[Report]],
        available_wallclock_values: list[float],
    ) -> Report:
        best_report_with_regression: Report | None = None

        for wallclock_value in sorted(available_wallclock_values):
            reports = selectable_reports_by_wallclock_value[wallclock_value]
            for report in reports:
                if not report.has_scenario_regression(scenario_name):
                    # this is the best report without regression (based on wallclock values)
                    return report
                elif best_report_with_regression is None:
                    best_report_with_regression = report

        assert best_report_with_regression is not None, "No report found"
        return best_report_with_regression


def get_discarded_reports_per_scenario(
    reports: list[Report], selected_report_by_scenario_name: dict[str, Report]
) -> dict[str, list[Report]]:
    assert len(reports) > 0, "No reports"
    all_scenario_names = reports[0].get_scenario_names()

    result = dict()
    for scenario_name in all_scenario_names:
        selected_report = selected_report_by_scenario_name[scenario_name]
        discarded_reports = []

        for report in reports:
            if report.cycle_number != selected_report.cycle_number:
                discarded_reports.append(report)

        result[scenario_name] = discarded_reports

    return result
