# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass
from typing import TypeVar

from materialize.scalability.df.df_details import DfDetails
from materialize.scalability.df.df_totals import DfTotals, concat_df_totals
from materialize.scalability.result.comparison_outcome import ComparisonOutcome
from materialize.scalability.result.workload_result import WorkloadResult
from materialize.scalability.workload.workload import Workload
from materialize.scalability.workload.workload_markers import WorkloadMarker
from materialize.scalability.workload.workload_version import WorkloadVersion

T = TypeVar("T")


@dataclass
class BenchmarkResult:
    overall_comparison_outcome: ComparisonOutcome
    df_total_by_endpoint_name_and_workload: dict[str, dict[str, DfTotals]]
    df_details_by_endpoint_name_and_workload: dict[str, dict[str, DfDetails]]
    workload_version_by_name: dict[str, WorkloadVersion]
    workload_group_by_name: dict[str, str]

    def __init__(self):
        self.overall_comparison_outcome = ComparisonOutcome()
        self.df_total_by_endpoint_name_and_workload = dict()
        self.df_details_by_endpoint_name_and_workload = dict()
        self.workload_version_by_name = dict()
        self.workload_group_by_name = dict()

    def add_regression(self, comparison_outcome: ComparisonOutcome | None) -> None:
        if comparison_outcome is not None:
            self.overall_comparison_outcome.merge(comparison_outcome)

    def record_workload_metadata(self, workload: Workload) -> None:
        self.workload_version_by_name[workload.name()] = workload.version()

        assert isinstance(workload, WorkloadMarker)
        self.workload_group_by_name[workload.name()] = workload.group_name()

    def get_endpoint_names(self) -> list[str]:
        return list(self.df_total_by_endpoint_name_and_workload.keys())

    def append_workload_result(
        self, endpoint_version_info: str, result: WorkloadResult
    ) -> None:
        if (
            endpoint_version_info
            not in self.df_total_by_endpoint_name_and_workload.keys()
        ):
            self.df_total_by_endpoint_name_and_workload[endpoint_version_info] = dict()
            self.df_details_by_endpoint_name_and_workload[endpoint_version_info] = (
                dict()
            )

        workload_name = result.workload.name()
        if (
            workload_name
            in self.df_total_by_endpoint_name_and_workload[endpoint_version_info].keys()
        ):
            # Entry already exists, this happens in case of retries
            print(
                f"Replacing result entry for endpoint ({endpoint_version_info}) and workload {workload_name}"
            )

        self.df_total_by_endpoint_name_and_workload[endpoint_version_info][
            workload_name
        ] = result.df_totals
        self.df_details_by_endpoint_name_and_workload[endpoint_version_info][
            workload_name
        ] = result.df_details

    def get_df_total_by_endpoint_name(self, endpoint_name: str) -> DfTotals:
        return concat_df_totals(
            list(self.df_total_by_endpoint_name_and_workload[endpoint_name].values())
        )

    def get_df_total_by_workload_and_endpoint(
        self,
    ) -> dict[str, dict[str, DfTotals]]:
        return self._swap_endpoint_and_workload_grouping(
            self.df_total_by_endpoint_name_and_workload
        )

    def get_df_details_by_workload_and_endpoint(
        self,
    ) -> dict[str, dict[str, DfDetails]]:
        return self._swap_endpoint_and_workload_grouping(
            self.df_details_by_endpoint_name_and_workload
        )

    def _swap_endpoint_and_workload_grouping(
        self, result_by_endpoint_and_workload: dict[str, dict[str, T]]
    ) -> dict[str, dict[str, T]]:
        result: dict[str, dict[str, T]] = dict()

        for (
            endpoint_name,
            data_by_workload,
        ) in result_by_endpoint_and_workload.items():
            for workload_name, data in data_by_workload.items():
                if workload_name not in result.keys():
                    result[workload_name] = dict()

                result[workload_name][endpoint_name] = data

        return result
