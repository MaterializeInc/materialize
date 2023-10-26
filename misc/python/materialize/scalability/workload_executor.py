# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pathlib
import threading
import time
from concurrent import futures
from typing import Any

import pandas as pd
from psycopg import Cursor

from materialize.scalability.benchmark_config import BenchmarkConfiguration
from materialize.scalability.df import df_details_cols, df_totals_cols, paths
from materialize.scalability.endpoint import Endpoint
from materialize.scalability.operation import Operation
from materialize.scalability.regression import RegressionOutcome
from materialize.scalability.result_analyzer import ResultAnalyzer
from materialize.scalability.schema import Schema
from materialize.scalability.workload import Workload
from materialize.scalability.workload_result import WorkloadResult
from materialize.scalability.workloads import *  # noqa: F401 F403
from materialize.scalability.workloads_test import *  # noqa: F401 F403

MAX_RETRIES_ON_REGRESSION = 1


class WorkloadExecutor:
    def __init__(
        self,
        config: BenchmarkConfiguration,
        schema: Schema,
        baseline_endpoint: Endpoint | None,
        other_endpoints: list[Endpoint],
        result_analyzer: ResultAnalyzer,
    ):
        self.config = config
        self.schema = schema
        self.baseline_endpoint = baseline_endpoint
        self.other_endpoints = other_endpoints
        self.result_analyzer = result_analyzer
        self.df_total_by_endpoint_name: dict[str, pd.DataFrame] = dict()

    def run_workloads(
        self,
    ) -> RegressionOutcome:
        overall_regression_outcome = RegressionOutcome()

        for workload_cls in self.config.workload_classes:
            assert issubclass(
                workload_cls, Workload
            ), f"{workload_cls} is not a Workload"
            self.run_workload_for_all_endpoints(
                workload_cls,
                overall_regression_outcome,
            )

        return overall_regression_outcome

    def run_workload_for_all_endpoints(
        self,
        workload_cls: type[Workload],
        overall_regression_outcome: RegressionOutcome,
    ):
        if self.baseline_endpoint is not None:
            baseline_result = self.run_workload_for_endpoint(
                self.baseline_endpoint, workload_cls()
            )
        else:
            baseline_result = None

        for other_endpoint in self.other_endpoints:
            regression_outcome = self.run_and_evaluate_workload_for_endpoint(
                workload_cls, other_endpoint, baseline_result, try_count=0
            )

            if regression_outcome is not None:
                overall_regression_outcome.merge(regression_outcome)

    def run_and_evaluate_workload_for_endpoint(
        self,
        workload_cls: type[Workload],
        other_endpoint: Endpoint,
        baseline_result: WorkloadResult | None,
        try_count: int,
    ) -> RegressionOutcome | None:
        other_endpoint_result = self.run_workload_for_endpoint(
            other_endpoint, workload_cls()
        )

        if self.baseline_endpoint is None or baseline_result is None:
            return None

        outcome = self.result_analyzer.determine_regression_in_workload(
            workload_cls.__name__,
            self.baseline_endpoint,
            other_endpoint,
            baseline_result,
            other_endpoint_result,
        )

        if outcome.has_regressions() and try_count < MAX_RETRIES_ON_REGRESSION:
            return self.run_and_evaluate_workload_for_endpoint(
                workload_cls, other_endpoint, baseline_result, try_count=try_count + 1
            )

        return outcome

    def run_workload_for_endpoint(
        self,
        endpoint: Endpoint,
        workload: Workload,
    ) -> WorkloadResult:
        print(f"Running workload {workload} on {endpoint}")

        df_totals = pd.DataFrame()
        df_details = pd.DataFrame()

        concurrencies = self._get_concurrencies()
        print(f"Concurrencies: {concurrencies}")

        for concurrency in concurrencies:
            df_total, df_detail = self.run_workload_for_endpoint_with_concurrency(
                endpoint,
                workload,
                concurrency,
                self.config.get_count_for_concurrency(concurrency),
            )
            df_totals = pd.concat([df_totals, df_total], ignore_index=True)
            df_details = pd.concat([df_details, df_detail], ignore_index=True)

            endpoint_version_name = endpoint.try_load_version()
            pathlib.Path(paths.endpoint_dir(endpoint_version_name)).mkdir(
                parents=True, exist_ok=True
            )

            df_totals.to_csv(paths.df_totals_csv(endpoint_version_name, workload))
            df_details.to_csv(paths.df_details_csv(endpoint_version_name, workload))

        self._record_results(endpoint, df_totals)
        return WorkloadResult(workload, df_totals, df_details)

    def run_workload_for_endpoint_with_concurrency(
        self,
        endpoint: Endpoint,
        workload: Workload,
        concurrency: int,
        count: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        print(
            f"Preparing benchmark for workload '{workload.name()}' at concurrency {concurrency} ..."
        )
        endpoint.up()

        init_sqls = self.schema.init_sqls()

        init_conn = endpoint.sql_connection()
        init_conn.autocommit = True
        init_cursor = init_conn.cursor()
        for init_sql in init_sqls:
            print(init_sql)
            init_cursor.execute(init_sql.encode("utf8"))

        print(
            f"Creating a cursor pool with {concurrency} entries against endpoint: {endpoint.url()}"
        )
        cursor_pool = self._create_cursor_pool(concurrency, endpoint)

        print(
            f"Benchmarking workload '{workload.name()}' at concurrency {concurrency} ..."
        )
        operations = workload.operations()

        global next_worker_id
        next_worker_id = 0
        local = threading.local()
        lock = threading.Lock()

        start = time.time()
        with futures.ThreadPoolExecutor(
            concurrency, initializer=self.initialize_worker, initargs=(local, lock)
        ) as executor:
            measurements = executor.map(
                self.execute_operation,
                [
                    (
                        workload,
                        concurrency,
                        local,
                        cursor_pool,
                        operations[i % len(operations)],
                    )
                    for i in range(count)
                ],
            )
        wallclock_total = time.time() - start

        df_detail = pd.DataFrame(measurements)
        print("Best and worst individual measurements:")
        print(df_detail.sort_values(by=[df_details_cols.WALLCLOCK]))

        print(
            f"concurrency: {concurrency}; wallclock_total: {wallclock_total}; tps = {count/wallclock_total}"
        )

        df_total = pd.DataFrame(
            [
                {
                    df_totals_cols.CONCURRENCY: concurrency,
                    df_totals_cols.WALLCLOCK: wallclock_total,
                    df_totals_cols.WORKLOAD: type(workload).__name__,
                    df_totals_cols.COUNT: count,
                    df_totals_cols.TPS: count / wallclock_total,
                    df_totals_cols.MEAN_TX_DURATION: df_detail[
                        df_totals_cols.WALLCLOCK
                    ].mean(),
                    df_totals_cols.MEDIAN_TX_DURATION: df_detail[
                        df_totals_cols.WALLCLOCK
                    ].median(),
                    df_totals_cols.MIN_TX_DURATION: df_detail[
                        df_totals_cols.WALLCLOCK
                    ].min(),
                    df_totals_cols.MAX_TX_DURATION: df_detail[
                        df_totals_cols.WALLCLOCK
                    ].max(),
                }
            ]
        )

        return df_total, df_detail

    def execute_operation(
        self, args: tuple[Workload, int, threading.local, list[Cursor], Operation]
    ) -> dict[str, Any]:
        workload, concurrency, local, cursor_pool, operation = args
        assert (
            len(cursor_pool) >= local.worker_id + 1
        ), f"len(cursor_pool) is {len(cursor_pool)} but local.worker_id is {local.worker_id}"
        cursor = cursor_pool[local.worker_id]

        start = time.time()
        operation.execute(cursor)
        wallclock = time.time() - start

        return {
            df_details_cols.CONCURRENCY: concurrency,
            df_details_cols.WALLCLOCK: wallclock,
            df_details_cols.OPERATION: type(operation).__name__,
            df_details_cols.WORKLOAD: type(workload).__name__,
        }

    def initialize_worker(self, local: threading.local, lock: threading.Lock):
        """Give each other worker thread a unique ID"""
        lock.acquire()
        global next_worker_id
        local.worker_id = next_worker_id
        next_worker_id = next_worker_id + 1
        lock.release()

    def _get_concurrencies(self) -> list[int]:
        concurrencies: list[int] = [
            round(self.config.exponent_base**c) for c in range(0, 1024)
        ]
        concurrencies = sorted(set(concurrencies))
        return [
            c
            for c in concurrencies
            if self.config.min_concurrency <= c <= self.config.max_concurrency
        ]

    def _create_cursor_pool(self, concurrency: int, endpoint: Endpoint) -> list[Cursor]:
        connect_sqls = self.schema.connect_sqls()

        cursor_pool = []
        for i in range(concurrency):
            conn = endpoint.sql_connection()
            conn.autocommit = True
            cursor = conn.cursor()
            for connect_sql in connect_sqls:
                cursor.execute(connect_sql.encode("utf8"))
            cursor_pool.append(cursor)

        return cursor_pool

    def _record_results(self, endpoint: Endpoint, df_totals: pd.DataFrame) -> None:
        endpoint_version_info = endpoint.try_load_version()
        print(
            f"Collecting results of endpoint {endpoint} with name {endpoint_version_info}"
        )

        if endpoint_version_info not in self.df_total_by_endpoint_name:
            self.df_total_by_endpoint_name[endpoint_version_info] = df_totals
        else:
            self.df_total_by_endpoint_name[endpoint_version_info] = pd.concat(
                [self.df_total_by_endpoint_name[endpoint_version_info], df_totals],
                ignore_index=True,
            )
