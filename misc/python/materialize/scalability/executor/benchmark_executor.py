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

from materialize.scalability.config.benchmark_config import BenchmarkConfiguration
from materialize.scalability.df import df_details_cols, df_totals_cols
from materialize.scalability.df.df_details import DfDetails, concat_df_details
from materialize.scalability.df.df_totals import DfTotals, concat_df_totals
from materialize.scalability.endpoint.endpoint import Endpoint
from materialize.scalability.io import paths
from materialize.scalability.operation.scalability_operation import Operation
from materialize.scalability.result.comparison_outcome import ComparisonOutcome
from materialize.scalability.result.result_analyzer import ResultAnalyzer
from materialize.scalability.result.scalability_result import BenchmarkResult
from materialize.scalability.result.workload_result import WorkloadResult
from materialize.scalability.schema.schema import Schema
from materialize.scalability.workload.workload import Workload, WorkloadWithContext
from materialize.scalability.workload.workloads.connection_workloads import *  # noqa: F401 F403
from materialize.scalability.workload.workloads.ddl_workloads import *  # noqa: F401 F403
from materialize.scalability.workload.workloads.dml_dql_workloads import *  # noqa: F401 F403
from materialize.scalability.workload.workloads.self_test_workloads import *  # noqa: F401 F403

# number of retries in addition to the first run
MAX_RETRIES_ON_REGRESSION = 2


class BenchmarkExecutor:
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
        self.result = BenchmarkResult()

    def run_workloads(
        self,
    ) -> BenchmarkResult:

        for workload_cls in self.config.workload_classes:
            assert issubclass(
                workload_cls, Workload
            ), f"{workload_cls} is not a Workload"
            self.run_workload_for_all_endpoints(
                workload_cls,
            )

        return self.result

    def run_workload_for_all_endpoints(
        self,
        workload_cls: type[Workload],
    ):
        if self.baseline_endpoint is not None:
            baseline_result = self.run_workload_for_endpoint(
                self.baseline_endpoint,
                self.create_workload_instance(
                    workload_cls, endpoint=self.baseline_endpoint
                ),
            )
        else:
            baseline_result = None

        for other_endpoint in self.other_endpoints:
            comparison_outcome = self.run_and_evaluate_workload_for_endpoint(
                workload_cls, other_endpoint, baseline_result, try_count=0
            )

            self.result.add_regression(comparison_outcome)

    def run_and_evaluate_workload_for_endpoint(
        self,
        workload_cls: type[Workload],
        other_endpoint: Endpoint,
        baseline_result: WorkloadResult | None,
        try_count: int,
    ) -> ComparisonOutcome | None:
        workload_name = workload_cls.__name__
        other_endpoint_result = self.run_workload_for_endpoint(
            other_endpoint,
            self.create_workload_instance(workload_cls, endpoint=other_endpoint),
        )

        if self.baseline_endpoint is None or baseline_result is None:
            return None

        outcome = self.result_analyzer.perform_comparison_in_workload(
            workload_name,
            self.baseline_endpoint,
            other_endpoint,
            baseline_result,
            other_endpoint_result,
        )

        if outcome.has_regressions() and try_count < MAX_RETRIES_ON_REGRESSION:
            print(
                f"Potential regression in workload {workload_name} at endpoint {other_endpoint},"
                f" triggering retry {try_count + 1} of {MAX_RETRIES_ON_REGRESSION}"
            )
            return self.run_and_evaluate_workload_for_endpoint(
                workload_cls, other_endpoint, baseline_result, try_count=try_count + 1
            )

        return outcome

    def run_workload_for_endpoint(
        self,
        endpoint: Endpoint,
        workload: Workload,
    ) -> WorkloadResult:
        print(f"--- Running workload {workload.name()} on {endpoint}")
        self.result.record_workload_metadata(workload)

        df_totals = DfTotals()
        df_details = DfDetails()

        concurrencies = self._get_concurrencies()
        print(f"Concurrencies: {concurrencies}")

        for concurrency in concurrencies:
            df_total, df_detail = self.run_workload_for_endpoint_with_concurrency(
                endpoint,
                workload,
                concurrency,
                self.config.get_count_for_concurrency(concurrency),
            )
            df_totals = concat_df_totals([df_totals, df_total])
            df_details = concat_df_details([df_details, df_detail])

            endpoint_version_name = endpoint.try_load_version()
            pathlib.Path(paths.endpoint_dir(endpoint_version_name)).mkdir(
                parents=True, exist_ok=True
            )

            df_totals.to_csv(
                paths.df_totals_csv(endpoint_version_name, workload.name())
            )
            df_details.to_csv(
                paths.df_details_csv(endpoint_version_name, workload.name())
            )

        result = WorkloadResult(workload, endpoint, df_totals, df_details)
        self._record_results(result)
        return result

    def run_workload_for_endpoint_with_concurrency(
        self,
        endpoint: Endpoint,
        workload: Workload,
        concurrency: int,
        count: int,
    ) -> tuple[DfTotals, DfDetails]:
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

        for init_operation in workload.init_operations():
            workload.execute_operation(
                init_operation, init_cursor, -1, -1, self.config.verbose
            )

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
                        int(i / len(operations)),
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
                    df_totals_cols.WORKLOAD: workload.name(),
                    df_totals_cols.COUNT: count,
                    df_totals_cols.TPS: count / wallclock_total,
                    df_totals_cols.MEAN_TX_DURATION: df_detail[
                        df_details_cols.WALLCLOCK
                    ].mean(),
                    df_totals_cols.MEDIAN_TX_DURATION: df_detail[
                        df_details_cols.WALLCLOCK
                    ].median(),
                    df_totals_cols.MIN_TX_DURATION: df_detail[
                        df_details_cols.WALLCLOCK
                    ].min(),
                    df_totals_cols.MAX_TX_DURATION: df_detail[
                        df_details_cols.WALLCLOCK
                    ].max(),
                }
            ]
        )

        return DfTotals(df_total), DfDetails(df_detail)

    def execute_operation(
        self, args: tuple[Workload, int, threading.local, list[Cursor], Operation, int]
    ) -> dict[str, Any]:
        workload, concurrency, local, cursor_pool, operation, transaction_index = args
        worker_id = local.worker_id
        assert (
            len(cursor_pool) >= worker_id + 1
        ), f"len(cursor_pool) is {len(cursor_pool)} but local.worker_id is {worker_id}"
        cursor = cursor_pool[worker_id]

        start = time.time()
        workload.execute_operation(
            operation, cursor, worker_id, transaction_index, self.config.verbose
        )
        wallclock = time.time() - start

        return {
            df_details_cols.CONCURRENCY: concurrency,
            df_details_cols.WALLCLOCK: wallclock,
            df_details_cols.OPERATION: type(operation).__name__,
            df_details_cols.WORKLOAD: workload.name(),
            df_details_cols.TRANSACTION_INDEX: transaction_index,
        }

    def create_workload_instance(
        self, workload_cls: type[Workload], endpoint: Endpoint
    ) -> Workload:
        workload = workload_cls()

        if isinstance(workload, WorkloadWithContext):
            workload.set_endpoint(endpoint)
            workload.set_schema(self.schema)

        return workload

    def initialize_worker(self, local: threading.local, lock: threading.Lock):
        """Give each other worker thread a unique ID"""
        lock.acquire()
        global next_worker_id
        local.worker_id = next_worker_id
        next_worker_id = next_worker_id + 1
        lock.release()

    def _get_concurrencies(self) -> list[int]:
        range_end = 1024 if self.config.exponent_base < 2.0 else 32
        concurrencies: list[int] = [
            round(self.config.exponent_base**c) for c in range(0, range_end)
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

    def _record_results(self, result: WorkloadResult) -> None:
        endpoint_version_info = result.endpoint.try_load_version()
        print(
            f"Collecting results of endpoint {result.endpoint} with name {endpoint_version_info}"
        )

        self.result.append_workload_result(endpoint_version_info, result)
