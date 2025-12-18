# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Test analytics database."""
from materialize import buildkite
from materialize.test_analytics.config.mz_db_config import MzDbConfig
from materialize.test_analytics.connector.test_analytics_connector import (
    DatabaseConnector,
    DatabaseConnectorImpl,
    DummyDatabaseConnector,
)
from materialize.test_analytics.data.bounded_memory.bounded_memory_minimal_search_storage import (
    BoundedMemoryMinimalSearchStorage,
)
from materialize.test_analytics.data.build.build_data_storage import BuildDataStorage
from materialize.test_analytics.data.build.build_history_analysis import (
    BuildHistoryAnalysis,
)
from materialize.test_analytics.data.build_annotation.build_annotation_storage import (
    BuildAnnotationStorage,
)
from materialize.test_analytics.data.cluster_spec_sheet.cluster_spec_sheet_result_storage import (
    ClusterSpecSheetEnvironmentdResultStorage,
    ClusterSpecSheetResultStorage,
)
from materialize.test_analytics.data.feature_benchmark.feature_benchmark_result_storage import (
    FeatureBenchmarkResultStorage,
)
from materialize.test_analytics.data.known_issues.known_issues_storage import (
    KnownIssuesStorage,
)
from materialize.test_analytics.data.output_consistency.output_consistency_stats_storage import (
    OutputConsistencyStatsStorage,
)
from materialize.test_analytics.data.parallel_benchmark.parallel_benchmark_result_storage import (
    ParallelBenchmarkResultStorage,
)
from materialize.test_analytics.data.product_limits.product_limits_result_storage import (
    ProductLimitsResultStorage,
)
from materialize.test_analytics.data.scalability_framework.scalability_framework_result_storage import (
    ScalabilityFrameworkResultStorage,
)
from materialize.test_analytics.data.upgrade_downtime.upgrade_downtime_result_storage import (
    UpgradeDowntimeResultStorage,
)

TEST_ANALYTICS_DATA_VERSION: int = 21


class TestAnalyticsDb:
    __test__ = False

    def __init__(self, config: MzDbConfig):
        self.database_connector = self._create_database_connector(config)

        self.builds = BuildDataStorage(
            self.database_connector, TEST_ANALYTICS_DATA_VERSION
        )
        self.scalability_results = ScalabilityFrameworkResultStorage(
            self.database_connector
        )
        self.benchmark_results = FeatureBenchmarkResultStorage(self.database_connector)
        self.build_annotations = BuildAnnotationStorage(self.database_connector)
        self.build_history = BuildHistoryAnalysis(self.database_connector)
        self.output_consistency = OutputConsistencyStatsStorage(self.database_connector)
        self.known_issues = KnownIssuesStorage(self.database_connector)
        self.parallel_benchmark_results = ParallelBenchmarkResultStorage(
            self.database_connector
        )
        self.bounded_memory_search = BoundedMemoryMinimalSearchStorage(
            self.database_connector
        )
        self.product_limits_results = ProductLimitsResultStorage(
            self.database_connector
        )
        self.cluster_spec_sheet_results = ClusterSpecSheetResultStorage(
            self.database_connector
        )
        self.cluster_spec_sheet_environmentd_results = (
            ClusterSpecSheetEnvironmentdResultStorage(self.database_connector)
        )
        self.upgrade_downtime_results = UpgradeDowntimeResultStorage(
            self.database_connector
        )

    def _create_database_connector(self, config: MzDbConfig) -> DatabaseConnector:
        if config.enabled:
            return DatabaseConnectorImpl(
                config, current_data_version=TEST_ANALYTICS_DATA_VERSION, log_sql=True
            )
        else:
            return DummyDatabaseConnector(config)

    def submit_updates(self) -> None:
        """
        This will open a connection to the test-analytics database and submit the SQL statements.
        Make sure that this method is always invoked within a try-catch block.
        """
        self.database_connector.submit_update_statements()

    def on_upload_failed(self, e: Exception) -> None:
        self._communication_failed(f"Uploading results failed! {e}")

    def on_data_retrieval_failed(self, e: Exception) -> None:
        self._communication_failed(f"Loading data failed! {e}")

    def _communication_failed(self, message: str) -> None:
        if not buildkite.is_in_buildkite():
            print(message)

        if not self.shall_notify_qa_team():
            return

        buildkite.notify_qa_team_about_failure(message)

    def shall_notify_qa_team(self) -> bool:
        if buildkite.is_on_default_branch():
            return True

        settings = self.database_connector.try_get_or_query_settings()

        if settings is None:
            return True

        return not settings.only_notify_about_communication_failures_on_main
