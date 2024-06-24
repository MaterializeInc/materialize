# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Test analytics database."""
from materialize.test_analytics.config.mz_db_config import MzDbConfig
from materialize.test_analytics.connector.test_analytics_connector import (
    DatabaseConnector,
)
from materialize.test_analytics.data.build.build_data_storage import BuildDataStorage
from materialize.test_analytics.data.build.build_history_analysis import (
    BuildHistoryAnalysis,
)
from materialize.test_analytics.data.build_annotation.build_annotation_storage import (
    BuildAnnotationStorage,
)
from materialize.test_analytics.data.feature_benchmark.feature_benchmark_result_storage import (
    FeatureBenchmarkResultStorage,
)
from materialize.test_analytics.data.scalability_framework.scalability_framework_result_storage import (
    ScalabilityFrameworkResultStorage,
)

TEST_ANALYTICS_DATA_VERSION: int = 7


class TestAnalyticsDb:

    def __init__(self, config: MzDbConfig):
        self.config = config
        self.database_connector = DatabaseConnector(
            config, current_data_version=TEST_ANALYTICS_DATA_VERSION, log_sql=True
        )

        self.builds = BuildDataStorage(
            self.database_connector, TEST_ANALYTICS_DATA_VERSION
        )
        self.scalability_results = ScalabilityFrameworkResultStorage(
            self.database_connector
        )
        self.benchmark_results = FeatureBenchmarkResultStorage(self.database_connector)
        self.build_annotations = BuildAnnotationStorage(self.database_connector)
        self.build_history = BuildHistoryAnalysis(self.database_connector)

    def submit_updates(self) -> None:
        """
        This will open a connection to the test-analytics database and submit the SQL statements.
        Make sure that this method is always invoked within a try-catch block.
        """
        self.database_connector.submit_update_statements()
