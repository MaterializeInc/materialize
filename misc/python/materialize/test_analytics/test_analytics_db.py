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
from materialize.test_analytics.data.build_annotation.build_annotation_storage import (
    BuildAnnotationStorage,
)
from materialize.test_analytics.data.feature_benchmark.feature_benchmark_result_storage import (
    FeatureBenchmarkResultStorage,
)
from materialize.test_analytics.data.scalability_framework.scalability_framework_result_storage import (
    ScalabilityFrameworkResultStorage,
)

TEST_ANALYTICS_DATA_VERSION: int = 4


class TestAnalyticsDb:

    def __init__(self, config: MzDbConfig):
        self.config = config
        database_connector = DatabaseConnector(config, log_sql=True)

        self.builds = BuildDataStorage(database_connector, TEST_ANALYTICS_DATA_VERSION)
        self.scalability_results = ScalabilityFrameworkResultStorage(database_connector)
        self.benchmark_results = FeatureBenchmarkResultStorage(database_connector)
        self.build_annotations = BuildAnnotationStorage(database_connector)

    def _disable_writer_if_on_unsupported_version(
        self, database_connector: DatabaseConnector
    ) -> None:
        min_required_data_version = database_connector.query_min_required_data_version()

        if TEST_ANALYTICS_DATA_VERSION < min_required_data_version:
            print(
                f"Uploading test_analytics data is not supported from this data version ({TEST_ANALYTICS_DATA_VERSION})"
            )
            database_connector.set_read_only()
