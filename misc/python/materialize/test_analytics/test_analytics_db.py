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
from materialize.test_analytics.writer.test_analytics_writer import RawDatabaseWriter

TEST_ANALYTICS_DATA_VERSION: int = 4


class TestAnalyticsDb:

    def __init__(self, config: MzDbConfig):
        self.config = config
        raw_database_writer = RawDatabaseWriter(config, log_sql=True)
        self.builds = BuildDataStorage(raw_database_writer, TEST_ANALYTICS_DATA_VERSION)
        self.scalability_results = ScalabilityFrameworkResultStorage(
            raw_database_writer
        )
        self.benchmark_results = FeatureBenchmarkResultStorage(raw_database_writer)
        self.build_annotations = BuildAnnotationStorage(raw_database_writer)
