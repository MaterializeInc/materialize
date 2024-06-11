# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.feature_benchmark.benchmark_versioning import (
    FEATURE_BENCHMARK_FRAMEWORK_DIR,
    FEATURE_BENCHMARK_SCENARIOS_DIR,
    SHA256_BY_SCENARIO_FILE,
    SHA256_OF_FRAMEWORK,
)
from materialize.test_analytics.versioning.versioning_config import (
    DirectoryVersioningConfig,
    PerFileVersioningConfig,
)

BENCHMARK_FRAMEWORK_VERSION_SKEW_CHECK_CONFIG = DirectoryVersioningConfig(
    root_directory=FEATURE_BENCHMARK_FRAMEWORK_DIR,
    sha256_per_entry=SHA256_OF_FRAMEWORK,
    sha256_per_entry_dict_name="SHA256_OF_FRAMEWORK",
    excluded_file_names={"benchmark_versioning.py"},
    excluded_paths={"scenarios"},
    task_on_hash_mismatch="Please update the version of the framework if the changes are expected to impact results!",
)

BENCHMARK_SCENARIOS_VERSION_SKEW_CHECK_CONFIG = PerFileVersioningConfig(
    root_directory=FEATURE_BENCHMARK_SCENARIOS_DIR,
    sha256_per_entry=SHA256_BY_SCENARIO_FILE,
    sha256_per_entry_dict_name="SHA256_BY_SCENARIO_FILE",
    task_on_hash_mismatch="Please update the version of the scenario if the scenario has changed semantically!",
)
