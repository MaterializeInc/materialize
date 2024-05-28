#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.scalability.scalability_versioning import (
    SCALABILITY_FRAMEWORK_DIR,
    SCALABILITY_WORKLOADS_DIR,
    SHA256_BY_FRAMEWORK_DIR,
    SHA256_BY_WORKLOAD_FILE,
)
from materialize.test_analytics.versioning.versioning_config import (
    PerDirectoryVersioningConfig,
    PerFileVersioningConfig,
)

SCALABILITY_FRAMEWORK_VERSION_SKEW_CHECK_CONFIG = PerDirectoryVersioningConfig(
    root_directory=SCALABILITY_FRAMEWORK_DIR,
    sha256_per_entry=SHA256_BY_FRAMEWORK_DIR,
    sha256_per_entry_dict_name="SHA256_BY_FRAMEWORK_DIR",
    excluded_file_names={"scalability_versioning.py"},
    excluded_paths={"workloads"},
    task_on_hash_mismatch="Please update the version of the framework if the changes are expected to impact results!",
)

SCALABILITY_WORKLOADS_VERSION_SKEW_CHECK_CONFIG = PerFileVersioningConfig(
    root_directory=SCALABILITY_WORKLOADS_DIR,
    sha256_per_entry=SHA256_BY_WORKLOAD_FILE,
    sha256_per_entry_dict_name="SHA256_BY_WORKLOAD_FILE",
    task_on_hash_mismatch="Please update the version of the workload if the workload has changed semantically!",
)
