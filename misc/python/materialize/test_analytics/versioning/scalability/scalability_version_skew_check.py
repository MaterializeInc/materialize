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
    SCALABILITY_FRAMEWORK_HASH_FILE,
    SCALABILITY_WORKLOADS_DIR,
    SHA256_BY_WORKLOAD_FILE,
    SHA256_OF_FRAMEWORK,
)
from materialize.test_analytics.versioning.versioning_config import (
    DirectoryVersioningConfig,
    PerFileVersioningConfig,
)

SCALABILITY_FRAMEWORK_VERSION_SKEW_CHECK_CONFIG = DirectoryVersioningConfig(
    root_directory=SCALABILITY_FRAMEWORK_DIR,
    sha256_definition_file=SCALABILITY_FRAMEWORK_HASH_FILE,
    sha256_per_entry=SHA256_OF_FRAMEWORK,
    sha256_per_entry_dict_name="SHA256_OF_FRAMEWORK",
    excluded_file_names={"scalability_versioning.py"},
    excluded_paths={"workloads"},
    task_on_hash_mismatch="Please update the version of the framework if the changes are expected to impact results!",
)

SCALABILITY_WORKLOADS_VERSION_SKEW_CHECK_CONFIG = PerFileVersioningConfig(
    root_directory=SCALABILITY_WORKLOADS_DIR,
    sha256_definition_file=SCALABILITY_FRAMEWORK_HASH_FILE,
    sha256_per_entry=SHA256_BY_WORKLOAD_FILE,
    sha256_per_entry_dict_name="SHA256_BY_WORKLOAD_FILE",
    task_on_hash_mismatch="Please update the version of the workload if the workload has changed semantically!",
)
