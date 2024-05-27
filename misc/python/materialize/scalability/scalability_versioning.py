# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from pathlib import Path

SCALABILITY_FRAMEWORK_VERSION = "1.0.0"

SCALABILITY_WORKLOADS_DIR = Path(__file__).resolve().parent / "workloads"

SHA256_BY_WORKLOAD_FILE: dict[str, str] = {}
SHA256_BY_WORKLOAD_FILE[
    "connection_workloads.py"
] = "6fb4acfc1f56456c47398bb633fab96338672f442386791ca43b1474d6a225b0"
SHA256_BY_WORKLOAD_FILE[
    "ddl_workloads.py"
] = "be640856e49339c4e1a84aea7493a8501166ee3076fb83d8fec5a8538148aa1d"
SHA256_BY_WORKLOAD_FILE[
    "dml_dql_workloads.py"
] = "c608fb218bd2eaa7841fb0bc865ade9d2aa8409ec2b1f2cd4c564d0b95e1226f"
SHA256_BY_WORKLOAD_FILE[
    "self_test_workloads.py"
] = "2ad6f97aec62bbcad7bd56c28e750e29c965a6cc8c9c40eb97cf676bb3666728"
