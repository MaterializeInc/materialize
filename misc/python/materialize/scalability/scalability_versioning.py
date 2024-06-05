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

SCALABILITY_FRAMEWORK_DIR = Path(__file__).resolve().parent
SCALABILITY_WORKLOADS_DIR = SCALABILITY_FRAMEWORK_DIR / "workloads"

SHA256_BY_FRAMEWORK_DIR: dict[str, str] = {}
SHA256_BY_FRAMEWORK_DIR[
    "."
] = "fc48c6a6b3f9ab3feb91d4fa01bddc7f76a86d6851b2f65016cc01d586752cbe"
SHA256_BY_FRAMEWORK_DIR[
    "df"
] = "e327f9eddd4bd9f14d69f549221f8acefd27ee63c199b7ca3b46c40bbbdfcc3f"
SHA256_BY_FRAMEWORK_DIR[
    "io"
] = "c93a7db5b5411b0bed09d19175570457d5561aebbe6c28a4892c78f9f6842981"
SHA256_BY_FRAMEWORK_DIR[
    "plot"
] = "f728940456ad6dfee9c7025dcb7bb264de31d6897152898a1b097547cb4173c5"
SHA256_BY_FRAMEWORK_DIR[
    "scalability"
] = "6fb4acfc1f56456c47398bb633fab96338672f442386791ca43b1474d6a225b0"

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
