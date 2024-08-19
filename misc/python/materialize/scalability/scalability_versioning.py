# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from pathlib import Path

from materialize import MZ_ROOT

SCALABILITY_FRAMEWORK_VERSION = "1.3.0"
SCALABILITY_FRAMEWORK_HASH_FILE = Path(__file__).relative_to(MZ_ROOT)

SCALABILITY_FRAMEWORK_DIR = Path(__file__).resolve().parent
SCALABILITY_WORKLOADS_DIR = SCALABILITY_FRAMEWORK_DIR / "workloads"

# Consider increasing the #SCALABILITY_FRAMEWORK_VERSION if changes are expected to impact results!
SHA256_OF_FRAMEWORK: dict[str, str] = {}
SHA256_OF_FRAMEWORK["*"] = (
    "bdc636367da82cafe005dd8e4bfe2b2d38028f723f9cb80e8ccf031a22272b5b"
)

# Consider increasing the workload's class #version() if changes are expected to impact results!
SHA256_BY_WORKLOAD_FILE: dict[str, str] = {}
SHA256_BY_WORKLOAD_FILE["connection_workloads.py"] = (
    "6fb4acfc1f56456c47398bb633fab96338672f442386791ca43b1474d6a225b0"
)
SHA256_BY_WORKLOAD_FILE["ddl_workloads.py"] = (
    "be640856e49339c4e1a84aea7493a8501166ee3076fb83d8fec5a8538148aa1d"
)
SHA256_BY_WORKLOAD_FILE["dml_dql_workloads.py"] = (
    "c608fb218bd2eaa7841fb0bc865ade9d2aa8409ec2b1f2cd4c564d0b95e1226f"
)
SHA256_BY_WORKLOAD_FILE["self_test_workloads.py"] = (
    "2ad6f97aec62bbcad7bd56c28e750e29c965a6cc8c9c40eb97cf676bb3666728"
)
