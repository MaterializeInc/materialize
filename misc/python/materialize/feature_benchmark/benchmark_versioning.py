# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from pathlib import Path

FEATURE_BENCHMARK_FRAMEWORK_VERSION = "1.0.0"

FEATURE_BENCHMARK_FRAMEWORK_DIR = Path(__file__).resolve().parent
FEATURE_BENCHMARK_SCENARIOS_DIR = FEATURE_BENCHMARK_FRAMEWORK_DIR / "scenarios"

SHA256_BY_FRAMEWORK_DIR: dict[str, str] = {}
SHA256_BY_FRAMEWORK_DIR[
    "."
] = "b8766c1d2afc54b4bebb36c527cfad66b46214ac4a580ef0beec5edc0eabaa3e"

SHA256_BY_SCENARIO_FILE: dict[str, str] = {}
SHA256_BY_SCENARIO_FILE[
    "benchmark_main.py"
] = "55675ac038d7ec50bae567f2096b16bb15ebb0b374093147ba407fe1040a04f1"
SHA256_BY_SCENARIO_FILE[
    "concurrency.py"
] = "2e9c149c136b83b3853abc923a1adbdaf55a998ab4557712f8424c8b16f2adb1"
SHA256_BY_SCENARIO_FILE[
    "customer.py"
] = "d1e72837a342c3ebf1f4a32ec583b1b78a78644cdba495030a6df45ebbffe703"
SHA256_BY_SCENARIO_FILE[
    "optbench.py"
] = "c7b8b97cb77ed8cebb74b9236da707cc516f928526a1ef440b89e367f5fc6898"
SHA256_BY_SCENARIO_FILE[
    "scale.py"
] = "c4c8749d166e4df34e0b0e92220434fdb508c5c2ac56eb80c07043be0048dded"
SHA256_BY_SCENARIO_FILE[
    "skew.py"
] = "bf60802205fc51ebf94fb008bbdb6b2ccce3c9ed88a6188fa7f090f2c84b120f"
SHA256_BY_SCENARIO_FILE[
    "subscribe.py"
] = "732c941a33645fc0942afef252e373f36395e119edbdb4f59008eeaebbea6abc"
