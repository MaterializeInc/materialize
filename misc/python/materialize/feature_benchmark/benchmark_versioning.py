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
] = "c582dc1983d1956dd4b85a866e5d74ca6577183c6e41e283f97127c56c12638b"

SHA256_BY_SCENARIO_FILE: dict[str, str] = {}
SHA256_BY_SCENARIO_FILE[
    "benchmark_main.py"
] = "a2d4c03bbf9fe668208ac29830d31ebc359b65b87c7eae1bdad379fdb8b32217"
SHA256_BY_SCENARIO_FILE[
    "concurrency.py"
] = "2e9c149c136b83b3853abc923a1adbdaf55a998ab4557712f8424c8b16f2adb1"
SHA256_BY_SCENARIO_FILE[
    "customer.py"
] = "d1e72837a342c3ebf1f4a32ec583b1b78a78644cdba495030a6df45ebbffe703"
SHA256_BY_SCENARIO_FILE[
    "optbench.py"
] = "e087c72dbda9c48d9de14917b3c40a1f8b07617a3b406d52db917318c8057af3"
SHA256_BY_SCENARIO_FILE[
    "scale.py"
] = "c4c8749d166e4df34e0b0e92220434fdb508c5c2ac56eb80c07043be0048dded"
SHA256_BY_SCENARIO_FILE[
    "skew.py"
] = "bf60802205fc51ebf94fb008bbdb6b2ccce3c9ed88a6188fa7f090f2c84b120f"
SHA256_BY_SCENARIO_FILE[
    "subscribe.py"
] = "732c941a33645fc0942afef252e373f36395e119edbdb4f59008eeaebbea6abc"
