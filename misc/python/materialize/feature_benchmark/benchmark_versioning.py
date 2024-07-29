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

FEATURE_BENCHMARK_FRAMEWORK_VERSION = "1.1.0"
FEATURE_BENCHMARK_FRAMEWORK_HASH_FILE = Path(__file__).relative_to(MZ_ROOT)

FEATURE_BENCHMARK_FRAMEWORK_DIR = Path(__file__).resolve().parent
FEATURE_BENCHMARK_SCENARIOS_DIR = FEATURE_BENCHMARK_FRAMEWORK_DIR / "scenarios"

# Consider increasing the #FEATURE_BENCHMARK_FRAMEWORK_VERSION if changes are expected to impact results!
SHA256_OF_FRAMEWORK: dict[str, str] = {}
SHA256_OF_FRAMEWORK["*"] = (
    "bed2e00943061489499aa2e57624bbc62225100dd03f4be58b2579f488c9bcb7"
)

# Consider increasing the scenario's class #version() if changes are expected to impact results!
SHA256_BY_SCENARIO_FILE: dict[str, str] = {}
SHA256_BY_SCENARIO_FILE["benchmark_main.py"] = (
    "fd65a33c37f2364a5ae10273d6d95779aac1ac562030d7fa47030bcb423e379e"
)
SHA256_BY_SCENARIO_FILE["concurrency.py"] = (
    "2e9c149c136b83b3853abc923a1adbdaf55a998ab4557712f8424c8b16f2adb1"
)
SHA256_BY_SCENARIO_FILE["customer.py"] = (
    "d1e72837a342c3ebf1f4a32ec583b1b78a78644cdba495030a6df45ebbffe703"
)
SHA256_BY_SCENARIO_FILE["optbench.py"] = (
    "f363bcdd49d133befebc45273f320540058e7d00edc57fba34451b675fceddd8"
)
SHA256_BY_SCENARIO_FILE["scale.py"] = (
    "c4c8749d166e4df34e0b0e92220434fdb508c5c2ac56eb80c07043be0048dded"
)
SHA256_BY_SCENARIO_FILE["skew.py"] = (
    "bf60802205fc51ebf94fb008bbdb6b2ccce3c9ed88a6188fa7f090f2c84b120f"
)
SHA256_BY_SCENARIO_FILE["subscribe.py"] = (
    "732c941a33645fc0942afef252e373f36395e119edbdb4f59008eeaebbea6abc"
)
