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

FEATURE_BENCHMARK_FRAMEWORK_VERSION = "1.3.0"
FEATURE_BENCHMARK_FRAMEWORK_HASH_FILE = Path(__file__).relative_to(MZ_ROOT)

FEATURE_BENCHMARK_FRAMEWORK_DIR = Path(__file__).resolve().parent
FEATURE_BENCHMARK_SCENARIOS_DIR = FEATURE_BENCHMARK_FRAMEWORK_DIR / "scenarios"

# Consider increasing the #FEATURE_BENCHMARK_FRAMEWORK_VERSION if changes are expected to impact results!
SHA256_OF_FRAMEWORK: dict[str, str] = {
    "*": "a23bdd87546cc0308bf92efc1d7f3841201bb33d56c26edd788c1cc42fd05ffb"
}

# Consider increasing the scenario's class #version() if changes are expected to impact results!
SHA256_BY_SCENARIO_FILE: dict[str, str] = {
    "benchmark_main.py": "1d61feccbf7178b6b430bbcbec00f70e008339d6f64f9cd9ea6208c13c1f5851",
    "concurrency.py": "2e9c149c136b83b3853abc923a1adbdaf55a998ab4557712f8424c8b16f2adb1",
    "customer.py": "d1e72837a342c3ebf1f4a32ec583b1b78a78644cdba495030a6df45ebbffe703",
    "optbench.py": "f1e63f31d3ec6bf55093a467046b8d2f12e4b8b419420bad53e14a4f23b72989",
    "scale.py": "c4c8749d166e4df34e0b0e92220434fdb508c5c2ac56eb80c07043be0048dded",
    "skew.py": "bf60802205fc51ebf94fb008bbdb6b2ccce3c9ed88a6188fa7f090f2c84b120f",
    "subscribe.py": "510cf9037308936f1bb02a1c2e6345a90bcbda89f701653518d46d1fb56a2328",
}
