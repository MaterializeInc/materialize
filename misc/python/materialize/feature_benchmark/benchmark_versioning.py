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

FEATURE_BENCHMARK_FRAMEWORK_VERSION = "1.4.0"
FEATURE_BENCHMARK_FRAMEWORK_HASH_FILE = Path(__file__).relative_to(MZ_ROOT)

FEATURE_BENCHMARK_FRAMEWORK_DIR = Path(__file__).resolve().parent
FEATURE_BENCHMARK_SCENARIOS_DIR = FEATURE_BENCHMARK_FRAMEWORK_DIR / "scenarios"

# Consider increasing the #FEATURE_BENCHMARK_FRAMEWORK_VERSION if changes are expected to impact results!
SHA256_OF_FRAMEWORK: dict[str, str] = {
    "*": "c13ba2db3cccea214fbf946ef9037a307dd64b1b2d837611fc407c82cb5d8cfc",
}

# Consider increasing the scenario's class #version() if changes are expected to impact results!
SHA256_BY_SCENARIO_FILE: dict[str, str] = {
    "benchmark_main.py": "aca597f4eedb9c3e9ea08327b9997dab9ccb7202a0fe7df35181958d7413356b",
    "concurrency.py": "2e9c149c136b83b3853abc923a1adbdaf55a998ab4557712f8424c8b16f2adb1",
    "customer.py": "d1e72837a342c3ebf1f4a32ec583b1b78a78644cdba495030a6df45ebbffe703",
    "optbench.py": "e0aa427c4af0467a408ebd11fcff55b75da910bae035cf272bebd3924ebc3483",
    "scale.py": "c4c8749d166e4df34e0b0e92220434fdb508c5c2ac56eb80c07043be0048dded",
    "skew.py": "bf60802205fc51ebf94fb008bbdb6b2ccce3c9ed88a6188fa7f090f2c84b120f",
    "subscribe.py": "66b6ba61daed10a0e78291f6251e62dcb41f206228028bc0cbd0d738ad76252b",
}
