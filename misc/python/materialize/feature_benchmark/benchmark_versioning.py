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
    "*": "56baba5e5efe24da35b8365089c59557f482aef630edeeaa358f7f3bb52a663f",
}

# Consider increasing the scenario's class #version() if changes are expected to impact results!
SHA256_BY_SCENARIO_FILE: dict[str, str] = {
    "benchmark_main.py": "742ad1c2bff5f59d02b941a8e3d47c22dc259201959a29c90fe00352de03ae02",
    "concurrency.py": "9c7d40ee2402b0aec399126cc79685039934301f80bd925d7a5d00157ad7d06f",
    "customer.py": "d1e72837a342c3ebf1f4a32ec583b1b78a78644cdba495030a6df45ebbffe703",
    "optbench.py": "b78682659b2f6f405e158643e50e4d68481174b9353af19907719c21f573a7a1",
    "scale.py": "c4c8749d166e4df34e0b0e92220434fdb508c5c2ac56eb80c07043be0048dded",
    "skew.py": "bf60802205fc51ebf94fb008bbdb6b2ccce3c9ed88a6188fa7f090f2c84b120f",
    "subscribe.py": "457734e97c1c6cc761636d38bf53544144567564615d2cf5654d28e058f7ca99",
}
