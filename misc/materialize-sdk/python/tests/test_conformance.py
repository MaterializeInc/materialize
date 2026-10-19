# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the
# root of this repository, or online at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Runs every conformance vector through the binding."""

import json
from pathlib import Path

import pytest
from materialize_sdk import ProtocolError, TimestampTooLarge, UsageError, _core

VECTORS = Path(__file__).resolve().parents[2] / "conformance"
CASES = [
    pytest.param(vectors["function"], case, id=f"{path.stem}: {case['name']}")
    for path in sorted(VECTORS.glob("*.json"))
    for vectors in [json.loads(path.read_text())]
    for case in vectors["cases"]
]


def test_the_vectors_are_found():
    assert len(CASES) > 200


KINDS = {
    ProtocolError: "protocol",
    TimestampTooLarge: "timestamp_too_large",
    UsageError: "usage",
}


def subscription(input):
    """Runs a ``subscription`` vector through the binding's own class, the
    way the core's runner does through ``binding::Subscription``."""
    options = {key: value for key, value in input.items() if key != "steps"}
    try:
        sub = _core.Subscription(json.dumps(options))
    except tuple(KINDS) as error:
        return {"error": KINDS[type(error)]}
    outputs = [{"declare": sub.declare_sql()}]
    for step in input["steps"]:
        [(name, step_input)] = step.items()
        try:
            outputs.append(json.loads(getattr(sub, name)(json.dumps(step_input))))
        except tuple(KINDS) as error:
            outputs.append({"error": KINDS[type(error)]})
    return outputs


@pytest.mark.parametrize("function, case", CASES)
def test_vector(function, case):
    if function == "subscription":
        output = subscription(case["input"])
    else:
        output = json.loads(_core.conformance(function, json.dumps(case["input"])))
    assert output == case["expect"]
